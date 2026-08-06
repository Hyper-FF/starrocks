# P0 报告：FE Deparser Round-Trip 保真度

- Date: 2026-07-29
- Plan: `SQL_AST_FUZZER_PLAN.md` Phase 0
- Harness: `fe/fe-fuzz/`（maven profile `fuzz`，不进默认构建）
- Baseline: main @ `8c2647a7b6d`

## 1. 方法

对语料里每条语句检查 deparser 的**不动点不变式**：

```
ast1 = SqlParser.parse(s)
s1   = AstToSQLBuilder.toSQL(ast1)
ast2 = SqlParser.parse(s1)     <- 必须成功
s2   = AstToSQLBuilder.toSQL(ast2)
s1 == s2                       <- 必须成立
```

**不比较原文 `s` 与 `s1`**——那里的差异只是格式噪声。`s1 == s2` 才是真不变式，也正是 AST 变异 fuzzer 赖以成立的性质。

语料：`test/sql/**/T/*`（1260 文件）+ `fe/fe-core/src/test/resources/**/*.sql`（551 文件），
剥离 SQL-Tester 指令行（`function:` / `shell:` / `--` 注释）、丢弃含 `${}` 插值的语句，
按字面量感知的分号切分，去重后得 **27903 条唯一语句**。

## 2. 结果

| 分类 | 数量 | 含义 |
|---|---:|---|
| OK | 16277 | 干净往返 |
| PARSE_FAIL | 2042 | 语料本身解析不了（版本差异/切分残片），不计入分母 |
| DEPARSE_EMPTY | 5778 | **deparser 未实现该语句类型**，返回空 —— 是能力边界，不是 bug |
| DEPARSE_THROW | 1233 | `toSQL()` 抛异常 |
| REPARSE_FAIL | 2325 | `toSQL()` 产出的 SQL 解析不回来 |
| FIXPOINT_MISMATCH | 248 | `s1 != s2` |

两个口径：

- 全部可解析语句：25861 条，干净 16277 = **62.94%**
- **扣掉 deparser 根本没实现的类型**（更有意义的口径）：20083 条，干净 16277 = **81.05%**，违例 3806 = **18.95%**

按语句类型：

| 语句 | 可解析 | 违例 | 干净率 |
|---|---:|---:|---:|
| QueryStatement | 15618 | 1197 | 92.3% |
| InsertStmt | 3602 | 2500 | 30.6% |
| SetStmt | 583 | 74 | 87.3% |

## 3. 发现

### 3.1 【能力边界】deparser 只覆盖 query/DML，完全不覆盖 DDL

**40 个语句类型的 deparse 成功率是 0%**，合计 5677 条语句 —— `AST2SQLVisitor` 对它们没有 handler，直接返回空串：

```
CreateTableStmt 2350   AlterTableStmt 849   DropTableStmt 402
CreateMaterializedViewStatement 344   CreateDbStmt 203   UseDbStmt 197
DropDbStmt 163   RefreshMaterializedViewStatement 163   UpdateStmt 138
AdminSetConfigStmt 134   ... （另 30 个类型）
```

这不是缺陷——deparser 的设计目标是序列化视图/MV 定义体与审计日志，本来就不含 DDL。
但它对 fuzzer 是**硬约束**，见 §5。

### 3.2 【BUG】`IN (subquery)` 的括号无界增长

每往返一轮多 2 层括号，不收敛：

```
in:      select v1 from t0 where v2 in (select v3 from t1)
round 1: SELECT `v1` FROM `t0` WHERE `v2` IN (((SELECT `v3` FROM `t1`)))
round 2: SELECT `v1` FROM `t0` WHERE `v2` IN (((((SELECT `v3` FROM `t1`)))))
round 3: SELECT `v1` FROM `t0` WHERE `v2` IN (((((((SELECT `v3` FROM `t1`)))))))
```

机制：`ExprExplainVisitor.visitInPredicate`（:261）对 `IN` 的实参列表**无条件包一层括号**，
而 `visitSubqueryExpr`（:385）对 Subquery **又包一层**；解析器把冗余括号物化进 AST，
下一轮打印再各加一层 → 每轮 +2。

对照组正常：`IN (1,2,3)` 稳定、`EXISTS (subquery)` 稳定、`select (v1)` 的冗余括号被正确归一。
所以是 `InPredicate` × `Subquery` 组合专有。

**产品影响（已核实，非推断）**：

- ❌ **不会**在视图存储上累积。`AST2SQLVisitor.visitView`（:380）打印的是**视图名**而非展开体，
  层叠视图 / `ALTER VIEW` 各自只经过一次 deparse。存储的定义里是 3 层括号而非 1 层，仅仅是难看。
  （初版报告推测"层叠视图逐轮累积"，核对代码后**该推测不成立**。）
- ✅ **确有一处生产代码依赖 deparse 幂等而被这个 bug 破坏**：
  `IvmRefreshDefinition.warnIfDiffersFromFrozen`（:85-105）把 frozen 文本
  「parse → 用同一个 serializer 再 deparse」后与 `derivedSql` 做**字符串相等比较**
  （注释原文：*"Normalize the frozen text through the same serializer before comparing"*）。
  归一化不幂等 → frozen 侧 5 层括号 vs derived 侧 3 层 → 定义含 `IN (subquery)` 的 IVM MV
  **每次都会误报** `"MV was built by an older rewrite"` WARN。
- 严重度：日志噪声 + 存储文本冗余。**没有**数据正确性或不可用后果。语料中命中 222 条。

### 3.3 【BUG，已修复】`INSERT ... VALUES` 产出无法解析的 SQL

```
in:      insert into t (id, val) values (1, 111)
round 1: INSERT INTO `t` (`id`,`val`) (VALUES(1, 111))
round 2: ParsingException: No viable statement for input '(VALUES'
```

`AST2StringVisitor.visitInsertStatement`（:1026-1028）直接把 `insert.getQueryStatement()` 的
打印结果拼上，而 VALUES 型 QueryStatement 的打印带前导括号，语法不接受
`INSERT INTO t (cols) (VALUES ...)`。

语料中命中 **2172 条**，是全部违例的最大单一来源；修掉它 InsertStmt 违例从 2500 降到 328。

**产品影响（已核实）**：`AstToSQLBuilder` 的类注释明写其契约是
*"ensure that the generated SQL must be a legal SQL"* —— 这里直接违约。但可达路径很窄：

- 审计日志 / query profile 的 SQL 字段（`StmtExecutor:438-449`）只在
  **多语句请求**、`AuditEncryptionChecker.needEncrypt`、或 `Config.enable_sql_desensitize_in_log=true`
  三种情况下走 deparse，其余情况直接记原文。命中时日志里的 `INSERT ... VALUES` **不可复制重放**。
- 逐一核对了其余 InsertStmt deparse 调用点，**没有**发现产物被回灌给 parser 的路径：
  `AstBuilder:5755` 仅用于报错文案；`CreatePipeStmt` 打印路径要求 `FILES()` 源，VALUES 不可达；
  `RecursiveCTEExecutor:149/229` 生成的是 INSERT…SELECT。
- 严重度：审计日志保真度。**没有**功能性破坏。

**修复**（`AST2StringVisitor` + `AST2SQLVisitor`）：`(VALUES ...)` 的外层括号在
**派生表位置**（`FROM (VALUES ...) t`）是必需的，只有 **INSERT 源位置**不该有。
故把裸列表渲染抽成 `visitValueRows()`，`visitValues()` 仍包括号，新增 `visitInsertSource()`
让 INSERT 走裸列表；带 CTE / 别名 / ORDER BY / LIMIT 的情况回退到原通用路径以免丢子句。
digest 路径把原分支原样搬进 `visitValueRows` 覆写，输出逐字节不变。

**验证**：

| 指标 | 修复前 | 修复后 |
|---|---:|---:|
| REPARSE_FAIL | 2325 | **174** |
| OK | 16277 | **18415** |
| InsertStmt 干净率 | 30.6% | **90.0%** |
| 扣除未实现类型后总干净率 | 81.05% | **91.71%** |

无回归：`−2151 REPARSE_FAIL = +2138 OK + 13 FIXPOINT_MISMATCH`，即仅原先 REPARSE 失败的语句重新分布，
**没有任何原本 OK 的语句变差**；QueryStatement 一行前后逐字节相同。
那 +13 是被 REPARSE 失败**掩盖**的既有问题重新暴露（§3.2 的括号增长，以及新发现的 §3.6）。

FE UT：`AstToSQLBuilderTest`（+3 新用例）/ `AstToSqlTest` / `DigestTest` / `SqlDigestBuilderTest` /
`InsertPlanTest` 共 **91 passed, 0 failed**；`mvn checkstyle:check -pl fe-core` 通过。

### 3.6 【BUG，新发现】超大整数字面量往返时静默丢精度

修 §3.3 后暴露出来的既有缺陷，**与 INSERT 无关**：

```
in:      select 1234567890123456789012345678901234567890123456789012345678901234567890123456
round 1: SELECT 1234567890123456789012345678901234567890123456789012345678901234567890123456E0
round 2: SELECT 1.2345678901234569E75          <- 76 位有效数字塌缩成 17 位
```

deparser 给超出整数类型范围的字面量补了 `E0` 后缀，parser 再读就当成 double。
`100.1234567890` 这类正常 decimal 稳定，只有超大整数命中。

严重度：比 §3.2 高——是**静默数值改变**而非日志噪声。视图/MV 定义若含这类字面量，
存储的定义与用户写的值不再等价。尚未评估生产可达性，未修。

### 3.4 ~~【BUG】~~【已证伪，测量产物】map 字面量把 Java 类型对象打进 SQL

> **P0.5 结论（2026-07-29）**：在已 analyze 的 AST 上 `REPARSE_FAIL = 0`，本现象不复现。
> 成因是本轮在未 analyze 的 AST 上测量，`map{}` 此时类型为空。
> **不是产品缺陷**，保留于此仅作方法论说明。详见 `DEPARSER_ROUNDTRIP_P05_REPORT.md` §3。

```
in:      select map1 in (map{}) from sc2
round 1: SELECT `map1` IN ((PseudoType.AnyMapType{})) FROM `sc2`
round 2: ParsingException: No viable statement for input '(PseudoType.AnyMapType{'
```

`PseudoType.AnyMapType` 是 Java 类型对象的 `toString()` 泄漏到了生成的 SQL 里。
**注意**：该 map 字面量在未 analyze 时类型为空，此现象是否在 analyze 后消失**尚未验证**（见 §4）。

### 3.5 ~~【待定】~~【已证伪，测量产物】未 analyze 的 AST 上 deparse 会 NPE

> **P0.5 结论（2026-07-29）**：在已 analyze 的 AST 上 QueryStatement 的 `DEPARSE_THROW` 从 830 降到 **0**。
> **不是产品缺陷**。详见 `DEPARSER_ROUNDTRIP_P05_REPORT.md` §3。
> （注意：InsertStmt 仍有 45 条 `DEPARSE_THROW`，但那是独立的 `VALUES (DEFAULT, ...)` 缺陷，见 P0.5 §4.3。）

`DEPARSE_THROW` 1233 条中占比最大的形态：

```
NullPointerException: Cannot invoke "com.starrocks.type.Type.isNull()" because "type" is null
  || select id, approx_l2_distance([1,1,1,1,1], vector1) from t_test_vector_table order by ... limit 1
```

类型未解析导致。这一类**大概率是本次测量方法的产物而非产品缺陷**，见 §4。

## 4. 本次测量的已知局限

**全部数字都是在「未 analyze 的 AST」上测的**，而 `AstToSQLBuilder` 在生产中只作用于
**已 analyze** 的 AST（视图定义、MV、审计日志）。因此：

- §3.2、§3.3 是**结构性**问题，与 analyze 无关，已用最小用例确认 —— 结论可靠。
- §3.4、§3.5 依赖类型解析，**analyze 后是否仍然复现未验证** —— 不能据此判定为产品缺陷。
- 81.05% 这个干净率是未 analyze 口径的下界，analyze 口径预计更高。

补齐方法：用 `UtFrameUtils` / `StarRocksAssert` 建带表的进程内 catalog，
对同一语料做 parse→**analyze**→deparse 复测。这是 P0.5，应在 P1 之前完成。

## 5. 对 P1 的约束（P0 的真正产出）

1. **DDL 不能走 AST 变异。** §3.1 的 40 个类型 deparse 率为 0。
   变异算子 M8（语句序列拼接）必须把 DDL 当**文本模板 + 参数**处理，
   只有 `QueryStatement` / `InsertStmt` 走真正的 AST 变异。

2. **变异循环必须是 parse → analyze → mutate → analyze → deparse。**
   §3.5 表明未 analyze 的 AST 上 deparse 不可靠。这也意味着 fuzzer **从第一版起就需要真实 catalog**，
   没有「无 catalog 的简化 v1」这条路。

3. **必须有不动点预检闸门。** 变异产物在送去执行前先跑一次 `s1 == s2`；不成立就丢弃，
   否则 §3.2/§3.3 这类 deparser 缺陷会伪装成执行器 bug，淹没真实信号。

4. **InsertStmt 在 §3.3 修复前不可用作变异宿主**（当前 69% 违例率）。
   P1 先只做 QueryStatement（92.3% 干净，占语料 60%）。

## 6. 复现

```bash
cd fe && JAVA_HOME=/home/public/jdk-17.0.2 \
  mvn -Pfuzz -pl fe-fuzz -am -DskipTests -Dcheckstyle.skip=true compile

# 全量保真度报告
java -Xmx4g -Dlog4j2.disable.jmx=true -cp "fe/fe-fuzz/target/classes:<fe-core-classpath>" \
  com.starrocks.fuzz.RoundTripFidelityChecker /tmp/report.md test/sql fe/fe-core/src/test/resources

# 已知违例的最小复现 + 对照组
java -Dlog4j2.disable.jmx=true -cp "fe/fe-fuzz/target/classes:<fe-core-classpath>" \
  com.starrocks.fuzz.DeparseIdempotenceProbe 5
# => violations: 3/6
```

## 7. 后续

| # | 项 | 说明 |
|---|---|---|
| 1 | P0.5：analyze 口径复测 | 判定 §3.4/§3.5 是否为真缺陷，并给出 P1 可用的准确干净率 |
| 2 | 修 §3.3 INSERT VALUES | 违反类注释写明的契约；占全部违例 57%。生产侧只影响审计日志保真度，**不紧急**，但修法明确、风险低 |
| 3 | 修 §3.2 IN-subquery 括号 | 有一处已核实的生产误报（IVM MV frozen-diff WARN）。同样**不紧急**，修法contained |
| 4 | 把 `DeparseIdempotenceProbe` 的用例转成 FE UT | 防回归；修完 2/3 后对照组应变成 6/6 STABLE |
