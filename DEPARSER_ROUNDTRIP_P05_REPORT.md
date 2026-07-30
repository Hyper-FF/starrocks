# P0.5 报告：analyze 口径的 Deparser Round-Trip 保真度

- Date: 2026-07-29
- Plan: `SQL_AST_FUZZER_PLAN.md` Phase 0.5
- Harness: `fe/fe-core/src/test/java/com/starrocks/fuzz/AnalyzedRoundTripCheckerTest.java`
- Baseline: main @ `8c2647a7b6d` + 本地 INSERT VALUES 修复（`7cd46632fed`）
- 原始数据: `analyzed_roundtrip_p05_raw.md`
- 前置: `DEPARSER_ROUNDTRIP_P0_REPORT.md`

## 1. 为什么要重测

P0 是在**未 analyze 的 AST** 上测的，而 `AstToSQLBuilder` 在生产中只作用于**已 analyze** 的 AST
（视图/MV 定义、审计日志）。P0 报告 §4 明确把两类发现标为"analyze 后是否复现未验证"。本轮回答这个问题。

链路改为生产形状，并对每个语料文件**用它自带的 DDL 建真实 catalog**：

```
每个 T-file → 建独立 db → 执行该文件的 CREATE TABLE/VIEW/MV
           → 对该文件的每条 query:
                parse → analyze → deparse(s1) → parse → analyze → deparse(s2) → 比较 s1/s2
           → drop db
```

比 P0 多两个判据（纯 parse 口径**不可能**产生）：

- `ANALYZE_FAIL_INTERNAL` —— analyzer 抛 NPE/ISE 而非声明式错误，**每一条都是 bug 候选**
- `REANALYZE_FAIL` —— 反解后的 SQL 能解析但**不再能 analyze**，即 deparser 静默改变了语义

## 2. 结果

语料 1319 个文件；catalog 构建 **3190 条 DDL 成功 / 1978 条失败**（失败多为外部 catalog、
未支持属性、跨库限定名，属预期噪声）。

| 分类 | 数量 |
|---|---:|
| OK | 19494 |
| ANALYZE_FAIL_INTERNAL | 5 |
| DEPARSE_THROW | 45 |
| DEPARSE_EMPTY | 0 |
| REPARSE_FAIL | **0** |
| REANALYZE_FAIL | 27 |
| FIXPOINT_MISMATCH | 683 |

**可 analyze 语句 20254 条，干净往返 19494 条 = 96.25%**

| 语句 | analyzed | OK | 干净率 |
|---|---:|---:|---:|
| QueryStatement | 16313 | 15647 | 95.9% |
| InsertStmt | 3941 | 3847 | 97.6% |

> ⚠️ **不要直接与 P0 的 91.71% 相减**：P0 语料含 `fe-core` 测试资源且**全局去重**，
> P0.5 只用 `test/sql` 且**按文件独立处理不去重**（同一条语句出现在 10 个文件里就计 10 次）。
> 两者分母不同，只能做定性比较。

## 3. P0 遗留问题：两条全部证伪

| P0 发现 | P0（未 analyze） | P0.5（已 analyze） | 结论 |
|---|---:|---:|---|
| §3.5 `Type.isNull()` NPE（`approx_l2_distance` 等） | DEPARSE_THROW 830 | **QueryStatement 0** | **测量产物，不是产品缺陷** |
| §3.4 `map{}` → `PseudoType.AnyMapType{}` | REPARSE_FAIL 174 | **0** | **测量产物，不是产品缺陷** |

两者都源于"类型未解析"。analyze 之后类型齐备，deparser 正常工作。
**P0 报告中这两条应降级为方法论说明，不应作为缺陷跟踪。**

## 4. 新发现

### 4.1 🔴【最严重】`TEMPORARY PARTITION` 被 deparser 静默丢弃

两条**语义不同**的语句反解成**逐字节相同**的 SQL：

```
select * from t1 temporary partition(p1)   →  SELECT * FROM `t1` PARTITION (`p1`)
select * from t1 partition(p1)             →  SELECT * FROM `t1` PARTITION (`p1`)
insert into t1 temporary partition(p1) ... →  INSERT INTO `t1` PARTITION (p1) ...
```

**根因**：`AST2SQLVisitor.visitTable:401-409` 手搓输出 `" PARTITION ("`，
**从不检查 `node.getPartitionNames().isTemp()`**：

```java
if (node.getPartitionNames() != null) {
    List<String> partitionNames = node.getPartitionNames().getPartitionNames();
    if (partitionNames != null && !partitionNames.isEmpty()) {
        sqlBuilder.append(" PARTITION (");        // <-- 无条件，丢掉 TEMPORARY
        ...
```

而 `PartitionNames` 本身有 `isTemp()` 访问器，其 `toString()`（`PartitionNames.java:79`）
是正确的：`if (isTemp) sb.append("TEMPORARY ")`。deparser 没有复用它。

**为什么 P0 抓不到**：丢失在第一轮就稳定了（`TEMPORARY PARTITION` → `PARTITION` → `PARTITION`），
**不动点检查显示 STABLE**。只有 re-analyze 判据能发现（临时分区名在正式分区里不存在 → 分析失败）。
这条单独验证了 P0.5 的设计价值。

**危害评估（已用 `TempPartitionDeparseTest` 实测，结论比初判低）**

初判曾假设"临时分区与正式分区允许同名 → 静默读写错分区"。**该假设被实测否定**：

```
ALTER TABLE t ADD TEMPORARY PARTITION p1 ...   （t 已有正式分区 p1）
  → REJECTED: AnalysisException: Duplicate partition name p1.
```

分区名在正式/临时两个命名空间间**是唯一的**，因此不存在"同名指向另一个分区"的场景。
**没有静默读写错数据的风险，此前的判断作废。**

真实危害是**持久化元数据被写坏**：

```
CREATE VIEW v_temp AS SELECT * FROM t TEMPORARY PARTITION (tp1)
  → VIEW CREATED（不报错）

存储的 inlineViewDef:
  SELECT `db`.`t`.`k`, `db`.`t`.`v` FROM `db`.`t` PARTITION (`tp1`)   ← TEMPORARY 已丢

重新解析该定义:
  → FAILS: SemanticException: Unknown partition 'tp1' in table 't'.
```

即：**建视图时成功且无任何警告，但这个视图从此永远无法被查询**，
且损坏状态已写入元数据（随 checkpoint / edit log 持久化，重启后依旧）。

INSERT 侧同理：`insert into t temporary partition(tp1) ...` 反解成 `PARTITION (tp1)`，
在经过 deparser 的路径（审计日志等）里记录的是错误的目标分区。

语料命中 24 条（`REANALYZE_FAIL` 的绝大部分）。

**严重度定级**：真实缺陷，产生**不可用且已持久化**的元数据，且创建时无告警；
但触发需要"在视图/MV 定义中引用临时分区"这种少见用法（临时分区通常只是导入过程中的临时载体）。
**不是数据正确性事故**，与 §3.2 括号增长同一量级或略高。

### 4.2 ~~🟠 `LEAD`/`LAG` over BITMAP 列 → analyzer NPE~~【已撤回，非缺陷】

```
NullPointerException: No assignment from VARBINARY(1048576) to BITMAP
  || SELECT LEAD(c26) OVER(ORDER BY k1) wv FROM t1
```

> **撤回（2026-07-30）**：这**不是缺陷**。该异常来自
> `com.google.common.base.Preconditions#checkNotNull`，是 FE **刻意的校验守卫**——
> 它带着开发者写的解释性消息（"No assignment from X to Y"），查询被干净拒绝、用户拿到清晰错误，
> 既不崩溃也不产生错误数据。只是异常**类型**用了 Guava 的 NPE 而非 `SemanticException`。
>
> 原判定源于 harness 分类器的错误启发式："非 SemanticException/AnalysisException/StarRocksException
> 即内部错误"。正确的判别依据是**抛出点**而非异常类型：栈顶在 `Preconditions` 即为刻意守卫。
> 分类器已在两个 harness 中修正。

5 条 `ANALYZE_FAIL_INTERNAL` 的真实构成：2 条 Preconditions 守卫（非缺陷，如上）+
3 条 `files()` 外部存储的 `StorageAccessException`（进程内无真实存储，环境噪声）。
**即：本轮 `ANALYZE_FAIL_INTERNAL` 一条真缺陷都没有。**

### 4.3 🟠 `INSERT ... VALUES (DEFAULT, ...)` → deparser NPE

```
NullPointerException || INSERT INTO t (id,name,job1,job2) VALUES (DEFAULT,3,3,3),(DEFAULT,4,4,4)
```

`DEFAULT` 关键字在 VALUES 中产生的节点被 `visit()` 直接 NPE。45 条 `DEPARSE_THROW` 全是这一类。
**与我已修复的 INSERT VALUES 括号 bug 无关**，是另一个独立缺陷（P0 未 analyze 口径下同样存在，
当时混在 InsertStmt 的 307 条 DEPARSE_THROW 里）。

### 4.4 ⚪【非缺陷，但影响 P1 设计】analyze 会让不动点检查产生良性假阳性

```
in: select * from (select max(c3), sum(c3) sc3, c0 from t5 group by c0 limit 10) t order by 3
s1: ... ORDER BY 3 ASC
s2: ... ORDER BY `t`.`c0` ASC
```

第二轮 analyze 把序号 `3` 绑定到了具体列，deparse 出的是绑定后的形式。
这个例子里两者语义等价，不是缺陷——但它让 `s1 == s2` 这个严格字符串判据失效。

> **⚠️ 2026-07-30 修正**：把序号展开一概称为"良性"是**错的**。
> P1 fuzz 证实：当被指向的 select 项**含子查询**时，展开会产出
> `ORDER BY <含子查询的表达式>`，触发"ORDER BY clause cannot contain subquery"，
> 即**合法查询被反解成非法 SQL**。详见 `FUZZ_P1_CONFIRMED_FINDINGS.md` 缺陷 2。
> 正确说法是：序号展开**通常**良性，但在 select 项含子查询时是真缺陷。

`FIXPOINT_MISMATCH = 683` 里**至少混有两种成因**：本条（良性）和 `IN (subquery)` 括号增长（真缺陷）。
**本轮未做拆分统计**，不能把 683 当作缺陷计数。

## 5. 本轮测量的局限

- `SetStmt` 被跳过 → 依赖 session flag 的查询是在**默认开关**下 analyze 的，与其在 SQL-Tester 中的真实上下文不同
- 语料里的 `create database X; use X;` 被忽略（harness 自行管理库），引用**限定名** `X.t` 的查询会
  analyze 失败，落入 `ANALYZE_FAIL_DECLARED`，属预期噪声
- 只测 `QueryStatement` / `InsertStmt`；`UpdateStmt`/`DeleteStmt` 的 deparse 率本就是 0%
- **不去重**，高频语句被重复计数
- `FIXPOINT_MISMATCH` 未按成因拆分（见 §4.4）

## 6. 对 P1 的修正

1. **不动点闸门不能用严格字符串相等。** §4.4 表明 analyze 自身会做等价归一化。
   P1 的变异产物预检应改为：`s1 能 parse` ∧ `s1 能 analyze` ∧ *（可选）计划摘要等价*，
   而不是 `s1 == s2`。严格相等只适合未 analyze 口径。

2. **`REANALYZE_FAIL` 应升级为 P1 的一等 oracle。** 它在本轮找到了全部发现里最严重的一条（§4.1），
   而不动点检查对该缺陷完全无感。

3. **P1 变异宿主范围可以按实测放宽**：QueryStatement 95.9%、InsertStmt 97.6%，
   两者都足够干净。P0 报告里"InsertStmt 暂不可用"的结论**已被本轮推翻**（那是括号 bug 未修时的数据）。

4. **catalog 生命周期方案已验证可用**：按文件建库 → 执行自带 DDL → 用完 drop，
   1319 个文件、3190 张表在单进程内跑完约 12 分钟，可直接作为 P1 的 catalog 层。

## 7. 复现

```bash
cd fe && JAVA_HOME=/home/public/jdk-17.0.2 mvn -pl fe-core -am test \
  -Dtest=AnalyzedRoundTripCheckerTest -DfailIfNoTests=false \
  -Djacoco.skip=true -Dcheckstyle.skip=true -DargLine="-Xmx8g" \
  -Dsrfuzz.corpus=/home/public/starrocks-2/test/sql \
  -Dsrfuzz.report=/tmp/analyzed_full.md
```

`-am` 必须带（否则 fe-grammar 取旧产物报 `IncludeMetadataContext` 缺失）。

## 8. 后续（按优先级）

| # | 项 | 说明 |
|---|---|---|
| ~~1~~ | ~~确认 §4.1 生产可达性~~ | **已完成**：`TempPartitionDeparseTest` 证实可达（视图可建成且定义已损坏），但同名场景不成立，严重度下调 |
| 2 | 修 §4.1 | 让 `visitTable` 复用 `PartitionNames` 的 `isTemp()`；`TempPartitionDeparseTest` 现有断言记录的是**当前错误行为**，修复后需翻转为 `assertNotEquals` |
| 3 | 拆分 §4.4 的 683 条 | 给 harness 加成因分类，得到真实缺陷计数 |
| 4 | 修 §4.3 `DEFAULT` NPE | 45 条，改动应该很小 |
| 5 | 报 §4.2 LEAD/LAG BITMAP NPE | 属 analyzer 缺陷，不在 deparser 范围 |
| 6 | 回填 P0 报告 | 把 §3.4/§3.5 降级为方法论说明 |
