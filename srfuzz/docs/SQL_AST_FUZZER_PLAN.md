# StarRocks AST 变异 + 覆盖率引导 SQL Fuzzer（srfuzz）Plan

- Status: draft
- Owner: hyper
- Last Updated: 2026-07-29
- 关联：`STABILITY_IMPROVEMENT_PLAN.md` 的 P0-5（`sqlgen` v1 + `plan_diff` v1）、W1.3（计划差分）、W2.1（Cancel Fuzzing）

---

## 0. 设计出发点

参考 Squirrel（CCS'20，*Testing DBMS with Language Validity and Coverage Feedback*），但**只抄一半**。

Squirrel 的两个组成部分：

| Squirrel 的做法 | 对 StarRocks 是否适用 |
|---|---|
| 自建 parser + 线性 IR + 对语法产生式人工标注 `define/use/scope`，运行时用近似符号表把变异产物里的标识符改写成合法名字（语义有效率 ~80%） | **不适用，直接丢弃**。那是为 SQLite/MySQL/PG 这类"parser 与执行深度耦合、没有 deparser"的 DBMS 手搓的近似 binder。StarRocks FE 自带真 parser、真 AST、真 deparser、真 Analyzer、真 catalog，语义正确性可以做到 100% 精确 |
| AFL 覆盖率反馈驱动语料库进化 | **适用，这是要抄的部分**。没有它就只是 SQLsmith 式盲打生成器，永远走不到需要特定多语句序列才能到达的深层状态 |

**一句话定位**：用 StarRocks FE 自己的 AST 做变异，用 Analyzer 做语义保证，把 AFL 的覆盖率反馈闭环补上。

现成可复用的件（已核实存在）：

| 组件 | 路径 |
|---|---|
| SQL → AST | `fe/fe-core/src/main/java/com/starrocks/sql/parser/SqlParser.java` |
| AST 遍历框架 | `fe/fe-core/src/main/java/com/starrocks/sql/ast/AstTraverser.java`、`fe/fe-parser/.../ast/AstVisitor.java` |
| AST → SQL（deparser） | `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/AstToSQLBuilder.java`（`toSQL` / `buildSimple` / `toDigest`） |
| 进程内可分析的 catalog | `fe/fe-core/src/test/java/com/starrocks/utframe/{UtFrameUtils,StarRocksAssert}.java`、`pseudocluster/PseudoCluster.java` |
| 种子语料 | `test/sql/**/T/*`（**1260** 个）+ `fe/fe-core/src/test/resources/**/*.sql`（**551** 个） |
| BE 故障注入 | `ENABLE_FAULT_INJECTION=ON`（`build.sh:306`）+ `fuzz` profile（ASan+FP，已就绪） |
| BE clang 工具链 | `be/CMakeLists.txt:63` 支持 `STARROCKS_LLVM_HOME` → SanitizerCoverage 可行 |

---

## 1. 架构

```
┌──────────── srfuzz JVM（进程内嵌 FE，含 catalog）────────────┐
│                                                              │
│   Corpus ──调度──> Seed(AST)                                 │
│      ▲                 │                                     │
│      │            [M] AST 变异算子                            │
│      │                 │                                     │
│      │            Analyzer.analyze(真 catalog)               │
│      │                 │  └─> Tier A / B / C 分档            │
│      │                 │                                     │
│      │            AstToSQLBuilder.toSQL                      │
│      │                 │                                     │
│      │            [O1] round-trip 自检                        │
│      │                 │                                     │
│      │            FE 覆盖率位图（ASM agent，进程内 int[]）      │
│      └──覆盖率增量──────┤                                     │
└────────────────────────┼─────────────────────────────────────┘
                         │ JDBC（串行，一次一条）
                   ┌─────▼─────────────────────────────┐
                   │ 真实集群：FE + BE(ASan+SanCov+FP) │
                   └─────┬─────────────────────────────┘
                         │ HTTP /api/coverage?reset=1
                         └──> BE 边覆盖位图 ──┐
                                              └─> 并入 Corpus 反馈
```

两种运行模式，共用同一个变异器：

- **Mode FE-ONLY**：进程内 pseudo-cluster，不起 BE。速度 100~1000 次/秒。抓 FE 崩溃、PlanValidator 失败、deparser 失真、规划超时。
- **Mode FULL**：产出的 SQL 打到真实 ASan 集群。速度 10~50 次/秒（串行，见 §5 风险 3）。抓 BE 崩溃 / ASan 报告 / 结果不一致 / hang。

---

## 2. 变异算子（[M]）

从语料库按 AST 节点类型建**片段池**（`Expr` / `Relation` / `QueryStatement` / `AlterClause` / …），变异时同类型互换。9 类算子，按性价比排序：

| # | 算子 | 说明 | 主要目标 bug 类 |
|---|---|---|---|
| M1 | 子树替换 | 同 class 的 AST 子树从片段池随机换入 | 通用 |
| M2 | 函数/算子替换 | 从**真实函数注册表**里挑同 arity 的函数替换；运算符 `+`→`/`、`=`→`<=>` | BE builtin 崩溃（承接 `be/test/fuzzy/builtin_functions_fuzzy_test.cpp` 的思路，但带上下文） |
| M3 | 字面量边界化 | INT_MIN/MAX、NaN、空串、超长串、非法 UTF-8、深嵌套 JSON、decimal 精度边界、日期边界 | 类型转换 / 溢出 |
| M4 | 标识符重绑定 | SlotRef 换成同 scope 内另一可见列（**用 Analyzer 解析出的 scope，不做近似**） | 类型泄漏、裁剪 |
| M5 | 子句增删 | WHERE / GROUP BY / HAVING / ORDER BY / LIMIT / DISTINCT / 窗口 | 优化器规则 |
| M6 | 嵌套包裹 | 关系包成子查询 / CTE / UNION / 自连接 / 视图 | CTE、子查询改写 |
| M7 | 类型压力 | 插入 CAST 链、STRUCT 字段访问、MAP key、ARRAY 下标、UNNEST | 复杂类型 / 子字段裁剪 |
| M8 | **语句序列拼接** | 在 DML 之间插入 ALTER / CREATE INDEX / CREATE MV / 导入 / compaction 触发 | **状态相关的深层 bug（最高价值）** |
| M9 | Session flag 扰动 | 查询前后翻转 session 变量 | 桥接 W1.3 计划差分 |

M8 是 Squirrel 相对单语句 fuzzer 的核心优势来源，也是 StarRocks 事故史（schema change、compaction、global dict）最集中的地方，应优先做。

---

## 3. Oracle（判定什么算 bug）

### O1 — Round-trip 保真（Phase 0 就交付，兼作自检）

```
s1 = toSQL(parse(s))
s2 = toSQL(parse(s1))
断言 s1 == s2                                  # deparser 不动点
断言 planDigest(analyze(parse(s))) == planDigest(analyze(parse(s1)))   # 语义不漂移
```

双重作用：**本身是一类 bug**（反解丢引号/丢 hint/丢精度/丢隐式 cast），同时是整条流水线的**防假阳性闸门**——不先做这个，后面所有"崩溃"都可能只是 deparser 的锅。

### O2 — Analyzer 分档（不要做二元过滤）

| 档 | 条件 | 处置 |
|---|---|---|
| **A** | `analyze()` 通过 | 主力路径，正常执行 |
| **B** | 抛 `AnalysisException` / `SemanticException`（声明式错误） | 低权重**仍然发给集群**——要验证的是"干净拒绝而非崩溃" |
| **C** | 抛 `NullPointerException` / `IllegalStateException` / `ClassCastException` / `StackOverflowError` / assert | **直接判定为 bug**，归档 |

> 只留 A 档会把"FE 本该拒绝却放行、然后 BE 炸掉"这一整类屏蔽掉——而这恰恰是历史事故的主要形态（JSON case-collision、subfield-prune 类型泄漏、CSV trim 下溢）。

### O3 — 崩溃 / Sanitizer

BE ASan 报告、BE core、FE OOM、FE 卡死、query 超时未返回。

### O4 — 差分（Phase 4，对接 W1.3）

同一条 SQL 在 session flag 矩阵下比对结果集；baseline = 关闭全部可选优化。
**前置约束**：diff 模式禁用非确定性函数（`now()`/`rand()`/`uuid()`），结果集强制排序后比对。

### O5 — PlanValidator

复用 `sql/optimizer/validate/PlanValidator`（`enable_plan_validation` 默认 true），校验失败即 P1。

---

## 4. 覆盖率反馈

### FE 侧（Mode FE-ONLY，进程内，便宜）

- 一个 ASM ClassFileTransformer agent，只插桩 `com.starrocks.sql.*` + `com.starrocks.planner.*`（控制开销）。
- 边 id = `hash(class, method, bbIndex) % 65536`，写进程内 `byte[65536]`，**不落盘**。
- 每条 query 前清零、后读取，算 new-edge 增量。因为在同一进程内，成本近乎为零 —— 这是把 FE fuzz 放在进程内跑的最大红利。

### BE 侧（Mode FULL，需要新建）

1. `build.sh` 新增 `--with-sancov` → `-fsanitize-coverage=trace-pc-guard`（需 clang，`STARROCKS_LLVM_HOME`）。
2. 新增 `be/src/util/sancov_bitmap.{h,cpp}`：实现 `__sanitizer_cov_trace_pc_guard_init` / `__sanitizer_cov_trace_pc_guard`，写 64K 全局位图。
3. 新增 `be/src/http/action/coverage_action.{h,cpp}`（目录已有 20+ 个同构 action 可照抄），暴露 `GET /api/coverage?reset=1` 返回位图。
4. **归因问题**：全局位图无法把覆盖率归属到某条并发 query。v1 的解法是**严格串行执行**（一次一条，前置 reset）。v2 再做 per-fragment-instance 的线程局部位图。

### 语料库调度

抄 AFL++ 的既有策略即可，不要自创：favored 集合、执行时间加权、new-edge 优先、周期性 corpus 精简（minset）。

---

## 5. 分阶段交付

| Phase | 内容 | 工作量 | 验收标准 |
|---|---|---|---|
| **P0** ✅ | 只做 O1 round-trip，零变异，跑遍 1260 + 551 条现有语料 | 实际 1 天 | **已完成**，见 `DEPARSER_ROUNDTRIP_P0_REPORT.md`：27903 条语句，扣除 deparser 未实现的类型后干净率 81.05%，确认 2 个结构性 deparser bug |
| **P0.5** | 用进程内 catalog 做 parse→**analyze**→deparse 复测 | ~2 天 | 给出 analyze 口径的准确干净率；判定 map 字面量 / 类型 NPE 两类是否为真缺陷 |
| **P1** 🚧 | 变异器 M1–M5 + O2 分档 + Mode FE-ONLY，**无覆盖率** | 进行中 | 连续跑 4 小时无假阳性淹没；至少产出 1 个 Tier-C（FE 崩溃）候选 |
| **P2** | FE ASM 覆盖率位图 + 语料库进化 | ~1 周 | 4 小时内 FE 边覆盖单调增长且明显超过 P1 的随机基线（≥1.5×） |
| **P3** | BE SanCov + `/api/coverage` + Mode FULL 接真集群（`fuzz` profile） | ~2 周 | 能对一条 query 采到稳定的 BE 边增量；跑通 8 小时长稳；产出首个 ASan 报告 |
| **P4** | M6–M9 + O4 差分（并入 W1.3）+ AST 级用例最小化 | ~1.5 周 | 任一确认 bug 能自动最小化成一个 `test/sql/` 的 T/R 用例 |

**P0 必须先做**——它用 2 天验证整个方案最脆弱的假设（deparser 是否可靠），如果 `AstToSQLBuilder` 对大量语句类型都不保真，后续方案要重新设计（退路：限制变异器只处理能保真的语句子集）。

---

## 6. 落位

建议新建 maven 模块 **`fe/fe-fuzz/`**（不进默认构建 profile），依赖 `fe-core` + `fe-core` 的 test-jar。

- 选它而不是 `STABILITY_IMPROVEMENT_PLAN.md` 里提的 `tools/fuzz/`：变异器必须拿到 `UtFrameUtils` / `StarRocksAssert`，这些在 `fe-core` 的 **test** 源集里，放到 `tools/` 下要额外折腾依赖。
- BE 侧的 SanCov 运行时和 HTTP action 仍按常规落在 `be/src/util/` 与 `be/src/http/action/`。
- 与既有 plan 的关系：本文是 `STABILITY_IMPROVEMENT_PLAN.md` **P0-5 的具体设计**，产出的能力直接供给 W1.3（计划差分）与 W2.1（cancel fuzzing，把 M8/M9 换成 failpoint 注入即可复用同一驱动器）。

---

## 7. 风险与取舍

| # | 风险 | 缓解 |
|---|---|---|
| 1 | **deparser 不保真** → 假阳性淹没 | P0 先量化；变异器只覆盖 P0 认定能保真的语句类型 |
| 2 | **BE SanCov 需要 clang**，而 dev 镜像默认可能走 GCC | `be/CMakeLists.txt:63` 已支持 `STARROCKS_LLVM_HOME`；先在 `fuzz` profile 上验证 clang+ASan+SanCov 三者能共存。退路：P2 只做 FE 覆盖率，BE 覆盖率延后 |
| 3 | **串行执行导致吞吐低**（10~50 q/s，比 Squirrel 打 SQLite 低 2~3 个数量级） | 接受。低吞吐正是覆盖率引导价值更高的理由——每条 query 都得"值钱"。变异算子要针对性强（M7/M8 优先），而非追求通用 |
| 4 | 进程内 pseudo-cluster 的 catalog 与真集群漂移（默认属性、shared-data vs shared-nothing） | DDL 只生成一次，两边同时 apply，用 `DESC` 校验一致 |
| 5 | 结果非确定性破坏 O4 差分 | diff 模式禁用非确定性函数 + 强制结果排序 |
| 6 | 崩溃去重不做 → 同一个 bug 刷屏 | 按 ASan 栈顶 N 帧 / FE 异常栈哈希去重，第一天就要有 |
| 7 | ASan 构建下 M8 的 DDL 序列很慢（compaction、schema change） | M8 的重型算子单独低频调度，不与轻量算子同权重 |

---

## 8. 第一周任务

1. 建 `fe/fe-fuzz/` 骨架模块（不进默认构建）。
2. 写 P0 的 round-trip 检查器：读 `test/sql/**/T/*` + `fe/fe-core/src/test/resources/**/*.sql`，对每条语句跑 `parse → toSQL → parse → toSQL` 不动点检查。
3. 产出保真度报告：按语句类型统计通过/失败，失败样本存档。
4. 在 `fuzz` profile 上验证 `clang + ASan + -fsanitize-coverage=trace-pc-guard` 能否编过并跑起 BE（只验证可行性，不写运行时）。
5. 根据 2–4 的结果决定 P1 的变异器语句范围。

## Decision Log

- 2026-07-29：确定丢弃 Squirrel 的 IR + define/use/scope 标注方案，改用 FE 真实 Analyzer 做语义保证；保留其覆盖率反馈闭环设计。
- 2026-07-29：确定 Analyzer 不作二元过滤而做 A/B/C 三档，避免屏蔽"FE 误放行 → BE 崩溃"这一主要事故形态。
- 2026-07-29：确定 P0（round-trip 保真度量化）为强制前置门，因为整个方案依赖 `AstToSQLBuilder` 的可靠性。
- 2026-07-29：v1 接受串行执行以解决 BE 全局覆盖率位图的归因问题。
- 2026-07-29（P0 完成后）：deparser 对 **40 个 DDL 语句类型完全未实现**（deparse 率 0%），
  故 M8 语句序列拼接必须把 DDL 当文本模板处理，只有 QueryStatement / InsertStmt 走 AST 变异。
- 2026-07-29（P0 完成后）：变异循环定为 parse → **analyze** → mutate → analyze → deparse；
  未 analyze 的 AST 上 deparse 不可靠，因此 fuzzer 从第一版起就需要真实 catalog，取消"无 catalog 简化 v1"选项。
- 2026-07-29（P0 完成后）：P1 的变异宿主先只取 QueryStatement（92.3% 干净），
  InsertStmt（30.6%）等 `INSERT ... VALUES` deparse bug 修复后再纳入。
- 2026-07-29（P0 完成后）：新增强制的不动点预检闸门——变异产物执行前先验 `s1 == s2`，不成立即丢弃。
- 2026-07-29（P0.5 实施）：**harness 分两处放**。`fe/fe-fuzz/`（profile `fuzz`）放不依赖测试框架的独立工具
  （`RoundTripFidelityChecker`、`DeparseIdempotenceProbe`）；需要真实 catalog 的检查器放
  `fe-core/src/test/java/com/starrocks/fuzz/`，因为它要 `UtFrameUtils` 而 **fe-core 不发布 test-jar**。
  用 `@EnabledIfSystemProperty(named="srfuzz.corpus")` 门控，CI 永不执行。
  P1 若要长时间独立进程运行，需另行解决（在 `fuzz` profile 下给 fe-core 加 test-jar goal，或在 fe-fuzz 里自建最小 catalog 引导）。
- 2026-07-29（P0.5 实施）：catalog 构建策略 = **按语料文件建库**。每个 T-file 开独立 db，
  先执行该文件自带的 DDL 建表，再测该文件的查询，测完 drop。这既解决了"语料引用几千张不存在的表"，
  也顺带原型化了 P1 需要的 catalog 生命周期管理。

## 环境备忘（踩过的坑）

- `mvn -pl fe-core ...` **必须带 `-am`**。否则 fe-grammar 取自 m2 里的旧产物，
  报 `cannot find symbol: class IncludeMetadataContext`（与代码改动无关）。
- 跑过 `mvn test` 之后 `fe-core/target/classes` 会被 **JaCoCo 离线插桩**，
  直接 `java -cp` 会 `NoClassDefFoundError: org/jacoco/agent/rt/...`。
  需 `rm -rf fe/fe-core/target/classes` 后带 `-Djacoco.skip=true` 重编。
- 会话 scratchpad 可能被清空，**报告不要只写在 scratchpad**，产出后立即拷回仓库。
