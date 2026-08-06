# P1 AST 变异 Fuzz —— 确认缺陷

- Date: 2026-07-30
- Harness: `fe/fe-core/src/test/java/com/starrocks/fuzz/AstMutationFuzzerTest.java`
- 分诊: `fe/fe-core/src/test/java/com/starrocks/fuzz/FuzzFindingTriageTest.java`
- 原始数据: `ast_fuzz_report_raw.md`
- Baseline: main @ `36ee71092aa`

## 规模与信噪比

15976 seeds → **201711 mutants**，产出 20 个不同签名。分诊后：

| 类别 | 签名数 | 说明 |
|---|---:|---|
| **确认缺陷** | **3** | 手写 SQL 可复现，见下 |
| Preconditions 刻意守卫 | 3 | 带解释性消息的正常校验，**非缺陷** |
| 变异器伪影（语法不可达） | 4 | `setChild` 造出 parser 无法产生的树 |
| 已知缺陷复现 | 1 | TEMPORARY PARTITION（P0.5 §4.1） |
| 未分诊（低频/无法重建） | 9 | 多为 x1–x6，含 1 条 mutant 不可渲染 |

> ⚠️ 按**实例数**看噪声更严重：最高频的两个签名（x1919 + x492）合计 2400+ 实例，
> 分别是 Preconditions 守卫和变异器伪影，**都不是缺陷**。原始报告的数字严重高估。

三个确认缺陷全部来自 `REANALYZE_FAIL` 与非 Preconditions 的 `ANALYZE_INTERNAL_ERROR`
两个类别——验证了 P0.5 §6 把 `REANALYZE_FAIL` 升为一等 oracle 的判断。

---

## 缺陷 1 🔴 `array_contains(NULL, <array>)` → ClassCastException

```sql
select array_contains(NULL, [1,2]);
-- ClassCastException: class com.starrocks.type.NullType cannot be cast to
--                     class com.starrocks.type.ArrayType

select array_contains(null, null);   -- 对照组：ACCEPTED，正常
```

- 抛出点：`PolymorphicFunctionAnalyzer#resolvePolymorphicArrayFunction`
- **不是** Preconditions 守卫，是意外的类型强转
- 第一个参数为裸 `NULL`、第二个参数为具体数组时触发；两个都为 `NULL` 时正常
- 用户可直接写出，应当给出声明式错误而非 CCE

---

## 缺陷 2 🔴 `ORDER BY <序号>` 指向含子查询的 select 项时，反解产出非法 SQL

```sql
select id in (select id from arr t) from arr s order by 1;
```

analyze 通过，但反解成：

```sql
SELECT `s`.`id` IN (((SELECT `t`.`id` FROM `arr` AS `t`))) AS `...`
FROM `arr` AS `s`
ORDER BY `s`.`id` IN (((SELECT `t`.`id` FROM `arr` AS `t`)))   -- 序号被展开成完整表达式
```

再次 analyze 直接失败：

```
SemanticException: ORDER BY clause cannot contain subquery.
```

- 对照组 `select id from arr s order by 1`（select 项不含子查询）**往返正常**
- 根因：analyze 把 `ORDER BY 1` 绑定到第 1 个 select 项，deparser 打印**绑定后的表达式**而非保留序号；
  当该项含子查询时就撞上"ORDER BY 不得含子查询"的规则
- **产品影响**：与 P0.5 §4.1 的 TEMPORARY PARTITION 同一形态——
  视图/MV 定义以反解文本存储，这类查询建成视图后**定义永久损坏、无法再查询**

> 这也修正了 P0.5 §4.4 的判断：我当时称序号展开是"良性归一化"。
> 它在 select 项含子查询时**不是良性的**，会产出非法 SQL。

---

## 缺陷 3 🔴 无类型数组字面量 `[NULL]` 反解时被具体化，改变函数重载解析

```sql
select id from arr where array_contains_all(a_datetime, [NULL]);
```

analyze 通过，但反解成：

```sql
... WHERE array_contains_all(`arr`.`a_datetime`, ARRAY<BOOLEAN>[NULL])
```

再次 analyze 失败：

```
SemanticException: No matching function with signature:
                   array_contains_all(array<datetime>, array<boolean>)
```

- 对照组 `select [NULL] from arr` **往返正常**（不涉及重载匹配时无害）
- 根因：`[NULL]` 在 analyze 时按上下文隐式定型，deparser 却打印出默认的
  `ARRAY<BOOLEAN>`，把原本能匹配的重载打掉
- **产品影响**：同缺陷 2，视图/MV 定义被写坏

---

## 确认为非缺陷的项（供后续避免重复调查）

| 现象 | 结论 |
|---|---|
| `NPE: No assignment from BITMAP to VARBINARY`（`hex(to_bitmap(x))`） | `Preconditions#checkNotNull` 刻意守卫，带解释消息 |
| `ISE: Implicit casting for decimal arithmetic ...`（`decimal * datetime`） | `Preconditions#checkState` 刻意守卫 |
| `IAE@Preconditions#checkArgument`（`array_generate` 错参，x1919） | 手写 SQL 下是正常 `SemanticException` |
| `dict_mapping(cast(null as json), ...)` | 正常 `SemanticException` |
| `where exists 'HIGH'` / `not (exists c1)` | 语法不可达，变异器伪影 |
| `time_slice(..., interval 5 year, 1)` | 语法不可达，伪影 |
| 嵌套 lambda 箭头 | 语法不可达，伪影 |
| `(a, b) not in ('l')` 多列 IN 接非子查询 | 语法不可达，伪影 |

## harness 已修正的两个缺陷

1. **分类器**：原按异常**类型**判断内部错误，导致 Guava `Preconditions` 守卫被误报为缺陷。
   改为按**抛出点**判断（栈顶在 `com.google.common.base.Preconditions` 即为刻意守卫）。
   已同步修正 `AnalyzedRoundTripCheckerTest`，并回填 P0.5 报告 §4.2。
2. **不动点差异未分类**：已加入 `firstDiffWindow`/`diffShape`，按差异形状聚类
   （上一轮全量运行未编入此改动，5090 条 `FIXPOINT_MISMATCH` 仍未拆分）。

## 尚未修复的 harness 缺陷

**语法可达性闸门缺失**——`TreeNode.setChild` 能造出 parser 无法产生的 AST，
是 4 个伪影签名的根源。计划做法：对"契约型子节点位置"建 denylist
（`ExistsPredicate`、`LambdaFunctionExpr`、`Subquery`、`InPredicate` 的子查询位、
`time_slice` 的 boundary 参数位等），这些位置不参与替换。

## 后续

| # | 项 |
|---|---|
| 1 | 缺陷 2、3 与 P0.5 §4.1 的 TEMPORARY PARTITION 同属"反解产出不可分析 SQL → 视图定义损坏"，可合并成一个 PR |
| 2 | 缺陷 1 属 analyzer 类型处理，单独提 |
| 3 | 补语法可达性闸门后重跑，量化伪影消除效果 |
| 4 | 分诊剩余 9 个低频签名 |
