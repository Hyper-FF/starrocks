# 上游缺陷漏检审计 2026-09-21

问题：最近 main 上修掉的缺陷，很多不是我们 fuzzer 找到的。逐条归因，找出该改 fuzzer 的哪一层、
该改流程的哪一环。**下面所有数字都是实测的**（语料 `/home/public/sr-fuzzer/emit` 760 组，
上游窗口 `starrocks/main` 2026-08-15 → 09-21）。

## 0. 靶面有多大

| | 条数 |
|---|---|
| 窗口内 `[BugFix]` 提交 | 233 |
| 其中我们自己的（Hyper-FF） | 57 |
| 其余里落在**查询路径**（planner / BE exec / SQLTest）的 | ~40 |
| 其余（lake/publish/compaction、load、外部 catalog、权限/RPC/配置/metrics） | ~136 |

**先把靶面说清楚：136 条结构上就不是 SQL 查询 fuzzer 能碰的**（湖仓事务、compaction、
导入、Hive/Iceberg/Paimon/ES、权限、RPC、日志级别）。拿它们当漏检算在 fuzzer 头上没有意义。
真正的靶面是那 ~40 条 / 5 周 ≈ **每周 8 条**。

## 1. 逐条归因：第一道拦住我们的闸

| 闸 | 含义 | 命中的上游缺陷 |
|---|---|---|
| **G1 形状产不出** | 变异器/语料根本产不出那个 SQL 结构 | #79298 FULL OUTER JOIN USING、#77970 NLJoin 空匹配丢探测行、#77296 GROUPING SETS 无可执行计划、#78979 prepared 参数绑错、#78645/#78915 嵌套 lambda hoist、#78006 GLM project 隐藏列、#78488 dict 化 ARRAY |
| **G2 schema 形态产不出** | setup 从不被变异，表模型冻结在种子里 | #78746 年份 0000 建分区、#78359 聚合表 ADD COLUMN 歧义、#77818 GIN index property、#76453 递归 CTE CTAS 列、#78090 虚拟列名遮蔽、#79096/#79079/#78296 Variant 全族 |
| **G3 数据形态产不出** | 值域/物理布局不可达 | #78722 substr 非法 UTF-8 跨行字节、#78480/#78479 unnest offset >2^31、#78376 map/array 子列 nullable、#78685 parquet FLBA 字典 |
| **G4 oracle 判不出** | 形状能产出，但没有判据说它算错了 | #79037 months_diff/years_diff、#78238 float→int 有损 cast 归约、#77847 encode_sort_key 常量参数 |
| **G5 流程丢了** | **我们找到了，没落地** | #78946 LIST 多值分区含 NULL（TLP 命中 5 次，08-27 分诊成"待复现"，09-10 被 JasonQu1 抢先）、#78358 yearweek 崩溃（记忆里 🔴未修，09-01 被 linjiayu1025 修掉） |

## 2. 每道闸的实测证据

### G1 形状

```
NOT IN (subquery)          0 / 760   0.0%     <- 整族为零
IN     (subquery)          0 / 760   0.0%     <- 整族为零
USING(...) join           16 / 760   2.1%     全部来自种子，变异器产出 0
GROUPING SETS              4 / 760   0.5%
ROLLUP / CUBE              8 / 760   1.1%
QUALIFY / PREPARE          0 / 760   0.0%
FULL OUTER JOIN           21 / 760   2.8%
```

`IN/NOT IN (subquery)` **为零**是唯一一个"整族不可达"的缺口：子查询→join 重写、
null-aware anti join、相关列绑定这三片代码，fuzzer 从来没有踏进去过一次。#77970 就在那里。
变异器也没有 **join 类型互换**算子（`grep JoinOperator` 在整个 fuzz 包里 0 命中）——
inner/left/right/full/semi/anti 之间、ON 与 USING 之间从不互换。

### G2 schema

setup 文件从不被变异（M8 `StatementSequenceMutation` 至今是死代码），表模型冻结在种子上：

```
generated column        3 / 760   0.4%
agg_state 列            5 / 760   0.7%
PARTITION BY LIST      15 / 760   2.0%
排序键 ORDER BY(...)   30 / 760   3.9%
bitmap/hll/percentile  24 / 760   3.2%
AGGREGATE KEY          35 / 760   4.6%
```

对照我们自己找到的缺陷清单：多列 RANGE 分区 BE 裁剪、LIST+NULL 裁剪、生成列分区裁剪、
聚合表 JSON 子字段、agg_state 子字段裁剪——**全部是 schema 形态依赖的**，而 schema 恰恰是
唯一不变异的那一维。这不是巧合，是缺口的形状。

### G3 数据

已记录在 [[fuzzer-blind-to-data-layout]]，这次新增两条实证：`gen_data.py` 的字符串生成
从不产出**非法 UTF-8**（#78722 的必要条件），数组 offset 也永远到不了 2^31（#78480）。

### G4 oracle

函数宇宙 555 个（gensrc + FunctionSet），语料里出现过的 337 个（60.7%）——但"出现过"不等于
"被判过对错"：`months_diff|years_diff` 全语料 **1 次**，`str_to_date|yearweek` **7 次**。
更要命的是**即使产出了也判不出**：现有 oracle 只有崩溃、TLP、knob 差分，
**没有任何判据能说"这个函数算错了"**。#79037 就算天天产出也抓不到。

### 附带发现：差分 knob 池 53 个，全是 planner 侧，执行粒度 0 个

```
chunk_size / pipeline_dop / enable_spill / spill_mode /
enable_tablet_internal_parallel / tablet_internal_parallel_mode /
enable_global_late_materialization / enable_pipeline_level_shuffle
```

以上 8 个 session 变量**都存在且可设**（`chunk_size` 是 INVISIBLE 不是 READ_ONLY，默认 4096），
一个都不在池里。整类"批次边界 / 并行度 / spill 路径"的错结果因此没有判据。

## 3. 建议（按 命中缺陷数 ÷ 改动成本 排序）

### A. 执行粒度差分臂（1 行 knob 池改动，最高性价比）

往 `DIFF_KNOBS` 加：

```
set chunk_size = 255                     # 批次边界 -> #78722 #79007 #78685 + 我们的 issue78300 延迟物化
set pipeline_dop = 1                     # 并行度 -> EXCEPT/INTERSECT 分区错位、skew join、array_sort Status 竞争
set enable_spill = true|set spill_mode = force   # spill 路径 -> #79085 族
set enable_tablet_internal_parallel = false
set enable_global_late_materialization = false
```

这类 knob 比现有 planner knob **更干净**：它们不改计划语义，只改执行切分，所以理论上
假阳性率更低。上线前必须过 `validate_knobs`（见 incident 8：服务器不认的 knob 比没有还糟）。

### B. 差分采样偏置

现在 53 个里随机抽 4 = 单条语句 7.5% 命中率。改成**条件必带**：
语句含 `regexp` → 必带 `push_down_heavy_exprs`；含 lambda → 必带 `enable_lambda_pushdown`；
含字符串函数 → 必带 `chunk_size`；含聚合/join → 必带 `spill`。这条记忆里提过两次没做。

### C. 三个结构性生成缺口

- **C1 子查询谓词算子**（唯一整族为零）：`IN/NOT IN/ANY/ALL (subquery)` + 相关化。
  一次打开 subquery-to-join 重写、null-aware anti join、相关列绑定三片从未被访问的代码。
  同时能复现我们自己产不出的 [[correlated-exists-union-npe]] / [[expressionmapping-on-clause-outer-column]]。
- **C2 join 形态变异**：join type 互换 + ON↔USING 互换 + 多列 USING。→ #79298。
- **C3 schema 变异器（M8 接线）**：同一份数据生成多个表模型变体（DUP/AGG/PK、buckets 1↔N、
  加排序键、加生成列、RANGE↔LIST↔表达式分区、分区值含 NULL）。
  ⚠️ 这条**同时是一个新 oracle**：*同数据不同 schema，同一查询结果必须相等*。
  我们自己找到的整个分区裁剪/BE prune 家族，用这个判据都是一轮就出。

### D. 函数语义 oracle（打 G4）

- **D1 常量折叠 vs 运行时差分**：同一表达式，一份喂常量（FE 折叠），一份喂等值的列（BE 执行），
  结果必须相等。现成基础设施见 [[checker-mode-leak-cast-order]]。直接命中 #78238。
- **D2 函数等价对表**（变形 oracle）：`months_diff(a,b)` ≡ `timestampdiff(MONTH,b,a)`、
  `substr` ≡ `left/right` 组合、`date_trunc` ≡ `date_format` 截断……几十条即可。命中 #79037。
- **D3 覆盖计量**：把 555 个函数的出现次数做成覆盖图的一维，低于阈值的进定向生成队列。

### E. 流程（我判断这是当前**最高**的边际收益，且成本最低）

- **E1 未结 mismatch 当轮结项**：TLP/diff 命中当轮就复现 + 开 issue/PR，禁止写"待复现"。
  去重键加表名与分区形态；同签名 ≥3 次不同组自动升级告警。（#78946 的直接教训。）
- **E2 存量清仓**：`fixes-fixed-unopened-pr-index` 里约 45 条**已修、PR 未开**。
  被抢两次（#78358、#78946）说明这些缺陷别人也会碰到——存量的期望价值是一个真缺陷 + 一次抢先。
  先批量核对哪些上游还没修，把那批开出去。
- **E3 每周对齐脚本**：把上周 main 的 `[BugFix]` 与我们的 findings/memory 做匹配，输出三列
  「提前找到并提了 / 找到没提 / 完全没找到」，第三列逐条归因到 G1-G4，变成下周 fuzzer 待办。
  也就是把今天这份分析自动化。

## 4. 一句话结论

漏检不是"fuzz 得不够久"。四道闸里 **G1/G2 是生成器结构性缺口**（子查询谓词整族为零、
schema 从不变异），**G4 是判据缺失**（没有任何函数语义 oracle），
而 **G5 说明我们的发现率已经够了、落地率才是瓶颈**——被抢先的两条都是我们先找到的。

---

## 5. 已实施（2026-09-21，`/home/public/sr-fuzzrebase`，三个提交）

⚠️ 开发树是 **`/home/public/sr-fuzzrebase`**（branch `fuzz-tool-20260909`，1662 行），不是
`/home/public/sr-fuzzer`（停在 08-21，1563 行，缺 `diff_run_incomplete`）。本报告第 2 节的
knob 池数字两棵树一致，但改动只能落在 fuzzrebase。

| 提交 | 内容 |
|---|---|
| `eb5d6397d12` | 7 个执行粒度 knob 进池（53→60）；`DIFF_KNOB_BIAS` 偏置采样；knob 侧补 `DIFF_MAX_BYTES` 截断守卫；`diff_skippable` 加非确定性聚合 + 无序窗口 |
| `c34cd020cd0` | 偏置 knob 不参与启动器的按实例劈分；`select_knobs` 与本实例实际持有的池取交集 |
| `6a4893b8945` | 运行目录自带 `restart.sh`（静态脚本，从 `run.conf` 自解，无模板） |

**实测验证**
- 离线 30 条断言全绿（`select_knobs` / `diff_skippable` / `window_unordered`）。
- 60 个 knob 全部被真 FE 接受（dev1 ns0916s16，`nsenter` 进 netns）。
  ⚠️ **`force` 是 SR 保留字**：`set spill_mode = force` 被拒，必须 `= 'force'`。
  这条正是 `validate_knobs` 存在的理由——不验证就是一个永远返回空、被 oracle 读成"一致"的 knob。
- 50000 行真表冒烟：7 个新 knob 的结果与 baseline **逐字节相同**（不会凭空造假阳性）。
- 模拟启动器劈分：60 knob 劈成 3 份 → 每份 24–26 个且 8 个偏置 knob 一个不缺；
  三份池上 `select_knobs` 均返回 4 个、substr 语句必抽 chunk_size、从不点名池外的 knob。

**部署状态**：dev1 `clusterfuzz-ns0916s16` 的 `clusterfuzz.run.sh` / `allknobs.txt`(60) /
`knobs_0.txt`(33) / `knobs_1.txt`(35) 已就位（原件备份为 `*.bak-20260921`）。
运行中的两个实例仍跑旧脚本旧 knob（`mv` 换的是 inode，老进程不受影响），
**需重启实例才生效**——这一步权限被拦，尚未执行。dev2 `ns0916s17` 未动。

### 顺带修掉的三个"名存实亡的补救措施"
1. `watch_cf.sh` 告警指向的 `restart_instances.sh` 里 `R=` 硬编码到早已不存在的 `clusterfuzz-run`；
2. 它的 `AUTO_RELAUNCH` 路径 exec 的是 `clusterfuzz.next.sh`——运行目录里叫 `clusterfuzz.run.sh`
   （这正是该文件自己在 `running_instances` 上方记录过的坑，grep 侧修了、relaunch 侧没修）；
3. 该路径不传 `DIFF_KNOBS`，重启后的实例会静默跑满池而不是自己那 1/N。

### 下一步（按第 3 节的排序）
A/B 已落地 → 观察 1–2 天：看 `diff_bad` 里执行粒度 knob 的占比、`diff_skipped` 是否因为新黑名单
上升（预期上升，那是噪声被挡住）。然后 **E2 存量清仓** 与 **C1 子查询谓词算子**。
