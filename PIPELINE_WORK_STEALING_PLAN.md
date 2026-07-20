# StarRocks BE Pipeline Work-Stealing 设计与实施 PLAN

> 目标：在 BE Pipeline 引擎里增加通用的 work-stealing 能力，让空闲 Driver 能主动窃取 sibling Driver 的待处理工作单元，缓解数据倾斜带来的尾延迟。
>
> 范围：只改 BE 侧；不改 FE plan；对现有查询默认无行为变化（开关默认关）。
>
> 注意：本仓库 BE 已模块化重组，§8/§9 引用的 `be/src/exec/pipeline/...` 为旧布局，落地时以实际路径为准（见文末「路径校准」）。

---

## 1. 背景与现状

### 1.1 现有调度机制

StarRocks BE Pipeline 已有的调度基础设施：

| 层次  | 组件  | 是否具备 work-stealing |
| --- | --- | --- |
| Executor 线程 | `QuerySharedDriverQueue`（8 级时间片优先级） + 每线程 `local_driver_queue`（TTL 1ms） | ❌   |
| WorkGroup | `WorkGroupDriverQueue`（CFS vruntime 调度） | ❌   |
| Scan 源 | `MorselQueue`（Fixed / Dynamic / Split / BucketSequence） | 只有 FE `append_morsels`，无 driver 间偷 |
| Local Exchange | `Shuffle / Ordered / AdaptivePassthrough` Partitioner | ❌（静态分发策略） |
| 自适应 DOP | `CollectStatsSource/Sink` → 运行时调整下游 DOP | ❌（粗粒度预分配） |
| 阻塞唤醒 | `PipelineDriverPoller`（轮询）或 `EventScheduler`（Observable 推送） | ❌   |

### 1.2 数据倾斜的常见来源

1. **Scan 侧**：tablet 大小差异、hot key 分布不均导致 `MorselQueue` 长短不齐。
2. **Local Exchange (PARTITION)**：hash key 倾斜导致下游某 source buffer 堆积。
3. **HashJoin Probe**：shuffle 后 partition key 倾斜，某个 probe driver 负载显著高于 sibling。
4. **BlockingAggregate**：group key 倾斜。

---

## 2. 核心设计

### 2.1 关键判断

- **Driver 迁移不可行**：Driver 持有算子内部状态（hash 表、agg 表、shuffle buffer 等），无法在线程/driver 间迁移。
- **"通用算子窃取"的正确含义**：偷的是**工作单元（chunk/morsel）**，不是 operator 实例。thief 在自己算子链上执行偷来的 unit。
- **窃取需遵守分区合约**：post-shuffle 的算子若假设 `driver_seq == partition_id`，随意跨 driver 会破坏结果正确性。

### 2.2 可偷前缀（Stealable Prefix）

给每个 Operator 打标签 `is_stealable() -> bool`。Pipeline 构建期计算：

```
steal_barrier_idx = 第一个 is_stealable() == false 的算子下标
```

- `steal_barrier_idx == 0` → 整条 pipeline 不可偷
- `steal_barrier_idx > 0` → thief 可以偷 Source 出的 unit，在自己实例上执行到 barrier 为止

| Operator | is_stealable | 理由  |
| --- | --- | --- |
| Scan / JDBCScan / MysqlScan | ✅   | morsel 无状态 |
| Project / Filter / Decode / CastExpr | ✅   | 无状态 |
| LocalExchangeSource (Passthrough / Random / AdaptivePassthrough) | ✅   | Sink 分发不依赖 driver_seq |
| LocalExchangeSource (PARTITION) | ✅（需 §2.4 配合） | chunk 带 partition_id 走 partition-aware probe |
| ExchangeSource (broadcast / random) | ✅   | 同上  |
| HashJoinProbe (broadcast) | ✅   | build 侧共享 |
| HashJoinProbe (shuffle / colocate) | ✅（需 §2.4 配合） | 借 peer 的 hash_table 只读查表 |
| Pre-Aggregation（下游还有 final agg） | ✅   | 下游 merge |
| — barrier 之后 — |     |     |
| HashJoin Build | ❌   | per-driver hash 表 |
| BlockingAggregate（final） | ❌   | per-driver group 表 |
| Sort / TopN / Window（分区内有序） | ❌   | 跨 chunk 有序状态 |
| LocalShuffleSink | ❌   | 下游按 driver_seq 分区 |
| ExchangeSource (shuffle hash) | ❌   | 上游按 driver_seq 分区 |

### 2.3 偷的工作单元与传递介质

```cpp
struct StealUnit {
    ChunkPtr  chunk;
    MorselPtr morsel;                // 二选一
    int32_t   partition_id = -1;     // -1 表示无分区语义
};
```

- Scan 源偷 morsel
- LocalExchangeSource 偷 chunk，带 `partition_id = victim.driver_sequence`
- 其余无分区的偷 chunk，`partition_id = -1`

### 2.4 HashJoinProbe 的 partition-aware probe

> **A 偷 B buffer 里的 chunk 时把 partition_id 一起带过来；A 的 JoinProbe 在做 lookup 那一步用 peer_joiner[partition_id]->hash_table，其他所有 probe 准备工作仍在 A 自己实例上完成。**

必要条件：

1. **post-build 哈希表只读**：build 完成后 `_hash_table` 不再变，A/B 并发读安全。
2. **all-builds-ready barrier**：steal-enabled probe pipeline 的 precondition 从 "自己 build 完成" 升级为 "所有 sibling build 完成"。
3. **probe 侧准备本地化**：`_probe_expr_ctxs` / `_probe_state`（key columns / hash values / cursor / match buffer）全部在 thief 自己的 HashJoiner 上做，只 lookup 步骤用 peer 的 hash_table。
4. **join 类型白名单（首版）**：INNER / LEFT OUTER / LEFT SEMI / LEFT ANTI；RIGHT/FULL OUTER 需要原子化 matched 位图，后续再开。
5. **chunk 期间 ht 不漂移**：per-chunk 绑定 `_current_probe_ht`，直到 chunk 流干才允许换。

### 2.5 与 Event Scheduler 结合

在 `PipelineObserver` 加第 4 个事件位：`STEAL_AVAILABLE_EVENT = 1u << 3`。

```
Victim source 累积 backlog 越过阈值
  └─ SourceOperatorFactory::steal_observes.notify_source_observers()
      └─ Thief.PipelineObserver.steal_trigger()  [置 STEAL bit]
          └─ PipelineObserver::_do_update()
              └─ Driver::try_steal_from_siblings()
                  └─ 成功 → EventScheduler::force_reschedule(driver)
                              ├─ 清 thief 在 peers 上的 _steal_observes 订阅
                              └─ driver_queue.put_back(driver)
```

Poller 模式（未启用 EventScheduler）：`PipelineDriverPoller::run_internal` 在 `!is_not_blocked()` 分支里直接调 `driver->try_steal_from_siblings()`，成功即 `set_driver_state(READY)`。轮询延迟为 poller tick（~10ms 级）。

---

## 3. 关键接口

### 3.1 `Operator` / `OperatorFactory`

```cpp
class OperatorFactory {
public:
    virtual bool is_stealable() const { return false; }   // 默认 false
};
```

### 3.2 `SourceOperator` / `SourceOperatorFactory`

```cpp
class SourceOperator {
public:
    virtual bool                support_steal() const                   { return false; }
    virtual size_t              stealable_backlog() const               { return 0; }
    virtual StatusOr<StealUnit> try_steal_unit()                        { return StealUnit{}; }
    virtual Status              accept_stolen_unit(StealUnit u,
                                                   PendingStealUnit* slot);
    bool has_output() const override {
        return _pending_slot_has_value() || _source_has_output_impl();
    }
};

class SourceOperatorFactory {
public:
    Observable& steal_observes() { return _steal_observes; }
    const std::vector<SourceOperatorFactory*>& siblings() const;
private:
    Observable _steal_observes;
};
```

### 3.3 `PipelineDriver`

```cpp
class PipelineDriver {
public:
    bool try_steal_from_siblings();
    void subscribe_peer_steal_observers();
    void unsubscribe_peer_steal_observers();
    bool steal_enabled() const;
private:
    PendingStealUnit _pending_steal_unit;
    std::vector<SourceOperatorFactory*> _steal_subscriptions;
    bool _steal_attempted_this_round = false;
    RuntimeProfile::Counter* _steal_success_counter = nullptr;
    RuntimeProfile::Counter* _steal_fail_counter    = nullptr;
    RuntimeProfile::Counter* _stolen_rows_counter   = nullptr;
};
```

### 3.4 `Pipeline`

```cpp
class Pipeline {
public:
    void   compute_steal_barrier();
    size_t steal_barrier_idx() const { return _steal_barrier_idx; }
private:
    size_t _steal_barrier_idx = 0;
};
```

### 3.5 `PipelineObserver`

```cpp
static constexpr uint32_t STEAL_AVAILABLE_EVENT = 1u << 3;
class PipelineObserver {
public:
    void steal_trigger();
};
```

### 3.6 `HashJoinProbe` 相关

```cpp
class HashJoiner {
public:
    Status                     prepare_probe(RuntimeState*, const ChunkPtr&);
    Status                     lookup_with(const HashTable&, ProbeState&);
    StatusOr<ChunkPtr>         emit_output_with(const HashTable&, ProbeState&);
private:
    HashTable  _hash_table;
    ProbeState _probe_state;
};

class HashJoinProbeOperatorFactory {
public:
    HashJoiner* sibling(int partition_id) const;
    bool        all_builds_ready() const;
private:
    std::vector<HashJoinerPtr>  _sibling_joiners;
    std::atomic<int>            _builds_ready_count{0};
};

class HashJoinProbeOperator {
public:
    Status push_stolen_chunk(RuntimeState*, ChunkPtr, int partition_id);
private:
    const HashTable* _current_probe_ht = nullptr;
};
```

---

## 4. Driver 侧实现

### 4.1 `process()` 让出点挂钩

在 num_chunks_moved==0 分支里、set INPUT_EMPTY / LOCAL_WAITING 之前插入 steal 尝试：

```cpp
} else if (!source_operator()->is_finished() && !source_operator()->has_output()) {
    if (!should_yield && try_steal_from_siblings()) {
        _steal_attempted_this_round = false;
        continue;
    }
    if (source_operator()->is_mutable()) {
        set_driver_state(DriverState::LOCAL_WAITING);
        COUNTER_UPDATE(_yield_by_local_wait_counter, 1);
    } else {
        set_driver_state(DriverState::INPUT_EMPTY);
        COUNTER_UPDATE(_block_by_input_empty_counter, 1);
    }
}
```

要点：
- `should_yield == true` 时**不偷**（时间片到期，必须让 CPU）。
- 偷到后 `continue`；下一轮 for 循环里 source `has_output()` 返回 true，`pull_chunk` 自然吐出偷来的 unit。
- `_steal_attempted_this_round` 只在单轮外层 while 内节流。

### 4.2 `try_steal_from_siblings()`

```cpp
bool PipelineDriver::try_steal_from_siblings() {
    if (!steal_enabled()) return false;
    if (_pending_steal_unit.valid()) return true;
    if (_steal_attempted_this_round) return false;
    _steal_attempted_this_round = true;

    auto* me_src = source_operator();
    if (!me_src->support_steal()) return false;

    auto peers = me_src->siblings();
    thread_local std::mt19937 rng{std::random_device{}()};
    std::shuffle(peers.begin(), peers.end(), rng);

    const size_t threshold =
        _runtime_state->query_options().pipeline_steal_backlog_threshold;

    for (auto* peer : peers) {
        if (peer->stealable_backlog() < threshold) continue;
        auto unit_or = peer->try_steal_unit();
        if (!unit_or.ok() || !unit_or->valid()) continue;

        Status st = me_src->accept_stolen_unit(std::move(*unit_or),
                                               &_pending_steal_unit);
        if (!st.ok()) { COUNTER_UPDATE(_steal_fail_counter, 1); continue; }

        COUNTER_UPDATE(_steal_success_counter, 1);
        if (_pending_steal_unit.chunk) {
            COUNTER_UPDATE(_stolen_rows_counter,
                           _pending_steal_unit.chunk->num_rows());
        }
        return true;
    }
    COUNTER_UPDATE(_steal_fail_counter, 1);
    return false;
}
```

### 4.3 `steal_enabled()`

```cpp
bool PipelineDriver::steal_enabled() const {
    if (!_runtime_state->query_options().enable_pipeline_work_stealing) return false;
    if (_state == DriverState::PRECONDITION_BLOCK)  return false;
    if (_pipeline->steal_barrier_idx() == 0)        return false;
    return true;
}
```

### 4.4 订阅 / 退订

```cpp
void PipelineDriver::subscribe_peer_steal_observers() {
    auto* me_factory = source_operator()->_source_factory();
    for (auto* peer : me_factory->siblings_of(_driver_id)) {
        peer->steal_observes().add_observer(_runtime_state, &_observer);
        _steal_subscriptions.push_back(peer);
    }
}
void PipelineDriver::unsubscribe_peer_steal_observers() {
    for (auto* peer : _steal_subscriptions) {
        peer->steal_observes().remove_observer(&_observer);
    }
    _steal_subscriptions.clear();
}
```

调用点：
- `EventScheduler::add_blocked_driver` 末尾（对 steal-enabled + INPUT_EMPTY 的 driver 订阅）
- `EventScheduler::try_schedule` 在 `put_back` 前退订
- Driver `finalize` / `cancel` 路径兜底退订

---

## 5. 正确性与风险

| 风险  | 缓解  |
| --- | --- |
| 偷跨过 partition barrier 结果错 | `steal_barrier_idx` 静态计算 + 白名单 `is_stealable()` + 单测 |
| RIGHT/FULL OUTER 写 build 侧 matched 位 | 首版禁用，后续用原子位图 |
| Colocate / bucket shuffle join | `BucketSequenceMorselQueue::support_steal() == false`，FE plan 标 `is_colocate` 的 fragment 强制关 |
| Runtime filter 归属 | 同 fragment 内 sibling 共享 RFBuilder，无额外处理 |
| Chunk 顺序敏感（TopN 前 project 等） | Operator 加 `requires_input_order()`，此类前缀不可偷 |
| Adaptive DOP 与 stealing 交互 | CollectStats 阶段 pipeline 未 materialize DOP，此期禁用 stealing |
| 内存计费 | thief 处理产生的中间态计入 thief 自己 memtracker，无需迁移 |
| 阈值抖动反复 notify | `stealable_backlog()` 越过阈值时置 `_steal_notified`；消费到阈值以下再复位 |
| 订阅风暴（N thief × M peer） | 高 DOP 场景可退化为随机订阅 K=2 个 peer |
| 多 thief 抢同一 peer | peer 内部 CAS/lock 保证单赢家，输者记 fail counter |
| Driver finalize 时 slot 非空 | DCHECK + 告警日志；fragment 层会看到输入未耗尽（视为异常） |

---

## 6. 可观测

### 6.1 Session / BE Config
- `enable_pipeline_work_stealing`（默认 false）
- `pipeline_steal_backlog_threshold`（默认 2）
- `pipeline_steal_cooldown_ns`（默认 100µs）
- `pipeline_steal_max_per_round`（默认 1）

### 6.2 Profile
Pipeline 级 `WorkStealing` 段：
- `StealAttempts / StealSuccess / StealFail`
- `StealedRows / StealedBytes`
- `StealSkippedBarrier`
- `StealLatencyNs`

### 6.3 BE Metrics
- `pipeline_steal_success_total`
- `pipeline_steal_fail_total`

---

## 7. 分阶段落地

### Phase 0 — 度量与开关（前置，1-2 天）
- 只加 config 项、profile 计数器、`SourceOperatorFactory::sibling()` 反查接口
- 默认关，行为无变化；独立 PR 可合

### Phase 1 — Driver + Source 骨架（1 周）
- `PipelineDriver::try_steal_from_siblings` 全套字段与方法（默认 `steal_enabled()==false` 所以 no-op）
- `SourceOperator` / `Factory` 加空 override 接口
- `Pipeline::steal_barrier_idx` 计算
- `PipelineObserver::STEAL_AVAILABLE_EVENT` bit + `steal_trigger`
- `EventScheduler` 加 `force_reschedule`、订阅生命周期钩子
- `PipelineDriverPoller` 加 steal fallback（默认关不触发）
- **不接任何具体 Source**，编译通过 + UT 全绿即可合

### Phase 2 — Scan 接入（首个可偷 Source，1 周）
- `MorselQueue::try_steal / unassigned_size`（`FixedMorselQueue` / `DynamicMorselQueue` 优先）
- `ScanOperator::support_steal / try_steal_unit / accept_stolen_unit`
- `BucketSequenceMorselQueue::support_steal() == false`
- 开 session 变量做 tablet 倾斜 benchmark

### Phase 3 — HashJoinProbe partition-aware（重头，2 周）
- **Step A**（纯重构）：`HashJoiner` 抽 `prepare_probe / lookup_with / emit_output_with`；散落 probe 暂存字段打包为 `ProbeState` struct
- **Step B**：`HashJoinProbeOperatorFactory::_sibling_joiners` + `all_builds_ready` barrier；`HashJoinBuildOperator::set_finishing` 完成时递增计数
- **Step C**：`push_stolen_chunk(chunk, partition_id)` + `_current_probe_ht` per-chunk 绑定 + join 类型白名单（只放 INNER / LEFT_*）

### Phase 4 — LocalExchange 接入（1 周）
- `PartitionLocalExchangeSourceOperator::support_steal / try_steal_unit`（尾端 pop）
- Passthrough / Random / AdaptivePassthrough 也 override
- Ordered / KeyPartition 强制关

### Phase 5 — 网络 ExchangeSource（1 周）
- 非 shuffle 模式的 ExchangeSource 允许从接收队列偷

### Phase 6 — Pre-Aggregation（1 周）
- Pre-Agg `is_stealable = true`，验证下游 final-agg merge 正确性

### Phase 7 — 后续增量（视需要）
- RIGHT/FULL OUTER + 原子化 matched 位图
- Executor 空闲线程辅助 steal（Cilk 风格 backoff）

---

## 8. 文件级改动清单（Phase 0-2 首批 PR）

> 路径以文末「路径校准」为准。

| 逻辑组件 | 变动 |
| --- | --- |
| BE config | 加 4 个 BE config |
| query_options + FE thrift | 加 3 个 session variable |
| `OperatorFactory` | `virtual bool is_stealable()` |
| `SourceOperator/Factory` | `support_steal/try_steal_unit/accept_stolen_unit/stealable_backlog` + `_steal_observes` + `siblings()` |
| `Pipeline` | `steal_barrier_idx` + `compute_steal_barrier` |
| `pipeline_builder` | 调 `compute_steal_barrier`、填 factory sibling 列表 |
| `PipelineDriver` | 5 字段 + 4 方法 + `process()` 挂钩 + profile counter |
| `schedule/observer` | `STEAL_AVAILABLE_EVENT` bit + `steal_trigger` + `_do_update` 分派 |
| `schedule/event_scheduler` | `force_reschedule` + 订阅生命周期 |
| `pipeline_driver_poller` | Poller 模式 fallback |
| `scan/morsel` | `MorselQueue::try_steal / unassigned_size` |
| `scan/scan_operator` | Scan Source override |

---

## 9. 验证

### 9.1 单元测试
- morsel_queue 并发 `try_steal` 正确性（morsel 数守恒、无重复）
- pipeline_driver：`try_steal_from_siblings` 与 `process()` yield 分支交互
- event_scheduler：`STEAL_AVAILABLE_EVENT` 唤醒路径
- hash_joiner：`prepare_probe / lookup_with / emit_output_with` 拆分后结果等价性

### 9.2 集成测试
- `test/` SQL 全回归（开关默认关，无行为变化）
- 开启开关跑主 SQL 回归，结果字节一致（按 order by 排序后）

### 9.3 性能基准
- TPC-H SF100 制造 tablet 大小偏差（Phase 2）
- TPC-DS Q95 / hash join 热点（Phase 3）
- 微基准：`SELECT count(*) FROM t GROUP BY skewed_key`（Phase 4-6）
- Profile 对比 enable/disable

### 9.4 harness 校验
```bash
python3 build-support/check_be_module_boundaries.py --mode full
python3 build-support/render_be_agents.py --check
./run-be-ut.sh --build-target pipeline_test
```

---

## 10. 落地建议
1. 第一刀只合 **Phase 0 + Phase 1**（骨架 + 度量），零行为改动。
2. 第二刀 **Phase 2**（Scan 接入），最高 ROI。
3. **Phase 3 拆成三个子 PR**（Step A/B/C），Step A 纯重构可先合。
4. Phase 4-6 视实测收益定优先级。
5. Phase 7 长期规划。

每步保证 **默认关闭 + UT 全绿 + 现有 profile 无字段回退**。

---

## 路径校准（本仓库模块化重组后的真实路径）

BE 已拆成三处 pipeline 代码家，依赖单向 `exec` → `exec_primitive`（`exec_primitive` **禁止** include `exec/`，由 `build-support/check_be_module_boundaries.py` + `be/module_boundary_manifest.json` 强制）。

### 关键边界约束（决定接口放哪）
- `exec_primitive` 模块只能 include：`exec_primitive/ exprs/ runtime/ column/ types/ common/ base/ gutil/ gen_cpp/`；**禁止** `exec/`（含 `exec/pipeline/fragment_context.h`）、`storage/`、`service/`、`util/`、`compute_env/workgroup/` 等。
- 推论：**抽象窃取接口**放 `exec_primitive`（无需上依赖）；**编排/调度逻辑**放 `exec/runtime`+`exec/pipeline`（可自由下依赖）。

### 逐组件真实路径

| 计划里的旧路径 | 真实路径 | 归属模块 |
| --- | --- | --- |
| `pipeline/operator_factory.h` | `be/src/exec_primitive/pipeline/operator_factory.h` (`.cpp` 同目录) | exec_primitive |
| `pipeline/operator.h` | `be/src/exec_primitive/pipeline/operator.h` | exec_primitive |
| `pipeline/source_operator.h/.cpp` | `be/src/exec_primitive/pipeline/source_operator.{h,cpp}` | exec_primitive |
| `pipeline/schedule/observer.*`（Observable/PipelineObserver） | `be/src/exec_primitive/pipeline/primitives/pipeline_observer.{h,cpp}`（基类 `primitives/driver_observer.h`） | exec_primitive |
| 事件位定义 | `be/src/exec_primitive/pipeline/primitives/event.{h,cpp}` / `pipeline_observer.h` | exec_primitive |
| `pipeline/scan/morsel.h/.cpp`（MorselQueue 全家桶） | 旧：`be/src/exec/pipeline/scan/morsel.{h,cpp}`；新拆分：`be/src/exec_primitive/pipeline/scan/`（`morsel_queue.*`、`fixed_morsel_queue.*`、`dynamic_morsel_queue.*`、`scan_morsel.*`、`ticketed_morsel_queue.h`…） | 两处，需确认 scan_operator 实际用哪套 |
| `pipeline.h/.cpp` | `be/src/exec/runtime/pipeline.{h,cpp}` | exec/runtime |
| `pipeline_driver.h/.cpp` | `be/src/exec/runtime/pipeline_driver.{h,cpp}` | exec/runtime |
| `pipeline_builder.cpp` | 无同名文件：头 `be/src/exec/pipeline/pipeline_builder.h`，实现拆 `be/src/exec/pipeline/pipeline_builder_operators.{cpp,h}` + `be/src/exec/runtime/pipeline_builder_context.{cpp,h}` | exec/pipeline + exec/runtime |
| `pipeline_driver_queue.*` | `be/src/exec/pipeline/pipeline_driver_queue.{cpp,h}`（抽象 iface `exec_primitive/pipeline/primitives/driver_queue.h`） | exec/pipeline |
| `pipeline_driver_executor.*` | `be/src/exec/pipeline/pipeline_driver_executor.{cpp,h}`（iface `exec_primitive/.../driver_executor.h`） | exec/pipeline |
| `pipeline_driver_poller.*` | `be/src/exec/pipeline/pipeline_driver_poller.{cpp,h}` | exec/pipeline |
| `schedule/event_scheduler.*` | `be/src/exec/runtime/schedule/event_scheduler.{cpp,h}` | exec/runtime |
| scheduler 侧 driver observer | `be/src/exec/runtime/schedule/pipeline_driver_observer.{h,cpp}` | exec/runtime |
| `scan/scan_operator.h/.cpp` | `be/src/exec/pipeline/scan/scan_operator.{h,cpp}` | exec/pipeline |
| `exchange/local_exchange.*` | `be/src/exec/pipeline/exchange/local_exchange.{h,cpp}`；source 在 `local_exchange_source_operator.{h,cpp}` | exec/pipeline |
| `hash_joiner.h/.cpp` | `be/src/exec/hash_joiner.{h,cpp}`（直接在 `be/src/exec/` 下，不在 pipeline/） | exec |
| `hash_join_probe_operator.*` | `be/src/exec/pipeline/hashjoin/hash_join_probe_operator.{h,cpp}` | exec/pipeline |

### 验真结论（设计 vs 真实代码，2026-07-16 三路 Explore 核对）

#### A. 接口面（exec_primitive）
- **`is_stealable()`** — ✅ 照抄 `OperatorFactory::is_source()` / `support_event_scheduler()`（`operator_factory.h:30/90`），一行虚函数。
- **`has_output()` 网关** — ⚠️ 修正：`has_output()` 是 `Operator` 上的**纯虚**（`operator.h:105`），`SourceOperator` 未实现，各具体 source 自己 override。**不做**全仓 NVI 重构；改为**只在 opt-in（`support_steal()==true`）的 source（scan/exchange）里**把 `has_output()` 改成 `_pending_slot || 原impl`。范围收窄到个位数文件。
- **`StealUnit`** — ✅ 放 `source_operator.h`；`ChunkPtr` 已经可见（经 `operator.h` → `column/vectorized_fwd.h`），`MorselPtr` 需加 `#include "exec_primitive/pipeline/scan/scan_morsel.h"`（同模块，不破边界）。注意 `MorselPtr = std::unique_ptr<ScanMorsel>` → **`StealUnit` move-only**，传递必须 `std::move`。
- **现成基础设施可复用** — 🎁 `SourceOperatorFactory` 已有 `_upstream_sources`、union-find（`_group_parent`/`group_leader`/`union_group`）、两个 `Observable`（`_sources_observes` 工厂级 + `_observable` 算子级）、`with_morsels()` 谓词。

#### B. 事件/唤醒（跨 exec_primitive + exec/runtime 两模块）
- **事件位** — ✅ `1<<3` 空闲（现最高 `CANCEL_EVENT = 1<<2`，见 `exec/runtime/schedule/pipeline_driver_observer.h:62-64`）。注意：位定义 + `_do_update` 分派在 **`exec/runtime` 的 `PipelineDriverObserver`**（`.cpp:56-93`），抽象 trigger 在 `exec_primitive` 的 `PipelineObserver`（`pipeline_observer.h:30-34`，只有 `source/sink/cancel/all/runtime_filter_timeout_trigger`，**无事件位常量**）。
- **`steal_trigger()`** — ⚠️ 修正：抽象层必须给**默认空实现 `{}`**，不能纯虚——否则 spill 等所有 `PipelineObserver` 子类（如 `compute_env/spill/spill_observable.h`）都被迫实现。
- **唤醒可复用现成链路** — 🎁 修正 §2.5：peer 有 backlog 时可直接调阻塞 driver 现成的 `source_trigger()` → `_do_update` → `try_schedule` → `put_back`，**Phase 1 不必新建 `STEAL_AVAILABLE_EVENT` bit + `force_reschedule`**。专用 STEAL bit 仅为减少 spurious wakeup 的后续优化，可延后到收益需要时。
- **`Observable` API** — `add_observer(RuntimeState*, PipelineObserver*)`（仅当 `enable_event_scheduler()` 记录）、`detach_observers()`（**无单点 remove_observer**，只有批量 detach）、`notify_source_observers()` 等。§4.4 的逐 peer `remove_observer` 不存在，退订只能整批 `detach_observers()` 或改用现成的 group observable。

#### C. 编排/调度（exec/runtime + exec/pipeline）
- **process() 挂钩** — ✅ 真实分支 `pipeline_driver.cpp:560-575`，`if (num_chunks_moved==0 || should_yield)` 内 `!source->has_output() && !source->is_finished()`（567，`is_mutable()` 分 `LOCAL_WAITING`/`INPUT_EMPTY`）。修正 §4.1：此处是 `return _state`，偷成功后要改成 `continue` 回 `while(true)` 顶。
- **Driver 字段修正**（§3.3/§4.4 错处）— source 是 `_operators[0]`（`source_operator()`，**无 `_source_operator`**）；**无 `_pipeline`**，back-pointer 是 `_driver_observer`，它*就是* owning `Pipeline`（`Pipeline : public DriverObserver`）；`driver_sequence` 在 source op 上（`get_driver_sequence()`），driver 只有 `_driver_id`；观察者 `observer()` 返回 `PipelineObserver*`。
- **siblings 大简化** — 🎁 全 pipeline **共享一个** `SourceOperatorFactory`（`pipeline.h:53 _op_factories[0]`）；sibling 集合 = 同 `Pipeline` 的全部 `dop` 个 driver 实例，`Pipeline` 已持有 `Drivers _drivers`（`drivers()`/`mutable_drivers()`），每个 driver 经 `_driver_observer` 可达。**根本不用给 factory 加 `siblings()` 列表**——`siblings()` ≈ `_driver_observer->drivers()`。§3.2/§4.4 的订阅样板大幅砍掉。fill 点（若仍要 factory 级列表）在 `pipeline_driver_instantiator.cpp:54-70`。
- **Scheduler 钩子** — subscribe 在 `event_scheduler.cpp:add_blocked_driver`（line 61 `set_in_blocked(true)` 之后）；unsub+reschedule 在 `try_schedule` 成功块（103-108，已含 `set_in_blocked(false)` + `put_back`）。
- **Poller fallback** — `pipeline_driver_poller.cpp:158` 的 `else`（仍阻塞）分支，偷成功后照 154-156 搬 ready；Poller 仅在 `event_scheduler==nullptr` 时才收 driver。

#### D. Morsel / Scan（**颠覆 Phase 2 定位**）
- **代码归属** — 旧 `exec/pipeline/scan/morsel.{h,cpp}` 是**纯 re-export shim**（无类定义）；真实类只在 `exec_primitive/pipeline/scan/`（`morsel_queue.*`/`fixed_morsel_queue.*`/`dynamic_morsel_queue.*`）。所有改动落在 exec_primitive。
- **MorselQueue 基类** — `try_get()` 纯虚、**基类不加锁**（锁交子类）；**无 remaining/unassigned 计数**（`num_original_morsels()`/`max_dop()` 返回静态总数）。`try_steal()`/`unassigned_size()` 需按子类新增虚函数。
- **FixedMorselQueue** — lock-free 原子游标 `_pop_index.fetch_add`。偷需加**第二个从尾递减的原子游标 + 头<尾 crossover CAS**；`unassigned = _num_morsels - _pop_index`。
- **DynamicMorselQueue** — `std::deque` + `std::mutex` 前端 pop。偷即同锁 `pop_back()`，`unassigned = _size`；但 split-morsel 的 ticket-checker + 前插 owner_id 局部性语义要小心，偷尾可能破坏 split 局部性。
- **🔴 关键：per-driver vs shared 是条件性的** — `ScanNode::convert_scan_range_to_morsel_queue_factory`（`scan_node.cpp:175-189`）决定：
  - **shared-scan 模式**（`always_shared_scan()` / `enable_shared_scan` / morsels 多于 io_parallelism → `SharedMorselQueueFactory`）：所有 scan driver **已共享一个** MorselQueue，空闲 driver 已经在竞争性排空同一队列 → **morsel-level stealing 完全冗余**。
  - **individual 模式**（`IndividualMorselQueueFactory`/`BucketSequenceMorselQueueFactory`，仅 morsel 少且 uniform、或 colocate/bucket 计划）：每 driver 私有队列，无跨 driver 拉取 → **只有这里 scan-stealing 才有价值**；gate on `!factory->is_shared()`，victim 从 `IndividualMorselQueueFactory::_queue_per_driver_seq` 枚举。
- **⚠️ 战略结论**：individual 模式恰是「morsel 少且 uniform」的**低倾斜**场景，而**高倾斜**（大 tablet 差异、morsel 多）走的是**已自平衡的 shared 队列**。**Phase 2（scan）的 ROI 被大幅削弱**。真正无自平衡、静态按 `driver_seq` 分区、最需要 stealing 的是**post-shuffle 场景**：LocalExchange(PARTITION) 与 HashJoinProbe。**建议重排阶段顺序**（见下）。

### 阶段顺序修正建议
基于 D 的结论，原「Phase 2 scan = 最高 ROI」不成立。建议：
1. Phase 0 + Phase 1（骨架 + 度量 + 复用现成唤醒链路，砍掉专用 STEAL bit）——不变，先合。
2. **提前做原 Phase 4（LocalExchange PARTITION）**：post-shuffle 静态分区、无自平衡，是真实倾斜重灾区，ROI 最高。
3. **再做原 Phase 3（HashJoinProbe partition-aware）**：重头戏，Step A 纯重构可先合。
4. **scan（原 Phase 2）降级为可选**：只在 `!is_shared()` 的 individual 模式接入，收益有限，视实测再定；或直接跳过。

### 待确认项（原 5 项，验真后状态）
1. **`StealUnit` 里的 `MorselPtr`** — ✅ 已解：同模块可见，加一行 include 即可；move-only。
2. **哪套 morsel 是活的** — ✅ 已解：exec_primitive 是活的，legacy morsel.h 是 shim。
3. **事件位空间** — ✅ 已解：`1<<3` 空闲；位在 exec/runtime 的 PipelineDriverObserver。
4. **`siblings()` 由谁填** — ✅ 已解：不用新加列表，`siblings()`≈`Pipeline::drivers()`；若仍要 factory 级列表则填在 `pipeline_driver_instantiator.cpp:54-70`。
5. **参考文档** — `be/src/exec/pipeline/README.md`、`handbook/architecture/be-boundary-harness.md`、`handbook/domains/backend.md`。

剩余需在写代码时现场定的小项：DynamicMorselQueue 偷尾对 split ticket-checker 语义的影响（Phase 4/scan 才碰）。

---

## 落地进度

### Worktree / 分支
- worktree：`/home/public/sr-worksteal`，分支 `feature/pipeline-work-stealing`，**off `29d73b8d875`**（= `feature/be-mem-psi-metrics` 的父提交，含 exec_primitive 模块重组、在 upstream `starrocks-2/main` 上）。
- ⚠️ 坑记录：本地 `main` 分支陈旧（2026-03，无模块重组），**不能**用作 baseline；必须从近期含 exec_primitive 的提交拉。

### Phase 0（度量与开关骨架）— ✅ 代码完成，未编译验证
四文件，76→ 行，纯加法、默认关、零行为改动。本地模块边界检查 clean、无 include 循环、thrift 无重复 ordinal：
- `be/src/exec_primitive/pipeline/operator_factory.h`：`virtual bool is_stealable() const { return false; }`（照 `support_event_scheduler` 风格）。
- `be/src/exec_primitive/pipeline/source_operator.h`：`struct StealUnit {ChunkPtr; MorselPtr; int32_t partition_id=-1; valid();}`（move-only）+ `SourceOperator` 四个 no-op 虚函数 `support_steal/stealable_backlog/try_steal_unit/accept_stolen_unit` + include `scan/scan_morsel.h`。
- `gensrc/thrift/InternalService.thrift`：`TQueryOptions` 末尾加 ordinal **225–228**（`enable_pipeline_work_stealing` bool / `pipeline_steal_backlog_threshold` i32=2 / `pipeline_steal_cooldown_ns` i64=100000 / `pipeline_steal_max_per_round` i32=1）。
  - ⚠️ 坑记录：`TQueryOptions` ordinal **非严格递增**——211 后接的是 http_request 系列 212–224，真实 max=224。首版误用 212–215 撞车，已改 225–228 并移到 struct 末尾。
- `fe/.../qe/SessionVariable.java`：4 常量 + 4 `@VarAttr` 字段（master switch VISIBLE，3 knob INVISIBLE）+ 4 getter + `toThrift()` 4 处 `setXxx`。BE 后续经 `state->query_options().enable_pipeline_work_stealing` 读。

**待办**：dev host 上 thrift 代码生成 + BE/FE 编译验证（FE 的 `setEnable_pipeline_work_stealing` 等 setter 由 thrift 生成，编译前不存在）。然后可作独立 PR。

### Phase 1+ — 未开始
按「阶段顺序修正建议」：Phase 1 骨架 → LocalExchange(PARTITION) → HashJoinProbe → scan 降级。
