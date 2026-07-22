# Continuation prompt — work-stealing simplification refactor

Paste the block below into a fresh session to resume.

---

继续 StarRocks BE pipeline work-stealing 的**精简重构**。Worktree `/home/public/sr-worksteal`,分支 `feature/pipeline-work-stealing-simplified`(在 fork hyper)。**先读 worktree 里的 `PIPELINE_WORKSTEAL_SIMPLIFIED_DESIGN.md`(蓝图:全部决策 + 删/留清单 + 批次)**;记忆见 [[pipeline-work-stealing-plan]]。

一句话背景:full code review + fable 架构 review + 实验后,决定把 work-stealing 大幅精简(~2200→~500 行)并重新定位为 **skew-join 的运行时兜底**。已完成 **Batch 1**(commit `d86c7f34e88` [WIP],**当前不可编译**):删掉整套 producer 侧 StealWaiterSet(UAF/lost-wake/off-path-atomic 的根源)。

按蓝图续做到可编译 + 验证:
- **1b** 实现 `PipelineDriver::_schedule_steal_retry_timer` + `StealRetryTimeout` timer task —— 照 `pipeline_driver.cpp:1099` 的 RF-timeout timer(`_update_global_rf_timer`/`_unschedule_global_rf_timer`);`unschedule_and_join` 的 join 语义从结构上消除 UAF;park 分支已调用它(需实现);删 `finalize` 里残留的 `deregister_steal_waiter`;observer 的 `steal_trigger`+`STEAL_CHANGE_EVENT` 直接调度分支保留(driver 侧,安全,由 timer 触发)。
- **2** 删 `local_exchange_source_operator` 的 partition-free steal(它还引用已删的 StealWaiterSet = 当前编译错主因之一);连带删 `Pipeline::fully_stealable`/`compute_steal_barrier`、`chunk_accumulate`/`project`/`local_exchange_sink` 的 `is_stealable` 标记、`local_exchange_memory_test` 的 passthrough-steal UT。
- **3** 6 个门控谓词收敛成一个 `HashJoinProbeOperator::can_steal_partition(partition_id)`(内部依次查 PARTITIONED + !has_post_probe + single build + build done + sink breaks_partition_identity + all_builds_ready);修 **F1**(`try_steal_from_siblings` 对 `try_steal_unit` 返回的硬错误 `return status()` 而非 `continue`,否则已 pop 的 chunk 丢行);删死配置 `pipeline_steal_cooldown_ns`/`pipeline_steal_max_per_round`(thrift+SessionVariable.java+TQueryOptions,BE 从没读)。
- **4** 远程编译(`SR_PROFILE=worksteal`,detached `setsid ... ./build.sh --be -j24`)+ 单机重验:32M 90%-skew PARTITIONED `[SHUFFLE]` join, dop=16, **event scheduler ON**, `enable_pipeline_work_stealing` off==on 逐字节一致(`32000000 / 512000016000000 / 1644800000`)+ ~2.8×;uniform 自连接(sub-partitioned build)off==on(steal 跳过,无丢失)。

**保留**(partition-aware 核心,勿删):ExchangeSource steal(`steal_chunk_for_pipeline`/`buffered_chunks_for_pipeline`,concurrency-safe pop 不碰 `unpluging`)+ HashJoinProbe peer-prober(`clone_readable_table(fresh_probe_state)` + `reset_probe` + empty-build `is_done()` guard)+ `HashJoinerFactory::all_builds_ready`/`builder_for_partition` + `supports_partition_aware_steal` + sink `breaks_partition_identity`。默认关(`enable_pipeline_work_stealing`)。

环境:远程 dev profile `worksteal`(worktree base 35c68c6305e,和本地 base 29d73b8d875 分叉 → 改动 scp/patch 到远端编译);单机 skew 数据库 `wstest`(t1 32M/4-bucket,t2 1000 行);steal 撞 scan 4-bucket 天花板 ~3.9×(已知,非 bug)。

---
