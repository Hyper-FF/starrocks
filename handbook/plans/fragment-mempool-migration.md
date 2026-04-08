# Plan: Fragment MemPool 全组件迁移

## 目标

查询期间所有 fragment 生命周期的组件，从独立 heap allocation 迁移到 RuntimeState 拥有的 fragment MemPool，实现：
1. 减少 per-object malloc/free 开销
2. 提升内存局部性
3. 为最终"批量释放、不调析构器"铺路

## 已完成基础设施

| 组件 | 状态 |
|------|------|
| RuntimeState: `_fragment_mem_pool` + `_mem_resource` | ✅ |
| ObjectPool::emplace() (placement new + destructor-only cleanup) | ✅ |
| MemPoolResource (std::pmr::memory_resource adapter) | ✅ |
| ExecNode placement new (exec_factory.cpp) | ✅ |
| Descriptor placement new (descriptors_ext.cpp) | ✅ |
| SlotDescriptor::col_name() → string_view | ✅ |

## 迁移分为 4 个 Phase

---

### Phase 1: Expr 体系（最高优先级）

Expr 创建在查询的关键路径上，数量多（一个查询可能有上百个 Expr 节点）。

#### 1.1 Expr 子类 — placement new

**文件**: `be/src/exprs/expr_factory.cpp`

当前模式：
```cpp
*expr = pool->add(new VectorizedLiteral(texpr_node));
*expr = pool->add(new FunctionCallExpr(texpr_node));
// ... 80+ 种 Expr 子类
```

迁移方式：与 ExecNode 相同的 `CREATE_NODE` 宏模式。

**工作量**: 1 个文件，~30 个 pool->add 调用点
**风险**: 低 — 纯机械替换

#### 1.2 ExprContext — placement new + MemPool 统一

**文件**: `be/src/exprs/expr_context.h/cpp`

当前问题：
```cpp
// 每个 ExprContext 独立创建 MemPool（双重 pooling）
ExprContext::ExprContext(Expr* root) : _root(root) {}
Status ExprContext::open(RuntimeState* state) {
    _pool = std::make_unique<MemPool>();  // 冗余！
}
```

迁移方式：
1. ExprContext 对象本身 placement new 到 fragment MemPool
2. ExprContext 内部的 `_pool` 改为指向 fragment MemPool（不再独立拥有）
3. ExprContext 析构器不再 `_pool->free_all()`

**注意**: ExprContext 有 per-driver clone 场景（`ExprContext::clone`），clone 出来的 ExprContext 可能需要独立 MemPool。需要区分 "fragment-level" 和 "driver-level" 两种生命周期。

**工作量**: 2 个文件，中等复杂度
**风险**: 中 — clone 语义需要仔细处理

#### 1.3 FunctionContext — placement new

**文件**: `be/src/exprs/function_context.h/cpp`

当前模式：
```cpp
// 静态工厂，返回裸 new
FunctionContext* FunctionContext::create_context(...)  {
    return new FunctionContext();
}
// ExprContext 析构器手动 delete
~ExprContext() { for (auto* fc : _fn_contexts) delete fc; }
```

迁移方式：
1. FunctionContext placement new 到 fragment MemPool
2. 用 ObjectPool::emplace 注册析构（因为有 fragment-local state 和 thread-local state）
3. ExprContext 析构器不再手动 delete

**工作量**: 2 个文件
**风险**: 中 — UDF state 生命周期需要注意

---

### Phase 2: Runtime Filter 体系

数量不多，但每个对象有较多 non-trivial 成员。

#### 2.1 RuntimeFilterBuildDescriptor — placement new

**文件**: `be/src/runtime/runtime_filter/runtime_filter_build.h/cpp`

当前模式：`pool->add(new RuntimeFilterBuildDescriptor())`
分布在: `hash_join_node.cpp`, `cross_join_node.cpp`, `topn_node.cpp`, `aggregate_base_node.cpp`

迁移方式：用 `ALLOC_DESC` 同类宏。

**工作量**: 4 个分散调用点
**风险**: 低

#### 2.2 RuntimeFilterProbeDescriptor — placement new

**文件**: `be/src/runtime/runtime_filter/runtime_filter_probe.h/cpp`

当前模式：`pool->add(new RuntimeFilterProbeDescriptor())`
调用点: `exec_node.cpp`

迁移方式：同上。

**工作量**: 1 个调用点
**风险**: 低

---

### Phase 3: Pipeline Operator Context 体系

这些 Context 对象数量少（每种一般 1-2 个），但体积大、成员多。

#### 3.1 Aggregator / AggregatorFactory

**文件**: `be/src/exec/aggregator.h/cpp`

当前模式：`std::make_shared<Aggregator>(...)`
内部拥有独立 `_mem_pool`。

迁移方式：
1. Aggregator 对象 placement new 到 fragment MemPool
2. Aggregator 内部 `_mem_pool` 改为指向 fragment MemPool
3. HashTableKeyAllocator 的 pool 指针统一

**工作量**: 2 个文件，复杂
**风险**: 高 — 聚合状态内存管理是性能关键路径，需要性能验证

#### 3.2 Analytor

**文件**: `be/src/exec/analytor.h/cpp`

当前模式：`std::make_shared<Analytor>(...)`
内部拥有独立 `_mem_pool`。

迁移方式：同 Aggregator。

**工作量**: 2 个文件
**风险**: 中

#### 3.3 SortContext / LocalPartitionTopnContext / HashPartitionContext

**文件**: `be/src/exec/pipeline/sort/*.h`, `be/src/exec/pipeline/hash_partition_context.h`

每个都拥有独立 `_mem_pool`。

迁移方式：统一指向 fragment MemPool。

**工作量**: 3 组文件
**风险**: 中

#### 3.4 Join Context (HashJoin / NLJoin / ExceptContext / IntersectContext)

**文件**: `be/src/exec/pipeline/set/*.h`, `be/src/exec/pipeline/nljoin/*.h`

当前模式：`std::make_shared<XxxContext>(...)`

迁移方式：placement new。

**工作量**: 4 组文件
**风险**: 低到中

---

### Phase 4: 消除析构器依赖（最终目标）

当 Phase 1-3 完成后，所有组件的对象内存和内部容器存储都来自 fragment MemPool。此时可以：

#### 4.1 成员类型 PMR 化

将 Phase 1-3 中各组件的 `std::string`/`std::vector`/`std::unordered_map` 替换为 `std::pmr::string`/`std::pmr::vector`/`std::pmr::unordered_map`，使内部存储也从 MemPool 分配。

此阶段复用 SlotDescriptor `col_name() → string_view` 的经验：
- 返回 `string_view` 而非 `const std::string&`
- 调用方按需构造 `std::string`

**注意**: Thrift/Protobuf 生成的类型（TFunction, TExprNode 等）无法 PMR 化，只能在边界处拷贝。

#### 4.2 MemPool::register_destroy() — 过渡机制

对于仍有非平凡析构器的对象，在 MemPool 上注册 destroy 回调：

```cpp
class MemPool {
    using DestroyFn = void(*)(void*);
    std::vector<std::pair<void*, DestroyFn>> _destroy_list;

    void register_destroy(void* obj, DestroyFn fn);
    void destroy_and_free_all() {
        for (auto it = _destroy_list.rbegin(); it != _destroy_list.rend(); ++it)
            it->second(it->first);
        free_all();
    }
};
```

#### 4.3 移除 ObjectPool 对查询组件的参与

最终状态：查询组件全部由 `MemPool::destroy_and_free_all()` 一把释放，ObjectPool 不再参与查询组件的生命周期管理。

---

## 依赖关系

```
Phase 1.1 (Expr)     ← 独立，可先做
Phase 1.2 (ExprContext) ← 依赖 1.1
Phase 1.3 (FunctionContext) ← 依赖 1.2
Phase 2   (RuntimeFilter) ← 独立，可并行
Phase 3   (Contexts)  ← 依赖 Phase 1 完成（因为 Context 持有 ExprContext*）
Phase 4   (PMR化)     ← 依赖 Phase 1-3 全部完成
```

## 每个 Phase 的验证标准

1. **编译通过**: `./build.sh --be` 无错误
2. **UT 通过**: `./run-be-ut.sh` 全部通过
3. **ASAN 无报错**: ASAN 构建下 UT 无 heap-use-after-free
4. **性能不退化**: TPC-H / TPC-DS 基准对比无回归
