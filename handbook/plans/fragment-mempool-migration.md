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

## 迁移分为 5 个 Phase

---

### Phase 0: Descriptor 成员 PMR 化（独立，可最先做）

Descriptor 对象已经 placement new 到 MemPool，col_name() 已改为 string_view。
现在把内部成员也改为 PMR 类型，使字符串/容器数据也从 MemPool 分配。

#### 0.1 SlotDescriptor — 字符串 PMR 化

**文件**: `be/src/runtime/descriptors.h/cpp`

```cpp
// 当前
const SlotId _id;
TypeDescriptor _type;
const TupleId _parent;
std::string _col_name;           // ← heap 分配
std::string _col_physical_name;  // ← heap 分配

// 目标
std::pmr::string _col_name;
std::pmr::string _col_physical_name;
```

构造函数加 `std::pmr::memory_resource* mr` 参数（已有默认值经验）。
col_name() 已返回 string_view，调用方无需改动。

**工作量**: 2 个文件
**风险**: 低 — col_name() 返回值已经是 string_view，无级联改动

#### 0.2 TableDescriptor — 字符串 PMR 化

**文件**: `be/src/runtime/descriptors.h/cpp`

```cpp
// 当前
std::string _name;      // ← heap
std::string _database;  // ← heap

// 目标
std::pmr::string _name;
std::pmr::string _database;
```

name()/database() 返回值改为 string_view（影响面需评估）。

**工作量**: 2 个文件 + 调用方 string_view 适配
**风险**: 低到中

#### 0.3 DescriptorTbl — maps PMR 化

**文件**: `be/src/runtime/descriptors.h/cpp`

```cpp
// 当前
std::unordered_map<TableId, TableDescriptor*> _tbl_desc_map;
std::unordered_map<TupleId, TupleDescriptor*> _tuple_desc_map;
std::unordered_map<SlotId, SlotDescriptor*> _slot_desc_map;
std::unordered_map<SlotId, SlotDescriptor*> _slot_with_column_name_map;

// 目标
std::pmr::unordered_map<TableId, TableDescriptor*> _tbl_desc_map;
std::pmr::unordered_map<TupleId, TupleDescriptor*> _tuple_desc_map;
std::pmr::unordered_map<SlotId, SlotDescriptor*> _slot_desc_map;
std::pmr::unordered_map<SlotId, SlotDescriptor*> _slot_with_column_name_map;
```

maps 不对外暴露，只通过 get_xxx() 方法返回裸指针，无外部兼容性问题。
构造函数接受 `std::pmr::memory_resource*`。

**工作量**: 2 个文件
**风险**: 低 — 内部变更，对外接口不变

#### 0.4 TupleDescriptor — vectors 保持不变

`_slots` 和 `_decoded_slots` 是 `std::vector<SlotDescriptor*>`。
30+ 文件用 `const std::vector<SlotDescriptor*>*` 指针指向它们。
改为 `std::pmr::vector` 会级联破坏整个 storage 层。**暂不迁移**。

#### 0.5 TableDescriptor 子类 — 字符串 PMR 化

**文件**: `be/src/runtime/descriptors_ext.h/cpp`

~15 个子类（HdfsTableDescriptor, IcebergTableDescriptor, MySQLTableDescriptor 等），
每个有 2-8 个 `std::string` 成员。

迁移方式：
1. 子类构造函数加 `mr` 参数，透传给基类
2. 子类 string 成员改为 `std::pmr::string`
3. `DescriptorTbl::create()` 的 `ALLOC_DESC` 宏传 `mr`

**工作量**: 2 个文件，~15 个类
**风险**: 中 — 工作量大但模式统一

#### 0.6 TypeDescriptor — 容器 PMR 化

**文件**: `be/src/types/type_descriptor.h/cpp`

```cpp
// 当前
std::vector<TypeDescriptor> children;
std::vector<std::string> field_names;
std::vector<int32_t> field_ids;
std::vector<std::string> field_physical_names;

// 目标
std::pmr::vector<TypeDescriptor> children;
std::pmr::vector<std::pmr::string> field_names;
std::pmr::vector<int32_t> field_ids;
std::pmr::vector<std::pmr::string> field_physical_names;
```

TypeDescriptor 是值类型，到处拷贝。PMR 容器在拷贝时不传播 allocator。
解法：from_thrift()/from_protobuf() 加 `mr` 参数，递归传递。
默认 `get_default_resource()`（堆），向后兼容。
栈上/静态工厂创建的 TypeDescriptor 继续用堆，无影响。

**工作量**: 2 个文件
**风险**: 低 — 读 API（size/operator[]/迭代）完全兼容，写入点只有 17 处

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

### Phase 3: Pipeline Operator / Context 体系

Pipeline 组件全部用 `std::make_shared<T>(...)` 管理，无法简单 placement new。

#### 现状分析

| 组件 | 分配方式 | 生命周期 | 内部拥有 MemPool? | 可 placement new? |
|------|---------|---------|-------------------|-----------------|
| Operator | make_shared | per-driver | 否 | **否** — shared_ptr 语义 |
| OperatorFactory | make_shared | fragment | 否 | **否** — 存在 vector 中 |
| PipelineDriver | make_shared | per-driver | 否 | **否** — 拥有 Operator 列表 |
| Pipeline | make_shared | fragment | 否 | **否** — 拥有 Factory/Driver |
| Aggregator | make_shared | per-driver-seq | **是** (unique_ptr) | **否** — 拥有 MemPool+ObjectPool |
| Analytor | make_shared | per-driver | **是** (unique_ptr) | **否** — 拥有 MemPool+queue |
| HashJoiner | make_shared | per-driver-seq | 否 | **否** — 拥有 hash table |
| SortContext | make_shared | fragment/per-DOP | 否 | **否** — 拥有 sorter+cursor |
| HashPartitionContext | make_shared | per-driver-seq | **是** (unique_ptr) | **否** — 拥有 MemPool |
| ExceptContext | make_shared | per-partition | **是** (unique_ptr) | **否** — 拥有 MemPool+hash set |
| IntersectContext | make_shared | per-partition | **是** (unique_ptr) | **否** — 拥有 MemPool+hash set |

**核心阻碍**: 这些对象用 `shared_ptr` 管理生命周期，内部拥有 MemPool / ObjectPool / hash table 等复杂结构。不适合 placement new。

#### 3.1 可行方向：内部 MemPool 统一

对于内部拥有独立 `unique_ptr<MemPool>` 的组件（Aggregator、Analytor、HashPartitionContext、ExceptContext、IntersectContext），可以把它们的内部 MemPool **替换为指向 fragment MemPool 的指针**：

```cpp
// Aggregator 当前
std::unique_ptr<MemPool> _mem_pool;  // 独立拥有

// 目标
MemPool* _mem_pool;  // 指向 RuntimeState::fragment_mem_pool()
```

**收益**: 减少 MemPool 数量，聚合状态/hash key 分配统一到 fragment MemPool。
**风险**: 高 — 这些 MemPool 有独立的 `clear()`/`free_all()` 调用控制中间数据释放（如 streaming agg 的 per-batch clear）。统一后 clear 语义需要重新设计。

**结论**: 需要逐个组件分析其 MemPool 使用模式（是 per-batch clear 还是 fragment-lifetime）后才能决定是否统一。不建议在 Phase 3 中一步完成，改为 Phase 3 只做调研，实际改动放到后续迭代。

#### 3.2 可行方向：pmr::polymorphic_allocator 替代 make_shared

用 `std::allocate_shared` + `std::pmr::polymorphic_allocator` 替代 `make_shared`，使对象本身的内存从 MemPool 分配，但 shared_ptr 控制块仍在堆上：

```cpp
// 当前
auto ctx = std::make_shared<SortContext>(...);

// 目标
std::pmr::polymorphic_allocator<SortContext> alloc(state->mem_resource());
auto ctx = std::allocate_shared<SortContext>(alloc, ...);
```

**收益**: 对象内存从 MemPool 分配，不改变 shared_ptr 所有权语义。
**风险**: 中 — shared_ptr 控制块仍然 heap 分配；析构器仍需调用。
**适用**: 所有 make_shared 调用点，无需改接口。

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
Phase 0   (Descriptor PMR)  ← 独立，可最先做
Phase 1.1 (Expr)            ← 独立，可与 Phase 0 并行
Phase 1.2 (ExprContext)     ← 依赖 1.1
Phase 1.3 (FunctionContext) ← 依赖 1.2
Phase 2   (RuntimeFilter)   ← 独立，可并行
Phase 3   (Contexts)        ← 依赖 Phase 1 完成（Context 持有 ExprContext*）
Phase 4   (消除析构器)       ← 依赖 Phase 0-3 全部完成
```

## 每个 Phase 的验证标准

1. **编译通过**: `./build.sh --be` 无错误
2. **UT 通过**: `./run-be-ut.sh` 全部通过
3. **ASAN 无报错**: ASAN 构建下 UT 无 heap-use-after-free
4. **性能不退化**: TPC-H / TPC-DS 基准对比无回归
