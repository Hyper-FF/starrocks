// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <memory_resource>

#include "runtime/mem_pool.h"

namespace starrocks {

// A std::pmr::memory_resource adapter that delegates allocations to a MemPool.
// deallocate() is a no-op — memory is reclaimed in bulk when the MemPool is freed.
//
// Usage:
//   MemPool pool;
//   MemPoolResource mr(&pool);
//   std::pmr::string s("hello", &mr);  // string data allocated from MemPool
class MemPoolResource final : public std::pmr::memory_resource {
public:
    explicit MemPoolResource(MemPool* pool) : _pool(pool) {}

    MemPool* pool() const { return _pool; }

private:
    void* do_allocate(size_t bytes, size_t alignment) override {
        // MemPool requires alignment to be a power of 2 and >= 1.
        // std::pmr guarantees alignment is a power of 2.
        // Clamp to MemPool's DEFAULT_ALIGNMENT (16) as the minimum — this
        // satisfies max_align_t and keeps MemPool's fast path.
        int align = static_cast<int>(std::max(alignment, static_cast<size_t>(MemPool::DEFAULT_ALIGNMENT)));
        return _pool->allocate_aligned(static_cast<int64_t>(bytes), align);
    }

    void do_deallocate(void* /*p*/, size_t /*bytes*/, size_t /*alignment*/) override {
        // No-op: MemPool reclaims memory in bulk via free_all().
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

    MemPool* _pool;
};

} // namespace starrocks
