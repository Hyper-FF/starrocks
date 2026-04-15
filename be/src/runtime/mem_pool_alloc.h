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

#include <utility>

#include "common/object_pool.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// Return the fragment-level MemPool when |pool| is the fragment-level
// ObjectPool (i.e. state->obj_pool()). Otherwise return nullptr so the
// caller falls back to ordinary heap allocation.
//
// The fragment MemPool is only safe to use during sequential prepare
// phases. Parallel-prepare code naturally uses operator-private ObjectPools
// that fail this check, so they fall back to heap automatically.
inline MemPool* fragment_mem_pool_of(RuntimeState* state, ObjectPool* pool) {
    return (state != nullptr && pool == state->obj_pool()) ? state->fragment_mem_pool() : nullptr;
}

// Placement-new |T| into |mp| (registering the destructor in |pool|) when
// |mp| is non-null; otherwise fall back to a heap allocation owned by |pool|.
template <typename T, typename... Args>
T* pool_alloc(ObjectPool* pool, MemPool* mp, Args&&... args) {
    if (mp != nullptr) {
        void* buf = mp->allocate_aligned(sizeof(T), alignof(T));
        DCHECK(buf != nullptr);
        return pool->emplace<T>(buf, std::forward<Args>(args)...);
    }
    return pool->add(new T(std::forward<Args>(args)...));
}

} // namespace starrocks
