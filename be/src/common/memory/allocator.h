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

#include <cstdlib>
#include <new>
#include <type_traits>

#include "common/compiler_util.h"

namespace starrocks {

// Allocator provides interfaces related to memory management,
// and their semantics are the same as the standard libc interface.
// In the future, we can use Allocator to replace mem_hook and achieve more precise memory management.
class Allocator {
public:
    virtual ~Allocator() = default;
    virtual void* alloc(size_t size) = 0;
    virtual void free(void* ptr) = 0;
    virtual void* realloc(void* ptr, size_t size) = 0;
    virtual void* calloc(size_t n, size_t size) = 0;
    virtual void cfree(void* ptr) = 0;
    virtual void* memalign(size_t align, size_t size) = 0;
    virtual void* aligned_alloc(size_t align, size_t size) = 0;
    virtual void* valloc(size_t size) = 0;
    virtual void* pvalloc(size_t size) = 0;
    virtual int posix_memalign(void** ptr, size_t align, size_t size) = 0;

    // Follow std::allocator::allocate() style, throw std::bad_alloc if allocate fail
    virtual void* checked_alloc(size_t size) = 0;
};

template <class Base, class Derived>
class AllocatorFactory : public Base {
private:
    // C++23 deducing-this (P0847) collapses the CRTP `derived()` helper into a
    // single function that propagates const-ness from `self`.
    template <typename Self>
    auto& derived(this Self& self) {
        using D = std::conditional_t<std::is_const_v<Self>, const Derived, Derived>;
        return static_cast<D&>(self);
    }

public:
    void* alloc(size_t size) override { return derived().alloc(size); }
    void free(void* ptr) override { derived().free(ptr); }
    void* realloc(void* ptr, size_t size) override { return derived().realloc(ptr, size); }
    void* calloc(size_t n, size_t size) override { return derived().calloc(n, size); }
    void cfree(void* ptr) override { derived().cfree(ptr); }
    void* memalign(size_t align, size_t size) override { return derived().memalign(align, size); }
    void* aligned_alloc(size_t align, size_t size) override { return derived().aligned_alloc(align, size); }
    void* valloc(size_t size) override { return derived().valloc(size); }
    void* pvalloc(size_t size) override { return derived().pvalloc(size); }
    int posix_memalign(void** ptr, size_t align, size_t size) override {
        return derived().posix_memalign(ptr, align, size);
    }

    void* checked_alloc(size_t size) override {
        void* result = derived().alloc(size);
        if (UNLIKELY(result == nullptr)) {
            throw std::bad_alloc();
        }
        return result;
    }
};

} // namespace starrocks