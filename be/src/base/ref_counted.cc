// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "base/ref_counted.h"

#include <atomic>

namespace starrocks::subtle {

RefCountedBase::RefCountedBase()
#ifndef NDEBUG
        : in_dtor_(false) {
}
#else
        = default;
#endif

RefCountedBase::~RefCountedBase() { // NOLINT
#ifndef NDEBUG
    DCHECK(in_dtor_) << "RefCounted object deleted without calling Release()";
#endif
}

void RefCountedBase::AddRef() const {
#ifndef NDEBUG
    DCHECK(!in_dtor_);
#endif
    ++ref_count_;
}

bool RefCountedBase::Release() const {
#ifndef NDEBUG
    DCHECK(!in_dtor_);
#endif
    if (--ref_count_ == 0) {
#ifndef NDEBUG
        in_dtor_ = true;
#endif
        return true;
    }
    return false;
}

bool RefCountedThreadSafeBase::HasOneRef() const {
    return ref_count_.load(std::memory_order_acquire) == 1;
}

RefCountedThreadSafeBase::RefCountedThreadSafeBase() { // NOLINT
#ifndef NDEBUG
    in_dtor_ = false;
#endif
}

RefCountedThreadSafeBase::~RefCountedThreadSafeBase() { // NOLINT
#ifndef NDEBUG
    DCHECK(in_dtor_) << "RefCountedThreadSafe object deleted without "
                        "calling Release()";
#endif
}

void RefCountedThreadSafeBase::AddRef() const {
#ifndef NDEBUG
    DCHECK(!in_dtor_);
#endif
    ref_count_.fetch_add(1, std::memory_order_relaxed);
}

bool RefCountedThreadSafeBase::Release() const {
#ifndef NDEBUG
    DCHECK(!in_dtor_);
    DCHECK(ref_count_.load(std::memory_order_acquire) > 0);
#endif
    if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
#ifndef NDEBUG
        in_dtor_ = true;
#endif
        return true;
    }
    return false;
}

} // namespace starrocks::subtle
