// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <utility> // IWYU pragma: keep

#ifndef NDEBUG
#include <glog/logging.h>
#endif

namespace starrocks {
namespace subtle {

class RefCountedBase {
public:
    bool HasOneRef() const { return ref_count_ == 1; }

protected:
    RefCountedBase();
    ~RefCountedBase();

    void AddRef() const;

    // Returns true if the object should self-delete.
    bool Release() const;

private:
    mutable int ref_count_{0};
#ifndef NDEBUG
    mutable bool in_dtor_;
#endif

    RefCountedBase(const RefCountedBase&) = delete;
    const RefCountedBase& operator=(const RefCountedBase&) = delete;
};

class RefCountedThreadSafeBase {
public:
    bool HasOneRef() const;

protected:
    RefCountedThreadSafeBase();
    ~RefCountedThreadSafeBase();

    void AddRef() const;

    // Returns true if the object should self-delete.
    bool Release() const;

private:
    mutable std::atomic<int32_t> ref_count_{0};
#ifndef NDEBUG
    mutable bool in_dtor_;
#endif

    RefCountedThreadSafeBase(const RefCountedThreadSafeBase&) = delete;
    const RefCountedThreadSafeBase& operator=(const RefCountedThreadSafeBase&) = delete;
};

} // namespace subtle

template <class T>
class RefCounted : public subtle::RefCountedBase {
public:
    RefCounted() = default;

    void AddRef() const { subtle::RefCountedBase::AddRef(); }

    void Release() const {
        if (subtle::RefCountedBase::Release()) {
            delete static_cast<const T*>(this);
        }
    }

protected:
    ~RefCounted() = default;

private:
    RefCounted(const RefCounted&) = delete;
    const RefCounted& operator=(const RefCounted&) = delete;
};

// Forward declaration.
template <class T, typename Traits>
class RefCountedThreadSafe;

template <typename T>
struct DefaultRefCountedThreadSafeTraits {
    static void Destruct(const T* x) { RefCountedThreadSafe<T, DefaultRefCountedThreadSafeTraits>::DeleteInternal(x); }
};

template <class T, typename Traits = DefaultRefCountedThreadSafeTraits<T> >
class RefCountedThreadSafe : public subtle::RefCountedThreadSafeBase {
public:
    RefCountedThreadSafe() = default;

    void AddRef() const { subtle::RefCountedThreadSafeBase::AddRef(); }

    void Release() const {
        if (subtle::RefCountedThreadSafeBase::Release()) {
            Traits::Destruct(static_cast<const T*>(this));
        }
    }

protected:
    ~RefCountedThreadSafe() = default;

private:
    friend struct DefaultRefCountedThreadSafeTraits<T>;
    static void DeleteInternal(const T* x) { delete x; }

    RefCountedThreadSafe(const RefCountedThreadSafe&) = delete;
    const RefCountedThreadSafe& operator=(const RefCountedThreadSafe&) = delete;
};

template <typename T>
class RefCountedData : public starrocks::RefCountedThreadSafe<starrocks::RefCountedData<T> > {
public:
    RefCountedData() : data() {}
    RefCountedData(const T& in_value) : data(in_value) {}

    T data;

private:
    friend class starrocks::RefCountedThreadSafe<starrocks::RefCountedData<T> >;
    ~RefCountedData() = default;
};

} // namespace starrocks

template <class T>
class scoped_refptr {
public:
    typedef T element_type;

    scoped_refptr() : ptr_(nullptr) {}

    scoped_refptr(T* p) : ptr_(p) {
        if (ptr_) ptr_->AddRef();
    }

    scoped_refptr(const scoped_refptr<T>& r) : ptr_(r.ptr_) {
        if (ptr_) ptr_->AddRef();
    }

    template <typename U>
    scoped_refptr(const scoped_refptr<U>& r) : ptr_(r.get()) {
        if (ptr_) ptr_->AddRef();
    }

    scoped_refptr(scoped_refptr&& r) noexcept : ptr_(r.get()) { // NOLINT
        r.ptr_ = nullptr;
    }

    template <typename U>
    scoped_refptr(scoped_refptr<U>&& r) noexcept : ptr_(r.get()) { // NOLINT
        r.ptr_ = nullptr;
    }

    ~scoped_refptr() {
        if (ptr_) ptr_->Release();
    }

    T* get() const { return ptr_; }

#if SCOPED_REFPTR_ALLOW_IMPLICIT_CONVERSION_TO_PTR
    operator T*() const { return ptr_; }
#else
    typedef T* scoped_refptr::*Testable;
    operator Testable() const { return ptr_ ? &scoped_refptr::ptr_ : nullptr; }
#endif

    T* operator->() const {
        assert(ptr_ != NULL);
        return ptr_;
    }

    scoped_refptr<T>& operator=(T* p) {
        if (p) p->AddRef();
        T* old_ptr = ptr_;
        ptr_ = p;
        if (old_ptr) old_ptr->Release();
        return *this;
    }

    scoped_refptr<T>& operator=(const scoped_refptr<T>& r) { return *this = r.ptr_; }

    template <typename U>
    scoped_refptr<T>& operator=(const scoped_refptr<U>& r) {
        return *this = r.get();
    }

    scoped_refptr<T>& operator=(scoped_refptr<T>&& r) noexcept {
        scoped_refptr<T>(std::move(r)).swap(*this);
        return *this;
    }

    template <typename U>
    scoped_refptr<T>& operator=(scoped_refptr<U>&& r) {
        scoped_refptr<T>(std::move(r)).swap(*this);
        return *this;
    }

    void swap(T** pp) {
        T* p = ptr_;
        ptr_ = *pp;
        *pp = p;
    }

    void swap(scoped_refptr<T>& r) { swap(&r.ptr_); }

    void reset(T* p = nullptr) { *this = p; }

protected:
    T* ptr_;

private:
    template <typename U>
    friend class scoped_refptr;
};

template <typename T>
scoped_refptr<T> make_scoped_refptr(T* t) {
    return scoped_refptr<T>(t);
}

template <class T>
struct ScopedRefPtrEqualToFunctor {
    bool operator()(const scoped_refptr<T>& x, const scoped_refptr<T>& y) const { return x.get() == y.get(); }
};

template <class T>
struct ScopedRefPtrHashFunctor {
    size_t operator()(const scoped_refptr<T>& p) const { return reinterpret_cast<size_t>(p.get()); }
};
