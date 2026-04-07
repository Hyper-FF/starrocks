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

#include <cerrno>
#include <cstddef>
#include <type_traits>

// Compiler hint that this branch is likely or unlikely to
// be taken. Take from the "What all programmers should know
// about memory" paper.
// example: if (LIKELY(size > 0)) { ... }
// example: if (UNLIKELY(!status.ok())) { ... }
#define CACHE_LINE_SIZE 64

#ifdef LIKELY
#undef LIKELY
#endif

#ifdef UNLIKELY
#undef UNLIKELY
#endif

#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)

#define PREFETCH(addr) __builtin_prefetch(addr)

/// Force inlining. The 'inline' keyword is treated by most compilers as a hint,
/// not a command. This should be used sparingly for cases when either the function
/// needs to be inlined for a specific reason or the compiler's heuristics make a bad
/// decision, e.g. not inlining a small function on a hot path.
#define ALWAYS_INLINE __attribute__((always_inline))
#define ALWAYS_NOINLINE __attribute__((noinline))

#define ALIGN_CACHE_LINE __attribute__((aligned(CACHE_LINE_SIZE)))

#ifndef DIAGNOSTIC_PUSH
#ifdef __clang__
#define DIAGNOSTIC_PUSH _Pragma("clang diagnostic push")
#define DIAGNOSTIC_POP _Pragma("clang diagnostic pop")
#elif defined(__GNUC__)
#define DIAGNOSTIC_PUSH _Pragma("GCC diagnostic push")
#define DIAGNOSTIC_POP _Pragma("GCC diagnostic pop")
#elif defined(_MSC_VER)
#define DIAGNOSTIC_PUSH __pragma(warning(push))
#define DIAGNOSTIC_POP __pragma(warning(pop))
#else
#error("Unknown compiler")
#endif
#endif // ifndef DIAGNOSTIC_PUSH

#ifndef DIAGNOSTIC_IGNORE
#define PRAGMA(TXT) _Pragma(#TXT)
#ifdef __clang__
#define DIAGNOSTIC_IGNORE(XXX) PRAGMA(clang diagnostic ignored XXX)
#elif defined(__GNUC__)
#define DIAGNOSTIC_IGNORE(XXX) PRAGMA(GCC diagnostic ignored XXX)
#elif defined(_MSC_VER)
#define DIAGNOSTIC_IGNORE(XXX) __pragma(warning(disable : XXX))
#else
#define DIAGNOSTIC_IGNORE(XXX)
#endif
#endif // ifndef DIAGNOSTIC_IGNORE

// ---- Macros migrated from gutil/macros.h ----

// A macro to disallow the copy constructor and operator= functions.
#undef DISALLOW_COPY
#define DISALLOW_COPY(TypeName)         \
    TypeName(const TypeName&) = delete; \
    void operator=(const TypeName&) = delete

// For class templates: use ClassName (no template-id) for constructor/operator=,
// and FullTypeName (e.g. ClassName<T>) for the parameter.
#undef DISALLOW_COPY_TEMPLATE
#define DISALLOW_COPY_TEMPLATE(ClassName, FullTypeName) \
    ClassName(const FullTypeName&) = delete;            \
    void operator=(const FullTypeName&) = delete

#undef DISALLOW_MOVE
#define DISALLOW_MOVE(TypeName)    \
    TypeName(TypeName&&) = delete; \
    void operator=(TypeName&&) = delete

#undef DISALLOW_COPY_AND_MOVE
#define DISALLOW_COPY_AND_MOVE(TypeName) \
    DISALLOW_COPY(TypeName);             \
    DISALLOW_MOVE(TypeName)

#ifndef DISALLOW_IMPLICIT_CONSTRUCTORS
#define DISALLOW_IMPLICIT_CONSTRUCTORS(TypeName) \
    TypeName() = delete;                         \
    DISALLOW_COPY_AND_MOVE(TypeName)
#endif

// The arraysize(arr) macro returns the # of elements in an array arr.
template <typename T, size_t N>
char (&ArraySizeHelper(T (&array)[N]))[N];
#ifndef _MSC_VER
template <typename T, size_t N>
char (&ArraySizeHelper(const T (&array)[N]))[N];
#endif
#ifndef arraysize
#define arraysize(array) (sizeof(ArraySizeHelper(array)))
#endif

// A macro to turn a symbol into a string
#ifndef AS_STRING
#define AS_STRING(x) AS_STRING_INTERNAL(x)
#define AS_STRING_INTERNAL(x) #x
#endif

// Retry on EINTR for functions like read() that return -1 on error.
#define RETRY_ON_EINTR(err, expr)                                                              \
    do {                                                                                       \
        static_assert(std::is_signed<decltype(err)>::value, #err " must be a signed integer"); \
        (err) = (expr);                                                                        \
    } while ((err) == -1 && errno == EINTR)

// Same as above but for stream API calls like fread() and fwrite().
#define STREAM_RETRY_ON_EINTR(nread, stream, expr)                                                              \
    do {                                                                                                        \
        static_assert(std::is_unsigned<decltype(nread)>::value == true, #nread " must be an unsigned integer"); \
        (nread) = (expr);                                                                                       \
    } while ((nread) == 0 && ferror(stream) == EINTR)

// Same as above but for functions that return pointer types.
#define POINTER_RETRY_ON_EINTR(ptr, expr)                                                        \
    do {                                                                                         \
        static_assert(std::is_pointer<decltype(ptr)>::value == true, #ptr " must be a pointer"); \
        (ptr) = (expr);                                                                          \
    } while ((ptr) == nullptr && errno == EINTR)
