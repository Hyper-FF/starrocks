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

// COMPILE_ASSERT: thin wrapper around static_assert.
// New code should use static_assert directly.
#ifndef COMPILE_ASSERT
#define COMPILE_ASSERT(expr, msg) static_assert(expr, #msg)
#endif

// Macros to disallow copy/move constructors and assignment operators.
#undef DISALLOW_COPY
#define DISALLOW_COPY(TypeName)         \
    TypeName(const TypeName&) = delete; \
    void operator=(const TypeName&) = delete

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

// arraysize: returns the number of elements in a statically-allocated array.
template <typename T, size_t N>
char (&ArraySizeHelper(T (&array)[N]))[N];

#ifndef _MSC_VER
template <typename T, size_t N>
char (&ArraySizeHelper(const T (&array)[N]))[N];
#endif

#ifndef arraysize
#define arraysize(array) (sizeof(ArraySizeHelper(array)))
#endif

#if !defined(_MSC_VER) || (defined(_MSC_VER) && _MSC_VER < 1400)
#ifndef ARRAYSIZE
#define ARRAYSIZE(a) ((sizeof(a) / sizeof(*(a))) / static_cast<size_t>(!(sizeof(a) % sizeof(*(a)))))
#endif
#endif

// Stringify a symbol.
#define AS_STRING(x) AS_STRING_INTERNAL(x)
#define AS_STRING_INTERNAL(x) #x

// Generate a unique variable name with line number.
#define VARNAME_LINENUM(varname) VARNAME_LINENUM_INTERNAL(varname##_L, __LINE__)
#define VARNAME_LINENUM_INTERNAL(v, line) VARNAME_LINENUM_INTERNAL2(v, line)
#define VARNAME_LINENUM_INTERNAL2(v, line) v##line

// FALLTHROUGH_INTENDED: annotate implicit fall-through between switch labels.
#if defined(__clang__) && defined(__has_warning)
#if __has_feature(cxx_attributes) && __has_warning("-Wimplicit-fallthrough")
#define FALLTHROUGH_INTENDED [[clang::fallthrough]] // NOLINT
#endif
#endif
#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED \
    do {                     \
    } while (0)
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
