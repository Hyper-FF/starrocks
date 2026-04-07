// Copyright 2002 and onwards Google Inc.
//
// Printf variants that place their output in a C++ string.
// Implementation delegates to fmt::sprintf.
// All new code should prefer fmt::format() directly.

#pragma once

#include <cstdarg>
#include <string>
#include <vector>

#include "fmt/printf.h"

// Return a C++ string via fmt::sprintf.
template <typename... Args>
std::string StringPrintf(const char* format, const Args&... args) {
    return fmt::sprintf(format, args...);
}

// Store result into a supplied string and return it.
template <typename... Args>
const std::string& SStringPrintf(std::string* dst, const char* format, const Args&... args) {
    *dst = fmt::sprintf(format, args...);
    return *dst;
}

// Append result to a supplied string.
template <typename... Args>
void StringAppendF(std::string* dst, const char* format, const Args&... args) {
    dst->append(fmt::sprintf(format, args...));
}

// Lower-level routine that takes a va_list and appends to a specified string.
// Kept for backward compatibility with code that uses va_list directly.
void StringAppendV(std::string* dst, const char* format, va_list ap);
