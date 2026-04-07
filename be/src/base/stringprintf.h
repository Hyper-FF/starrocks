// Copyright 2002 and onwards Google Inc.
// Self-contained version moved from base/gutil/stringprintf.h.
#pragma once

#include <cstdarg>
#include <string>
#include <vector>

using std::string;
using std::vector;

#if defined(__GNUC__) || defined(__APPLE__)
#define STRINGPRINTF_PRINTF_ATTRIBUTE(string_index, first_to_check) \
    __attribute__((__format__(__printf__, string_index, first_to_check)))
#else
#define STRINGPRINTF_PRINTF_ATTRIBUTE(string_index, first_to_check)
#endif

extern string StringPrintf(const char* format, ...) STRINGPRINTF_PRINTF_ATTRIBUTE(1, 2);
extern const string& SStringPrintf(string* dst, const char* format, ...) STRINGPRINTF_PRINTF_ATTRIBUTE(2, 3);
extern void StringAppendF(string* dst, const char* format, ...) STRINGPRINTF_PRINTF_ATTRIBUTE(2, 3);
extern void StringAppendV(string* dst, const char* format, va_list ap);
extern const int kStringPrintfVectorMaxArgs;
extern string StringPrintfVector(const char* format, const vector<string>& v);
