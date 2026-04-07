// Copyright 2002 and onwards Google Inc.
//
// Implementation migrated to fmt::sprintf (see stringprintf.h).
// Only StringAppendV remains here for va_list backward compatibility.

#include "gutil/stringprintf.h"

#include <cstdarg>
#include <cstdio>
#include <string>

void StringAppendV(std::string* dst, const char* format, va_list ap) {
    // First try with a small fixed size buffer.
    static const int kSpaceLength = 1024;
    char space[kSpaceLength];

    va_list backup_ap;
    va_copy(backup_ap, ap);
    int result = vsnprintf(space, kSpaceLength, format, backup_ap);
    va_end(backup_ap);

    if (result < kSpaceLength) {
        if (result >= 0) {
            dst->append(space, result);
            return;
        }
        if (result < 0) {
            return;
        }
    }

    // Increase the buffer size to the size requested by vsnprintf,
    // plus one for the closing \0.
    int length = result + 1;
    auto buf = new char[length];

    va_copy(backup_ap, ap);
    result = vsnprintf(buf, length, format, backup_ap);
    va_end(backup_ap);

    if (result >= 0 && result < length) {
        dst->append(buf, result);
    }
    delete[] buf;
}
