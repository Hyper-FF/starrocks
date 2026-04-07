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

// Slim compatibility header that re-exports absl endian utilities and provides
// custom functions (gbswap_128, bswap_24, 128-bit and 24-bit BigEndian/LittleEndian
// helpers) that have no direct absl equivalent.

#pragma once

#include <cstdint>

#include "absl/base/internal/endian.h"

// 128-bit byte swap (no absl equivalent).
inline unsigned __int128 gbswap_128(unsigned __int128 host_int) {
    return static_cast<unsigned __int128>(absl::gbswap_64(static_cast<uint64_t>(host_int >> 64))) |
           (static_cast<unsigned __int128>(absl::gbswap_64(static_cast<uint64_t>(host_int))) << 64);
}

// 24-bit byte swap (no absl equivalent).
inline uint32_t bswap_24(uint32_t x) {
    return ((x & 0x0000ffULL) << 16) | ((x & 0x00ff00ULL)) | ((x & 0xff0000ULL) >> 16);
}

namespace starrocks::endian_compat {

// BigEndian helpers for widths that absl does not cover.
struct BigEndian {
    static uint32_t FromHost24(uint32_t x) {
#if defined(ABSL_IS_LITTLE_ENDIAN)
        return bswap_24(x);
#else
        return x;
#endif
    }
    static uint32_t ToHost24(uint32_t x) {
#if defined(ABSL_IS_LITTLE_ENDIAN)
        return bswap_24(x);
#else
        return x;
#endif
    }

    static unsigned __int128 FromHost128(unsigned __int128 x) {
#if defined(ABSL_IS_LITTLE_ENDIAN)
        return gbswap_128(x);
#else
        return x;
#endif
    }
    static unsigned __int128 ToHost128(unsigned __int128 x) {
#if defined(ABSL_IS_LITTLE_ENDIAN)
        return gbswap_128(x);
#else
        return x;
#endif
    }
};

// LittleEndian helpers for widths that absl does not cover.
struct LittleEndian {
    static unsigned __int128 FromHost128(unsigned __int128 x) {
#if defined(ABSL_IS_LITTLE_ENDIAN)
        return x;
#else
        return gbswap_128(x);
#endif
    }
    static unsigned __int128 ToHost128(unsigned __int128 x) {
#if defined(ABSL_IS_LITTLE_ENDIAN)
        return x;
#else
        return gbswap_128(x);
#endif
    }
};

} // namespace starrocks::endian_compat
