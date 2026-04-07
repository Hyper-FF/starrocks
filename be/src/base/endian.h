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

// Endian utilities built on absl. Provides BigEndian/LittleEndian classes
// compatible with the old gutil API, delegating to absl for standard widths
// and adding 24-bit / 128-bit helpers that absl lacks.

#pragma once

#include <cstdint>
#include <cstring>

#include "absl/base/internal/endian.h"

// 128-bit byte swap (no absl equivalent).
inline unsigned __int128 gbswap_128(unsigned __int128 host_int) {
    return static_cast<unsigned __int128>(absl::gbswap_64(static_cast<uint64_t>(host_int >> 64))) |
           (static_cast<unsigned __int128>(absl::gbswap_64(static_cast<uint64_t>(host_int))) << 64);
}

// 24-bit byte swap (no absl equivalent).
inline uint32_t bswap_24(uint32_t x) {
    return ((x & 0x0000ffU) << 16) | (x & 0x00ff00U) | ((x & 0xff0000U) >> 16);
}

// BigEndian provides byte-swapping for host ↔ big-endian conversions and
// unaligned load/store in big-endian byte order.  Standard widths delegate
// to absl; 24-bit and 128-bit are handled locally.
class BigEndian {
public:
    // --- 16-bit ---
    static uint16_t FromHost16(uint16_t x) { return absl::big_endian::FromHost16(x); }
    static uint16_t ToHost16(uint16_t x) { return absl::big_endian::ToHost16(x); }
    static uint16_t Load16(const void* p) { return absl::big_endian::Load16(p); }
    static void Store16(void* p, uint16_t v) { absl::big_endian::Store16(p, v); }

    // --- 24-bit (no absl equivalent) ---
    static uint32_t FromHost24(uint32_t x) {
#if defined(ABSL_IS_LITTLE_ENDIAN)
        return bswap_24(x);
#else
        return x;
#endif
    }
    static uint32_t ToHost24(uint32_t x) { return FromHost24(x); }

    // --- 32-bit ---
    static uint32_t FromHost32(uint32_t x) { return absl::big_endian::FromHost32(x); }
    static uint32_t ToHost32(uint32_t x) { return absl::big_endian::ToHost32(x); }
    static uint32_t Load32(const void* p) { return absl::big_endian::Load32(p); }
    static void Store32(void* p, uint32_t v) { absl::big_endian::Store32(p, v); }

    // --- 64-bit ---
    static uint64_t FromHost64(uint64_t x) { return absl::big_endian::FromHost64(x); }
    static uint64_t ToHost64(uint64_t x) { return absl::big_endian::ToHost64(x); }
    static uint64_t Load64(const void* p) { return absl::big_endian::Load64(p); }
    static void Store64(void* p, uint64_t v) { absl::big_endian::Store64(p, v); }

    // --- 128-bit (no absl equivalent) ---
    static unsigned __int128 FromHost128(unsigned __int128 x) {
#if defined(ABSL_IS_LITTLE_ENDIAN)
        return gbswap_128(x);
#else
        return x;
#endif
    }
    static unsigned __int128 ToHost128(unsigned __int128 x) { return FromHost128(x); }
};

// LittleEndian counterpart.
class LittleEndian {
public:
    static uint16_t FromHost16(uint16_t x) { return absl::little_endian::FromHost16(x); }
    static uint16_t ToHost16(uint16_t x) { return absl::little_endian::ToHost16(x); }
    static uint16_t Load16(const void* p) { return absl::little_endian::Load16(p); }
    static void Store16(void* p, uint16_t v) { absl::little_endian::Store16(p, v); }

    static uint32_t FromHost32(uint32_t x) { return absl::little_endian::FromHost32(x); }
    static uint32_t ToHost32(uint32_t x) { return absl::little_endian::ToHost32(x); }
    static uint32_t Load32(const void* p) { return absl::little_endian::Load32(p); }
    static void Store32(void* p, uint32_t v) { absl::little_endian::Store32(p, v); }

    static uint64_t FromHost64(uint64_t x) { return absl::little_endian::FromHost64(x); }
    static uint64_t ToHost64(uint64_t x) { return absl::little_endian::ToHost64(x); }
    static uint64_t Load64(const void* p) { return absl::little_endian::Load64(p); }
    static void Store64(void* p, uint64_t v) { absl::little_endian::Store64(p, v); }

    static unsigned __int128 FromHost128(unsigned __int128 x) {
#if defined(ABSL_IS_LITTLE_ENDIAN)
        return x;
#else
        return gbswap_128(x);
#endif
    }
    static unsigned __int128 ToHost128(unsigned __int128 x) { return FromHost128(x); }

    static bool IsLittleEndian() {
#if defined(ABSL_IS_LITTLE_ENDIAN)
        return true;
#else
        return false;
#endif
    }
};
