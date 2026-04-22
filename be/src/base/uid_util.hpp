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

#include "base/uid_util.h"

namespace starrocks {

// convert int to a hex format string, buf must enough to hold coverted hex string
template <typename T>
inline void to_hex(T val, char* buf) {
    static const char* digits = "0123456789abcdef";
    for (int i = 0; i < 2 * sizeof(T); ++i) {
        buf[2 * sizeof(T) - 1 - i] = digits[val & 0x0F];
        val >>= 4;
    }
}

template <typename T>
inline void from_hex(T* ret, std::string_view buf) {
    T val = 0;
    for (char c : buf) {
        int buf_val = 0;
        if (c >= '0' && c <= '9')
            buf_val = c - '0';
        else {
            buf_val = c - 'a' + 10;
        }
        val <<= 4;
        val = val | buf_val;
    }
    *ret = val;
}

inline UniqueId::UniqueId(int64_t hi_, int64_t lo_) : hi(hi_), lo(lo_) {}

inline UniqueId::UniqueId(const TUniqueId& tuid) : hi(tuid.hi), lo(tuid.lo) {}

inline UniqueId::UniqueId(const PUniqueId& puid) : hi(puid.hi()), lo(puid.lo()) {}

inline UniqueId::UniqueId(std::string_view hi_str, std::string_view lo_str) {
    from_hex(&hi, hi_str);
    from_hex(&lo, lo_str);
}

inline std::string UniqueId::to_string() const {
    char buf[33];
    to_hex(hi, buf);
    buf[16] = '-';
    to_hex(lo, buf + 17);
    return {buf, 33};
}

inline bool UniqueId::operator<(const UniqueId& right) const {
    if (hi != right.hi) {
        return hi < right.hi;
    } else {
        return lo < right.lo;
    }
}

inline size_t UniqueId::hash(size_t seed) const {
    return starrocks::HashUtil::hash(this, sizeof(*this), seed);
}

inline bool UniqueId::operator==(const UniqueId& rhs) const {
    return hi == rhs.hi && lo == rhs.lo;
}

inline bool UniqueId::operator!=(const UniqueId& rhs) const {
    return hi != rhs.hi || lo != rhs.lo;
}

inline TUniqueId UniqueId::to_thrift() const {
    TUniqueId tid;
    tid.__set_hi(hi);
    tid.__set_lo(lo);
    return tid;
}

inline PUniqueId UniqueId::to_proto() const {
    PUniqueId pid;
    pid.set_hi(hi);
    pid.set_lo(lo);
    return pid;
}

// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const starrocks::TUniqueId& id) {
    std::size_t seed = 0;
    HashUtil::hash_combine(seed, id.lo);
    HashUtil::hash_combine(seed, id.hi);
    return seed;
}

} // namespace starrocks

namespace std {

template <>
struct hash<starrocks::TUniqueId> {
    std::size_t operator()(const starrocks::TUniqueId& id) const {
        std::size_t seed = 0;
        seed = starrocks::HashUtil::hash(&id.lo, sizeof(id.lo), seed);
        seed = starrocks::HashUtil::hash(&id.hi, sizeof(id.hi), seed);
        return seed;
    }
};

template <>
struct hash<starrocks::TNetworkAddress> {
    size_t operator()(const starrocks::TNetworkAddress& address) const {
        std::size_t seed = 0;
        seed = starrocks::HashUtil::hash(address.hostname.data(), address.hostname.size(), seed);
        seed = starrocks::HashUtil::hash(&address.port, 4, seed);
        return seed;
    }
};

template <>
struct hash<starrocks::UniqueId> {
    size_t operator()(const starrocks::UniqueId& uid) const { return uid.hash(); }
};

} // namespace std
