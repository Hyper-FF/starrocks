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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/uid_util.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <ostream>
#include <string>
#include <string_view>

#include "base/hash/hash_util.hpp"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "gen_cpp/types.pb.h"    // for PUniqueId

namespace starrocks {

// convert int to a hex format string, buf must enough to hold coverted hex string
template <typename T>
inline void to_hex(T val, char* buf);

template <typename T>
inline void from_hex(T* ret, std::string_view buf);

struct UniqueId {
    int64_t hi = 0;
    int64_t lo = 0;

    UniqueId() = default;
    UniqueId(int64_t hi_, int64_t lo_);
    UniqueId(const TUniqueId& tuid);
    UniqueId(const PUniqueId& puid);
    UniqueId(std::string_view hi_str, std::string_view lo_str);

    // currently, the implementation is uuid, but it may change in the future
    static UniqueId gen_uid();

    ~UniqueId() noexcept = default;

    std::string to_string() const;

    // std::map std::set needs this operator
    bool operator<(const UniqueId& right) const;

    // std::unordered_map need this api
    size_t hash(size_t seed = 0) const;

    // std::unordered_map need this api
    bool operator==(const UniqueId& rhs) const;

    bool operator!=(const UniqueId& rhs) const;

    TUniqueId to_thrift() const;

    PUniqueId to_proto() const;
};

/// generates a 16 byte UUID
std::string generate_uuid_string();

/// generates a 16 byte UUID
TUniqueId generate_uuid();

bool parse_id(const std::string& s, TUniqueId* id);

std::ostream& operator<<(std::ostream& os, const UniqueId& uid);

std::string print_id(const UniqueId& id);
std::string print_id(const TUniqueId& id);
std::string print_id(const PUniqueId& id);

} // namespace starrocks

#include "base/uid_util.hpp"
