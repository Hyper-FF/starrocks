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
//
// AggHashMapVariant methods live in agg_hash_map_variant.cpp.
// AggHashSetVariant methods live in agg_hash_set_variant.cpp.
// This file contains only HashVariantResolver and fail-point definitions.

#include "exec/aggregate/agg_hash_variant.h"

#include <tuple>

#include "base/phmap/phmap.h"

namespace starrocks {

// HashVariantResolver
template <typename HashVariantType>
HashVariantResolver<HashVariantType>::HashVariantResolver() {
#define VARIANT_EMPLACE(...) CHECK(_types.emplace(__VA_ARGS__).second)

#define VARIANT_TYPE(value) HashVariantType::Type::value
#define ADD_VARIANT_PHASE1_TYPE(LOGICAL_TYPE, VALUE)                                                     \
    VARIANT_EMPLACE(std::make_tuple(AggrPhase1, LOGICAL_TYPE, true), VARIANT_TYPE(phase1_null_##VALUE)); \
    VARIANT_EMPLACE(std::make_tuple(AggrPhase1, LOGICAL_TYPE, false), VARIANT_TYPE(phase1_##VALUE));     \
    VARIANT_EMPLACE(std::make_tuple(AggrPhase2, LOGICAL_TYPE, true), VARIANT_TYPE(phase2_null_##VALUE)); \
    VARIANT_EMPLACE(std::make_tuple(AggrPhase2, LOGICAL_TYPE, false), VARIANT_TYPE(phase2_##VALUE));

    ADD_VARIANT_PHASE1_TYPE(TYPE_BOOLEAN, uint8);
    ADD_VARIANT_PHASE1_TYPE(TYPE_TINYINT, int8);
    ADD_VARIANT_PHASE1_TYPE(TYPE_SMALLINT, int16);
    ADD_VARIANT_PHASE1_TYPE(TYPE_INT, int32);
    ADD_VARIANT_PHASE1_TYPE(TYPE_DECIMAL32, decimal32);
    ADD_VARIANT_PHASE1_TYPE(TYPE_BIGINT, int64);
    ADD_VARIANT_PHASE1_TYPE(TYPE_DECIMAL64, decimal64);
    ADD_VARIANT_PHASE1_TYPE(TYPE_DATE, date);
    ADD_VARIANT_PHASE1_TYPE(TYPE_DATETIME, timestamp);
    ADD_VARIANT_PHASE1_TYPE(TYPE_DECIMAL128, decimal128);
    ADD_VARIANT_PHASE1_TYPE(TYPE_DECIMAL256, decimal256);
    ADD_VARIANT_PHASE1_TYPE(TYPE_LARGEINT, int128);
    ADD_VARIANT_PHASE1_TYPE(TYPE_CHAR, string);
    ADD_VARIANT_PHASE1_TYPE(TYPE_VARCHAR, string);
}

template <typename HashVariantType>
auto HashVariantResolver<HashVariantType>::instance() -> HashVariantResolver<HashVariantType>& {
    static HashVariantResolver resolver;
    return resolver;
}

template class HashVariantResolver<AggHashSetVariant>;
template class HashVariantResolver<AggHashMapVariant>;

DEFINE_FAIL_POINT(agg_hash_set_bad_alloc);
DEFINE_FAIL_POINT(aggregate_build_hash_map_bad_alloc);

} // namespace starrocks
