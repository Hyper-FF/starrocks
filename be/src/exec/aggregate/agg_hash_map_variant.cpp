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
#include "exec/aggregate/agg_hash_variant.h"

#include <variant>

#include "runtime/runtime_state.h"

#define APPLY_FOR_AGG_VARIANT_ALL(M) \
    M(phase1_uint8)                  \
    M(phase1_int8)                   \
    M(phase1_int16)                  \
    M(phase1_int32)                  \
    M(phase1_int64)                  \
    M(phase1_int128)                 \
    M(phase1_decimal32)              \
    M(phase1_decimal64)              \
    M(phase1_decimal128)             \
    M(phase1_decimal256)             \
    M(phase1_date)                   \
    M(phase1_timestamp)              \
    M(phase1_string)                 \
    M(phase1_slice)                  \
    M(phase1_null_uint8)             \
    M(phase1_null_int8)              \
    M(phase1_null_int16)             \
    M(phase1_null_int32)             \
    M(phase1_null_int64)             \
    M(phase1_null_int128)            \
    M(phase1_null_decimal32)         \
    M(phase1_null_decimal64)         \
    M(phase1_null_decimal128)        \
    M(phase1_null_decimal256)        \
    M(phase1_null_date)              \
    M(phase1_null_timestamp)         \
    M(phase1_null_string)            \
    M(phase1_slice_two_level)        \
    M(phase1_int32_two_level)        \
    M(phase1_null_string_two_level)  \
    M(phase1_string_two_level)       \
                                     \
    M(phase2_uint8)                  \
    M(phase2_int8)                   \
    M(phase2_int16)                  \
    M(phase2_int32)                  \
    M(phase2_int64)                  \
    M(phase2_int128)                 \
    M(phase2_decimal32)              \
    M(phase2_decimal64)              \
    M(phase2_decimal128)             \
    M(phase2_decimal256)             \
    M(phase2_date)                   \
    M(phase2_timestamp)              \
    M(phase2_string)                 \
    M(phase2_slice)                  \
    M(phase2_null_uint8)             \
    M(phase2_null_int8)              \
    M(phase2_null_int16)             \
    M(phase2_null_int32)             \
    M(phase2_null_int64)             \
    M(phase2_null_int128)            \
    M(phase2_null_decimal32)         \
    M(phase2_null_decimal64)         \
    M(phase2_null_decimal128)        \
    M(phase2_null_decimal256)        \
    M(phase2_null_date)              \
    M(phase2_null_timestamp)         \
    M(phase2_null_string)            \
    M(phase2_slice_two_level)        \
    M(phase2_int32_two_level)        \
    M(phase2_null_string_two_level)  \
    M(phase2_string_two_level)       \
                                     \
    M(phase1_slice_fx4)              \
    M(phase1_slice_fx8)              \
    M(phase1_slice_fx16)             \
    M(phase2_slice_fx4)              \
    M(phase2_slice_fx8)              \
    M(phase2_slice_fx16)             \
    M(phase1_slice_cx1)              \
    M(phase1_slice_cx4)              \
    M(phase1_slice_cx8)              \
    M(phase1_slice_cx16)             \
    M(phase2_slice_cx1)              \
    M(phase2_slice_cx4)              \
    M(phase2_slice_cx8)              \
    M(phase2_slice_cx16)

namespace starrocks {
namespace detail {
template <AggHashMapVariant::Type>
struct AggHashMapVariantTypeTraits;

#define DEFINE_MAP_TYPE(enum_value, type)            \
    template <>                                      \
    struct AggHashMapVariantTypeTraits<enum_value> { \
        using HashMapWithKeyType = type;             \
    }

DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_uint8, UInt8AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int8, Int8AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int16, Int16AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int32, Int32AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int64, Int64AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int128, Int128AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_decimal32, Decimal32AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_decimal64, Decimal64AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_decimal128, Decimal128AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_decimal256, Decimal256AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_date, DateAggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_timestamp, TimeStampAggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_string, OneStringAggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_uint8, NullUInt8AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_int8, NullInt8AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_int16, NullInt16AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_int32, NullInt32AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_int64, NullInt64AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_int128, NullInt128AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_decimal32, NullDecimal32AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_decimal64, NullDecimal64AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_decimal128, NullDecimal128AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_decimal256, NullDecimal256AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_date, NullDateAggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_timestamp, NullTimeStampAggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_string, NullOneStringAggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice, SerializedKeyAggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_two_level, SerializedKeyTwoLevelAggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int32_two_level, Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_string_two_level, NullOneStringTwoLevelAggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_string_two_level, OneStringTwoLevelAggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_fx4, SerializedKeyFixedSize4AggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_fx8, SerializedKeyFixedSize8AggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_fx16, SerializedKeyFixedSize16AggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_cx1, CompressedFixedSize1AggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_cx4, CompressedFixedSize4AggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_cx8, CompressedFixedSize8AggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_cx16, CompressedFixedSize16AggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_uint8, UInt8AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int8, Int8AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int16, Int16AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int32, Int32AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int64, Int64AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int128, Int128AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_decimal32, Decimal32AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_decimal64, Decimal64AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_decimal128, Decimal128AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_decimal256, Decimal256AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_date, DateAggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_timestamp, TimeStampAggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_string, OneStringAggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_uint8, NullUInt8AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_int8, NullInt8AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_int16, NullInt16AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_int32, NullInt32AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_int64, NullInt64AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_int128, NullInt128AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_decimal32, NullDecimal32AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_decimal64, NullDecimal64AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_decimal128, NullDecimal128AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_decimal256, NullDecimal256AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_date, NullDateAggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_timestamp, NullTimeStampAggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_string, NullOneStringAggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice, SerializedKeyAggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_two_level, SerializedKeyTwoLevelAggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int32_two_level, Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_string_two_level, NullOneStringTwoLevelAggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_string_two_level, OneStringTwoLevelAggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_fx4, SerializedKeyFixedSize4AggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_fx8, SerializedKeyFixedSize8AggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_fx16, SerializedKeyFixedSize16AggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_cx1, CompressedFixedSize1AggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_cx4, CompressedFixedSize4AggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_cx8, CompressedFixedSize8AggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_cx16, CompressedFixedSize16AggHashMap<PhmapSeed2>);

} // namespace detail
void AggHashMapVariant::init(RuntimeState* state, Type type, AggStatistics* agg_stat) {
    _type = type;
    _agg_stat = agg_stat;
    switch (_type) {
#define M(NAME)                                                                                                    \
    case Type::NAME:                                                                                               \
        hash_map_with_key = std::make_unique<detail::AggHashMapVariantTypeTraits<Type::NAME>::HashMapWithKeyType>( \
                state->chunk_size(), _agg_stat);                                                                   \
        break;
        APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
    }
}

#define CONVERT_TO_TWO_LEVEL_MAP(DST, SRC)                                                                            \
    if (_type == AggHashMapVariant::Type::SRC) {                                                                      \
        auto dst = std::make_unique<detail::AggHashMapVariantTypeTraits<Type::DST>::HashMapWithKeyType>(              \
                state->chunk_size(), _agg_stat);                                                                      \
        std::visit(                                                                                                   \
                [&](auto& hash_map_with_key) {                                                                        \
                    if constexpr (std::is_same_v<typename decltype(hash_map_with_key->hash_map)::key_type,            \
                                                 typename decltype(dst->hash_map)::key_type>) {                       \
                        dst->hash_map.reserve(hash_map_with_key->hash_map.capacity());                                \
                        dst->hash_map.insert(hash_map_with_key->hash_map.begin(), hash_map_with_key->hash_map.end()); \
                        auto null_data_ptr = hash_map_with_key->get_null_key_data();                                  \
                        if (null_data_ptr != nullptr) {                                                               \
                            dst->set_null_key_data(null_data_ptr);                                                    \
                        }                                                                                             \
                    }                                                                                                 \
                },                                                                                                    \
                hash_map_with_key);                                                                                   \
                                                                                                                      \
        _type = AggHashMapVariant::Type::DST;                                                                         \
        hash_map_with_key = std::move(dst);                                                                           \
        return;                                                                                                       \
    }

void AggHashMapVariant::convert_to_two_level(RuntimeState* state) {
    CONVERT_TO_TWO_LEVEL_MAP(phase1_slice_two_level, phase1_slice);
    CONVERT_TO_TWO_LEVEL_MAP(phase2_slice_two_level, phase2_slice);

    CONVERT_TO_TWO_LEVEL_MAP(phase1_string_two_level, phase1_string);
    CONVERT_TO_TWO_LEVEL_MAP(phase2_string_two_level, phase2_string);

    CONVERT_TO_TWO_LEVEL_MAP(phase1_null_string_two_level, phase1_null_string);
    CONVERT_TO_TWO_LEVEL_MAP(phase2_null_string_two_level, phase2_null_string);
}

void AggHashMapVariant::reset() {
    detail::AggHashMapWithKeyPtr ptr;
    hash_map_with_key = std::move(ptr);
}

size_t AggHashMapVariant::capacity() const {
    return visit([](const auto& hash_map_with_key) { return hash_map_with_key->hash_map.capacity(); });
}

size_t AggHashMapVariant::size() const {
    return visit([](const auto& hash_map_with_key) {
        return hash_map_with_key->hash_map.size() + (hash_map_with_key->get_null_key_data() != nullptr);
    });
}

bool AggHashMapVariant::need_expand(size_t increasement) const {
    size_t capacity = this->capacity();
    // TODO: think about two-level hashmap
    size_t size = this->size() + increasement;
    // see detail implement in reset_growth_left
    return size >= capacity - capacity / 8;
}

size_t AggHashMapVariant::reserved_memory_usage(const MemPool* pool) const {
    return visit([pool](const auto& hash_map_with_key) {
        size_t pool_bytes = (pool != nullptr) ? pool->total_reserved_bytes() : 0;
        return hash_map_with_key->hash_map.dump_bound() + pool_bytes;
    });
}

size_t AggHashMapVariant::allocated_memory_usage(const MemPool* pool) const {
    return visit([pool](const auto& hash_map_with_key) {
        return sizeof(typename decltype(hash_map_with_key->hash_map)::key_type) *
                       hash_map_with_key->hash_map.capacity() +
               pool->total_allocated_bytes();
    });
}

} // namespace starrocks
