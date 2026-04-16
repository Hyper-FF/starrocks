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
// Internal header shared by agg_hash_map_variant.cpp and agg_hash_set_variant.cpp.
// Provides single-source-of-truth macros for variant enum names and type mappings.
// NOT intended for inclusion outside the aggregate/ implementation files.

#pragma once

// Applies M(name) for every variant enum value shared by both
// AggHashMapVariant::Type and AggHashSetVariant::Type.
// Used in init() switch statements.
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

// Applies M(enum_value, concrete_type) for every AggHashMapVariant type mapping.
// Single source of truth for the enum -> hash-map-with-key type relationship.
#define APPLY_FOR_AGG_MAP_VARIANT_TYPES(M)                                                                              \
    M(AggHashMapVariant::Type::phase1_uint8, UInt8AggHashMapWithOneNumberKey<PhmapSeed1>)                               \
    M(AggHashMapVariant::Type::phase1_int8, Int8AggHashMapWithOneNumberKey<PhmapSeed1>)                                 \
    M(AggHashMapVariant::Type::phase1_int16, Int16AggHashMapWithOneNumberKey<PhmapSeed1>)                               \
    M(AggHashMapVariant::Type::phase1_int32, Int32AggHashMapWithOneNumberKey<PhmapSeed1>)                               \
    M(AggHashMapVariant::Type::phase1_int64, Int64AggHashMapWithOneNumberKey<PhmapSeed1>)                               \
    M(AggHashMapVariant::Type::phase1_int128, Int128AggHashMapWithOneNumberKey<PhmapSeed1>)                             \
    M(AggHashMapVariant::Type::phase1_decimal32, Decimal32AggHashMapWithOneNumberKey<PhmapSeed1>)                       \
    M(AggHashMapVariant::Type::phase1_decimal64, Decimal64AggHashMapWithOneNumberKey<PhmapSeed1>)                       \
    M(AggHashMapVariant::Type::phase1_decimal128, Decimal128AggHashMapWithOneNumberKey<PhmapSeed1>)                     \
    M(AggHashMapVariant::Type::phase1_decimal256, Decimal256AggHashMapWithOneNumberKey<PhmapSeed1>)                     \
    M(AggHashMapVariant::Type::phase1_date, DateAggHashMapWithOneNumberKey<PhmapSeed1>)                                 \
    M(AggHashMapVariant::Type::phase1_timestamp, TimeStampAggHashMapWithOneNumberKey<PhmapSeed1>)                       \
    M(AggHashMapVariant::Type::phase1_string, OneStringAggHashMap<PhmapSeed1>)                                          \
    M(AggHashMapVariant::Type::phase1_null_uint8, NullUInt8AggHashMapWithOneNumberKey<PhmapSeed1>)                      \
    M(AggHashMapVariant::Type::phase1_null_int8, NullInt8AggHashMapWithOneNumberKey<PhmapSeed1>)                        \
    M(AggHashMapVariant::Type::phase1_null_int16, NullInt16AggHashMapWithOneNumberKey<PhmapSeed1>)                      \
    M(AggHashMapVariant::Type::phase1_null_int32, NullInt32AggHashMapWithOneNumberKey<PhmapSeed1>)                      \
    M(AggHashMapVariant::Type::phase1_null_int64, NullInt64AggHashMapWithOneNumberKey<PhmapSeed1>)                      \
    M(AggHashMapVariant::Type::phase1_null_int128, NullInt128AggHashMapWithOneNumberKey<PhmapSeed1>)                    \
    M(AggHashMapVariant::Type::phase1_null_decimal32, NullDecimal32AggHashMapWithOneNumberKey<PhmapSeed1>)              \
    M(AggHashMapVariant::Type::phase1_null_decimal64, NullDecimal64AggHashMapWithOneNumberKey<PhmapSeed1>)              \
    M(AggHashMapVariant::Type::phase1_null_decimal128, NullDecimal128AggHashMapWithOneNumberKey<PhmapSeed1>)            \
    M(AggHashMapVariant::Type::phase1_null_decimal256, NullDecimal256AggHashMapWithOneNumberKey<PhmapSeed1>)            \
    M(AggHashMapVariant::Type::phase1_null_date, NullDateAggHashMapWithOneNumberKey<PhmapSeed1>)                        \
    M(AggHashMapVariant::Type::phase1_null_timestamp, NullTimeStampAggHashMapWithOneNumberKey<PhmapSeed1>)              \
    M(AggHashMapVariant::Type::phase1_null_string, NullOneStringAggHashMap<PhmapSeed1>)                                 \
    M(AggHashMapVariant::Type::phase1_slice, SerializedKeyAggHashMap<PhmapSeed1>)                                       \
    M(AggHashMapVariant::Type::phase1_slice_two_level, SerializedKeyTwoLevelAggHashMap<PhmapSeed1>)                     \
    M(AggHashMapVariant::Type::phase1_int32_two_level, Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed1>)             \
    M(AggHashMapVariant::Type::phase1_null_string_two_level, NullOneStringTwoLevelAggHashMap<PhmapSeed1>)               \
    M(AggHashMapVariant::Type::phase1_string_two_level, OneStringTwoLevelAggHashMap<PhmapSeed1>)                        \
    M(AggHashMapVariant::Type::phase1_slice_fx4, SerializedKeyFixedSize4AggHashMap<PhmapSeed1>)                         \
    M(AggHashMapVariant::Type::phase1_slice_fx8, SerializedKeyFixedSize8AggHashMap<PhmapSeed1>)                         \
    M(AggHashMapVariant::Type::phase1_slice_fx16, SerializedKeyFixedSize16AggHashMap<PhmapSeed1>)                       \
    M(AggHashMapVariant::Type::phase1_slice_cx1, CompressedFixedSize1AggHashMap<PhmapSeed1>)                            \
    M(AggHashMapVariant::Type::phase1_slice_cx4, CompressedFixedSize4AggHashMap<PhmapSeed1>)                            \
    M(AggHashMapVariant::Type::phase1_slice_cx8, CompressedFixedSize8AggHashMap<PhmapSeed1>)                            \
    M(AggHashMapVariant::Type::phase1_slice_cx16, CompressedFixedSize16AggHashMap<PhmapSeed1>)                          \
    M(AggHashMapVariant::Type::phase2_uint8, UInt8AggHashMapWithOneNumberKey<PhmapSeed2>)                               \
    M(AggHashMapVariant::Type::phase2_int8, Int8AggHashMapWithOneNumberKey<PhmapSeed2>)                                 \
    M(AggHashMapVariant::Type::phase2_int16, Int16AggHashMapWithOneNumberKey<PhmapSeed2>)                               \
    M(AggHashMapVariant::Type::phase2_int32, Int32AggHashMapWithOneNumberKey<PhmapSeed2>)                               \
    M(AggHashMapVariant::Type::phase2_int64, Int64AggHashMapWithOneNumberKey<PhmapSeed2>)                               \
    M(AggHashMapVariant::Type::phase2_int128, Int128AggHashMapWithOneNumberKey<PhmapSeed2>)                             \
    M(AggHashMapVariant::Type::phase2_decimal32, Decimal32AggHashMapWithOneNumberKey<PhmapSeed2>)                       \
    M(AggHashMapVariant::Type::phase2_decimal64, Decimal64AggHashMapWithOneNumberKey<PhmapSeed2>)                       \
    M(AggHashMapVariant::Type::phase2_decimal128, Decimal128AggHashMapWithOneNumberKey<PhmapSeed2>)                     \
    M(AggHashMapVariant::Type::phase2_decimal256, Decimal256AggHashMapWithOneNumberKey<PhmapSeed2>)                     \
    M(AggHashMapVariant::Type::phase2_date, DateAggHashMapWithOneNumberKey<PhmapSeed2>)                                 \
    M(AggHashMapVariant::Type::phase2_timestamp, TimeStampAggHashMapWithOneNumberKey<PhmapSeed2>)                       \
    M(AggHashMapVariant::Type::phase2_string, OneStringAggHashMap<PhmapSeed2>)                                          \
    M(AggHashMapVariant::Type::phase2_null_uint8, NullUInt8AggHashMapWithOneNumberKey<PhmapSeed2>)                      \
    M(AggHashMapVariant::Type::phase2_null_int8, NullInt8AggHashMapWithOneNumberKey<PhmapSeed2>)                        \
    M(AggHashMapVariant::Type::phase2_null_int16, NullInt16AggHashMapWithOneNumberKey<PhmapSeed2>)                      \
    M(AggHashMapVariant::Type::phase2_null_int32, NullInt32AggHashMapWithOneNumberKey<PhmapSeed2>)                      \
    M(AggHashMapVariant::Type::phase2_null_int64, NullInt64AggHashMapWithOneNumberKey<PhmapSeed2>)                      \
    M(AggHashMapVariant::Type::phase2_null_int128, NullInt128AggHashMapWithOneNumberKey<PhmapSeed2>)                    \
    M(AggHashMapVariant::Type::phase2_null_decimal32, NullDecimal32AggHashMapWithOneNumberKey<PhmapSeed2>)              \
    M(AggHashMapVariant::Type::phase2_null_decimal64, NullDecimal64AggHashMapWithOneNumberKey<PhmapSeed2>)              \
    M(AggHashMapVariant::Type::phase2_null_decimal128, NullDecimal128AggHashMapWithOneNumberKey<PhmapSeed2>)            \
    M(AggHashMapVariant::Type::phase2_null_decimal256, NullDecimal256AggHashMapWithOneNumberKey<PhmapSeed2>)            \
    M(AggHashMapVariant::Type::phase2_null_date, NullDateAggHashMapWithOneNumberKey<PhmapSeed2>)                        \
    M(AggHashMapVariant::Type::phase2_null_timestamp, NullTimeStampAggHashMapWithOneNumberKey<PhmapSeed2>)              \
    M(AggHashMapVariant::Type::phase2_null_string, NullOneStringAggHashMap<PhmapSeed2>)                                 \
    M(AggHashMapVariant::Type::phase2_slice, SerializedKeyAggHashMap<PhmapSeed2>)                                       \
    M(AggHashMapVariant::Type::phase2_slice_two_level, SerializedKeyTwoLevelAggHashMap<PhmapSeed2>)                     \
    M(AggHashMapVariant::Type::phase2_int32_two_level, Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed2>)             \
    M(AggHashMapVariant::Type::phase2_null_string_two_level, NullOneStringTwoLevelAggHashMap<PhmapSeed2>)               \
    M(AggHashMapVariant::Type::phase2_string_two_level, OneStringTwoLevelAggHashMap<PhmapSeed2>)                        \
    M(AggHashMapVariant::Type::phase2_slice_fx4, SerializedKeyFixedSize4AggHashMap<PhmapSeed2>)                         \
    M(AggHashMapVariant::Type::phase2_slice_fx8, SerializedKeyFixedSize8AggHashMap<PhmapSeed2>)                         \
    M(AggHashMapVariant::Type::phase2_slice_fx16, SerializedKeyFixedSize16AggHashMap<PhmapSeed2>)                       \
    M(AggHashMapVariant::Type::phase2_slice_cx1, CompressedFixedSize1AggHashMap<PhmapSeed2>)                            \
    M(AggHashMapVariant::Type::phase2_slice_cx4, CompressedFixedSize4AggHashMap<PhmapSeed2>)                            \
    M(AggHashMapVariant::Type::phase2_slice_cx8, CompressedFixedSize8AggHashMap<PhmapSeed2>)                            \
    M(AggHashMapVariant::Type::phase2_slice_cx16, CompressedFixedSize16AggHashMap<PhmapSeed2>)

// Applies M(enum_value, concrete_type) for every AggHashSetVariant type mapping.
// Single source of truth for the enum -> hash-set-with-key type relationship.
#define APPLY_FOR_AGG_SET_VARIANT_TYPES(M)                                                                              \
    M(AggHashSetVariant::Type::phase1_uint8, UInt8AggHashSetOfOneNumberKey<PhmapSeed1>)                                 \
    M(AggHashSetVariant::Type::phase1_int8, Int8AggHashSetOfOneNumberKey<PhmapSeed1>)                                   \
    M(AggHashSetVariant::Type::phase1_int16, Int16AggHashSetOfOneNumberKey<PhmapSeed1>)                                 \
    M(AggHashSetVariant::Type::phase1_int32, Int32AggHashSetOfOneNumberKey<PhmapSeed1>)                                 \
    M(AggHashSetVariant::Type::phase1_int64, Int64AggHashSetOfOneNumberKey<PhmapSeed1>)                                 \
    M(AggHashSetVariant::Type::phase1_int128, Int128AggHashSetOfOneNumberKey<PhmapSeed1>)                               \
    M(AggHashSetVariant::Type::phase1_decimal32, Decimal32AggHashSetOfOneNumberKey<PhmapSeed1>)                         \
    M(AggHashSetVariant::Type::phase1_decimal64, Decimal64AggHashSetOfOneNumberKey<PhmapSeed1>)                         \
    M(AggHashSetVariant::Type::phase1_decimal128, Decimal128AggHashSetOfOneNumberKey<PhmapSeed1>)                       \
    M(AggHashSetVariant::Type::phase1_decimal256, Decimal256AggHashSetOfOneNumberKey<PhmapSeed1>)                       \
    M(AggHashSetVariant::Type::phase1_date, DateAggHashSetOfOneNumberKey<PhmapSeed1>)                                   \
    M(AggHashSetVariant::Type::phase1_timestamp, TimeStampAggHashSetOfOneNumberKey<PhmapSeed1>)                         \
    M(AggHashSetVariant::Type::phase1_string, OneStringAggHashSet<PhmapSeed1>)                                          \
    M(AggHashSetVariant::Type::phase1_null_uint8, NullUInt8AggHashSetOfOneNumberKey<PhmapSeed1>)                        \
    M(AggHashSetVariant::Type::phase1_null_int8, NullInt8AggHashSetOfOneNumberKey<PhmapSeed1>)                          \
    M(AggHashSetVariant::Type::phase1_null_int16, NullInt16AggHashSetOfOneNumberKey<PhmapSeed1>)                        \
    M(AggHashSetVariant::Type::phase1_null_int32, NullInt32AggHashSetOfOneNumberKey<PhmapSeed1>)                        \
    M(AggHashSetVariant::Type::phase1_null_int64, NullInt64AggHashSetOfOneNumberKey<PhmapSeed1>)                        \
    M(AggHashSetVariant::Type::phase1_null_int128, NullInt128AggHashSetOfOneNumberKey<PhmapSeed1>)                      \
    M(AggHashSetVariant::Type::phase1_null_decimal32, NullDecimal32AggHashSetOfOneNumberKey<PhmapSeed1>)                \
    M(AggHashSetVariant::Type::phase1_null_decimal64, NullDecimal64AggHashSetOfOneNumberKey<PhmapSeed1>)                \
    M(AggHashSetVariant::Type::phase1_null_decimal128, NullDecimal128AggHashSetOfOneNumberKey<PhmapSeed1>)              \
    M(AggHashSetVariant::Type::phase1_null_decimal256, NullDecimal256AggHashSetOfOneNumberKey<PhmapSeed1>)              \
    M(AggHashSetVariant::Type::phase1_null_date, NullDateAggHashSetOfOneNumberKey<PhmapSeed1>)                          \
    M(AggHashSetVariant::Type::phase1_null_timestamp, NullTimeStampAggHashSetOfOneNumberKey<PhmapSeed1>)                \
    M(AggHashSetVariant::Type::phase1_null_string, NullOneStringAggHashSet<PhmapSeed1>)                                 \
    M(AggHashSetVariant::Type::phase1_slice, SerializedKeyAggHashSet<PhmapSeed1>)                                       \
    M(AggHashSetVariant::Type::phase1_slice_two_level, SerializedTwoLevelKeyAggHashSet<PhmapSeed1>)                     \
    M(AggHashSetVariant::Type::phase1_int32_two_level, Int32TwoLevelAggHashSetOfOneNumberKey<PhmapSeed1>)               \
    M(AggHashSetVariant::Type::phase1_string_two_level, OneStringTwoLevelAggHashSet<PhmapSeed1>)                        \
    M(AggHashSetVariant::Type::phase1_null_string_two_level, NullOneStringTwoLevelAggHashSet<PhmapSeed1>)               \
    M(AggHashSetVariant::Type::phase2_uint8, UInt8AggHashSetOfOneNumberKey<PhmapSeed2>)                                 \
    M(AggHashSetVariant::Type::phase2_int8, Int8AggHashSetOfOneNumberKey<PhmapSeed2>)                                   \
    M(AggHashSetVariant::Type::phase2_int16, Int16AggHashSetOfOneNumberKey<PhmapSeed2>)                                 \
    M(AggHashSetVariant::Type::phase2_int32, Int32AggHashSetOfOneNumberKey<PhmapSeed2>)                                 \
    M(AggHashSetVariant::Type::phase2_int64, Int64AggHashSetOfOneNumberKey<PhmapSeed2>)                                 \
    M(AggHashSetVariant::Type::phase2_int128, Int128AggHashSetOfOneNumberKey<PhmapSeed2>)                               \
    M(AggHashSetVariant::Type::phase2_decimal32, Decimal32AggHashSetOfOneNumberKey<PhmapSeed2>)                         \
    M(AggHashSetVariant::Type::phase2_decimal64, Decimal64AggHashSetOfOneNumberKey<PhmapSeed2>)                         \
    M(AggHashSetVariant::Type::phase2_decimal128, Decimal128AggHashSetOfOneNumberKey<PhmapSeed2>)                       \
    M(AggHashSetVariant::Type::phase2_decimal256, Decimal256AggHashSetOfOneNumberKey<PhmapSeed2>)                       \
    M(AggHashSetVariant::Type::phase2_date, DateAggHashSetOfOneNumberKey<PhmapSeed2>)                                   \
    M(AggHashSetVariant::Type::phase2_timestamp, TimeStampAggHashSetOfOneNumberKey<PhmapSeed2>)                         \
    M(AggHashSetVariant::Type::phase2_string, OneStringAggHashSet<PhmapSeed2>)                                          \
    M(AggHashSetVariant::Type::phase2_null_uint8, NullUInt8AggHashSetOfOneNumberKey<PhmapSeed2>)                        \
    M(AggHashSetVariant::Type::phase2_null_int8, NullInt8AggHashSetOfOneNumberKey<PhmapSeed2>)                          \
    M(AggHashSetVariant::Type::phase2_null_int16, NullInt16AggHashSetOfOneNumberKey<PhmapSeed2>)                        \
    M(AggHashSetVariant::Type::phase2_null_int32, NullInt32AggHashSetOfOneNumberKey<PhmapSeed2>)                        \
    M(AggHashSetVariant::Type::phase2_null_int64, NullInt64AggHashSetOfOneNumberKey<PhmapSeed2>)                        \
    M(AggHashSetVariant::Type::phase2_null_int128, NullInt128AggHashSetOfOneNumberKey<PhmapSeed2>)                      \
    M(AggHashSetVariant::Type::phase2_null_decimal32, NullDecimal32AggHashSetOfOneNumberKey<PhmapSeed2>)                \
    M(AggHashSetVariant::Type::phase2_null_decimal64, NullDecimal64AggHashSetOfOneNumberKey<PhmapSeed2>)                \
    M(AggHashSetVariant::Type::phase2_null_decimal128, NullDecimal128AggHashSetOfOneNumberKey<PhmapSeed2>)              \
    M(AggHashSetVariant::Type::phase2_null_decimal256, NullDecimal256AggHashSetOfOneNumberKey<PhmapSeed2>)              \
    M(AggHashSetVariant::Type::phase2_null_date, NullDateAggHashSetOfOneNumberKey<PhmapSeed2>)                          \
    M(AggHashSetVariant::Type::phase2_null_timestamp, NullTimeStampAggHashSetOfOneNumberKey<PhmapSeed2>)                \
    M(AggHashSetVariant::Type::phase2_null_string, NullOneStringAggHashSet<PhmapSeed2>)                                 \
    M(AggHashSetVariant::Type::phase2_slice, SerializedKeyAggHashSet<PhmapSeed2>)                                       \
    M(AggHashSetVariant::Type::phase2_slice_two_level, SerializedTwoLevelKeyAggHashSet<PhmapSeed2>)                     \
    M(AggHashSetVariant::Type::phase2_int32_two_level, Int32TwoLevelAggHashSetOfOneNumberKey<PhmapSeed2>)               \
    M(AggHashSetVariant::Type::phase2_string_two_level, OneStringTwoLevelAggHashSet<PhmapSeed2>)                        \
    M(AggHashSetVariant::Type::phase2_null_string_two_level, NullOneStringTwoLevelAggHashSet<PhmapSeed2>)               \
    M(AggHashSetVariant::Type::phase1_slice_fx4, SerializedKeyAggHashSetFixedSize4<PhmapSeed1>)                         \
    M(AggHashSetVariant::Type::phase1_slice_fx8, SerializedKeyAggHashSetFixedSize8<PhmapSeed1>)                         \
    M(AggHashSetVariant::Type::phase1_slice_fx16, SerializedKeyAggHashSetFixedSize16<PhmapSeed1>)                       \
    M(AggHashSetVariant::Type::phase2_slice_fx4, SerializedKeyAggHashSetFixedSize4<PhmapSeed2>)                         \
    M(AggHashSetVariant::Type::phase2_slice_fx8, SerializedKeyAggHashSetFixedSize8<PhmapSeed2>)                         \
    M(AggHashSetVariant::Type::phase2_slice_fx16, SerializedKeyAggHashSetFixedSize16<PhmapSeed2>)                       \
    M(AggHashSetVariant::Type::phase1_slice_cx1, CompressedAggHashSetFixedSize1<PhmapSeed1>)                            \
    M(AggHashSetVariant::Type::phase1_slice_cx4, CompressedAggHashSetFixedSize4<PhmapSeed1>)                            \
    M(AggHashSetVariant::Type::phase1_slice_cx8, CompressedAggHashSetFixedSize8<PhmapSeed1>)                            \
    M(AggHashSetVariant::Type::phase1_slice_cx16, CompressedAggHashSetFixedSize16<PhmapSeed1>)                          \
    M(AggHashSetVariant::Type::phase2_slice_cx1, CompressedAggHashSetFixedSize1<PhmapSeed2>)                            \
    M(AggHashSetVariant::Type::phase2_slice_cx4, CompressedAggHashSetFixedSize4<PhmapSeed2>)                            \
    M(AggHashSetVariant::Type::phase2_slice_cx8, CompressedAggHashSetFixedSize8<PhmapSeed2>)                            \
    M(AggHashSetVariant::Type::phase2_slice_cx16, CompressedAggHashSetFixedSize16<PhmapSeed2>)
