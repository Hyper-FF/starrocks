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

#include "exec/aggregate/agg_hash_variant_type_traits.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace detail {

template <AggHashMapVariant::Type>
struct AggHashMapVariantTypeTraits;

#define DEFINE_MAP_TYPE(enum_value, type)            \
    template <>                                      \
    struct AggHashMapVariantTypeTraits<enum_value> { \
        using HashMapWithKeyType = type;             \
    };
APPLY_FOR_AGG_MAP_VARIANT_TYPES(DEFINE_MAP_TYPE)
#undef DEFINE_MAP_TYPE

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
