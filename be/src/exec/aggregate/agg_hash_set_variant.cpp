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

template <AggHashSetVariant::Type>
struct AggHashSetVariantTypeTraits;

#define DEFINE_SET_TYPE(enum_value, type)            \
    template <>                                      \
    struct AggHashSetVariantTypeTraits<enum_value> { \
        using HashSetWithKeyType = type;             \
    };
APPLY_FOR_AGG_SET_VARIANT_TYPES(DEFINE_SET_TYPE)
#undef DEFINE_SET_TYPE

} // namespace detail

void AggHashSetVariant::init(RuntimeState* state, Type type, AggStatistics* agg_stat) {
    _type = type;
    _agg_stat = agg_stat;
    switch (_type) {
#define M(NAME)                                                                                                    \
    case Type::NAME:                                                                                               \
        hash_set_with_key = std::make_unique<detail::AggHashSetVariantTypeTraits<Type::NAME>::HashSetWithKeyType>( \
                state->chunk_size(), _agg_stat);                                                                   \
        break;
        APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
    }
}

#define CONVERT_TO_TWO_LEVEL_SET(DST, SRC)                                                                            \
    if (_type == AggHashSetVariant::Type::SRC) {                                                                      \
        auto dst = std::make_unique<detail::AggHashSetVariantTypeTraits<Type::DST>::HashSetWithKeyType>(              \
                state->chunk_size(), _agg_stat);                                                                      \
        std::visit(                                                                                                   \
                [&](auto& hash_set_with_key) {                                                                        \
                    if constexpr (std::is_same_v<typename decltype(hash_set_with_key->hash_set)::key_type,            \
                                                 typename decltype(dst->hash_set)::key_type>) {                       \
                        dst->hash_set.reserve(hash_set_with_key->hash_set.capacity());                                \
                        dst->hash_set.insert(hash_set_with_key->hash_set.begin(), hash_set_with_key->hash_set.end()); \
                        using SrcType = std::remove_reference_t<decltype(*hash_set_with_key)>;                        \
                        using DstType = std::remove_reference_t<decltype(*dst)>;                                      \
                        if constexpr (SrcType::has_single_null_key && DstType::has_single_null_key) {                 \
                            dst->has_null_key = hash_set_with_key->has_null_key;                                      \
                        }                                                                                             \
                    }                                                                                                 \
                },                                                                                                    \
                hash_set_with_key);                                                                                   \
        _type = AggHashSetVariant::Type::DST;                                                                         \
        hash_set_with_key = std::move(dst);                                                                           \
        return;                                                                                                       \
    }

void AggHashSetVariant::convert_to_two_level(RuntimeState* state) {
    CONVERT_TO_TWO_LEVEL_SET(phase1_slice_two_level, phase1_slice);
    CONVERT_TO_TWO_LEVEL_SET(phase2_slice_two_level, phase2_slice);

    CONVERT_TO_TWO_LEVEL_SET(phase1_string_two_level, phase1_string);
    CONVERT_TO_TWO_LEVEL_SET(phase2_string_two_level, phase2_string);

    CONVERT_TO_TWO_LEVEL_SET(phase1_null_string_two_level, phase1_null_string);
    CONVERT_TO_TWO_LEVEL_SET(phase2_null_string_two_level, phase2_null_string);
}

void AggHashSetVariant::reset() {
    detail::AggHashSetWithKeyPtr ptr;
    hash_set_with_key = std::move(ptr);
}

size_t AggHashSetVariant::capacity() const {
    return visit([](auto& hash_set_with_key) { return hash_set_with_key->hash_set.capacity(); });
}

size_t AggHashSetVariant::size() const {
    return visit([](auto& hash_set_with_key) {
        size_t sz = hash_set_with_key->hash_set.size();
        if constexpr (std::decay_t<decltype(*hash_set_with_key)>::has_single_null_key) {
            sz += hash_set_with_key->has_null_key ? 1 : 0;
        }
        return sz;
    });
}

bool AggHashSetVariant::need_expand(size_t increasement) const {
    size_t capacity = this->capacity();
    size_t size = this->size() + increasement;
    // see detail implement in reset_growth_left
    return size >= capacity - capacity / 8;
}

size_t AggHashSetVariant::reserved_memory_usage(const MemPool* pool) const {
    return visit([&](auto& hash_set_with_key) {
        size_t pool_bytes = pool != nullptr ? pool->total_reserved_bytes() : 0;
        return hash_set_with_key->hash_set.dump_bound() + pool_bytes;
    });
}

size_t AggHashSetVariant::allocated_memory_usage(const MemPool* pool) const {
    return visit([&](auto& hash_set_with_key) {
        return sizeof(typename decltype(hash_set_with_key->hash_set)::key_type) *
                       hash_set_with_key->hash_set.capacity() +
               pool->total_allocated_bytes();
    });
}

} // namespace starrocks
