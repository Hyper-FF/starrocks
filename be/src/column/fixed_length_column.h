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

#include "column/fixed_length_column_base.h"
namespace starrocks {
template <typename T>
class FixedLengthColumn final : public ColumnCRTPBase<FixedLengthColumn<T>, FixedLengthColumnBase<T>> {
    friend class ColumnCRTPBase<FixedLengthColumn<T>, FixedLengthColumnBase<T>>;

public:
    using ValueType = T;
    using Container = Buffer<ValueType>;
    using SuperClass = ColumnCRTPBase<FixedLengthColumn<T>, FixedLengthColumnBase<T>>;
    FixedLengthColumn() = default;

    explicit FixedLengthColumn(const size_t n) : SuperClass(n) {}

    FixedLengthColumn(const size_t n, const ValueType x) : SuperClass(n, x) {}

    DISALLOW_COPY_TEMPLATE(FixedLengthColumn, FixedLengthColumn<T>);

    MutableColumnPtr clone_empty() const override { return this->create(); }

    MutableColumnPtr clone() const override {
        auto p = clone_empty();
        p->append(*this, 0, this->size());
        return p;
    }
};
} // namespace starrocks
