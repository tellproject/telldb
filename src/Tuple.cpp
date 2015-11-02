/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */

#include <telldb/Tuple.hpp>
#include <tellstore/Table.hpp>

#include <crossbow/alignment.hpp>

#include <memory.h>

namespace tell {
namespace db {

namespace {

boost::any deserialize(tell::store::FieldType type, const char* const data) {
    using namespace tell::store;
    switch (type) {
    case FieldType::NULLTYPE:
        return nullptr;
    case FieldType::SMALLINT:
        LOG_ASSERT(reinterpret_cast<uintptr_t>(data) % alignof(int16_t) == 0u, "Pointer to field must be aligned");
        return *reinterpret_cast<const int16_t*>(data);
    case FieldType::INT:
        LOG_ASSERT(reinterpret_cast<uintptr_t>(data) % alignof(int32_t) == 0u, "Pointer to field must be aligned");
        return *reinterpret_cast<const int32_t*>(data);
    case FieldType::BIGINT:
        LOG_ASSERT(reinterpret_cast<uintptr_t>(data) % alignof(int64_t) == 0u, "Pointer to field must be aligned");
        return *reinterpret_cast<const int64_t*>(data);
    case FieldType::FLOAT:
        LOG_ASSERT(reinterpret_cast<uintptr_t>(data) % alignof(float) == 0u, "Pointer to field must be aligned");
        return *reinterpret_cast<const float*>(data);
    case FieldType::DOUBLE:
        LOG_ASSERT(reinterpret_cast<uintptr_t>(data) % alignof(double) == 0u, "Pointer to field must be aligned");
        return *reinterpret_cast<const double*>(data);
    case FieldType::TEXT:
    case FieldType::BLOB:
        {
            LOG_ASSERT(reinterpret_cast<uintptr_t>(data) % alignof(uint32_t) == 0u, "Pointer to field must be aligned");
            auto sz = *reinterpret_cast<const uint32_t*>(data);
            return crossbow::string(data + sizeof(uint32_t), sz);
        }
    case FieldType::NOTYPE:
        LOG_ASSERT(false, "One should never use a field of type NOTYPE");
        return false;
    }
    LOG_ASSERT(false, "Unreachable code");
    return false;
}

} // namespace {}

Tuple::Tuple(
        const tell::store::Record& record,
        const tell::store::Tuple& tuple,
        crossbow::ChunkMemoryPool& pool)
    : mRecord(record)
    , mPool(pool)
    , mFields(&mPool)
{
    int numFields = record.fieldCount();
    for (int i = 0; i < numFields; ++i) {
        bool isNull = false;
        tell::store::FieldType type;
        auto data = record.data(tuple.data(), id_t(i), isNull, &type);
        if (isNull) {
            mFields.emplace_back(tell::store::FieldType::NULLTYPE, nullptr);
        } else {
            mFields.emplace_back(type, deserialize(type, data));
        }
    }
}

Tuple::Tuple(const store::Record& record, crossbow::ChunkMemoryPool& pool)
    : mRecord(record)
    , mPool(pool)
    , mFields(&mPool)
{
    int numFields = record.fieldCount();
    mFields.resize(numFields);
}

size_t Tuple::size() const {
    auto result = mRecord.fixedSize();
    const auto& schema = mRecord.schema();
    for (decltype(mFields.size()) i = schema.fixedSizeFields().size(); i < mFields.size(); ++i) {
        result = crossbow::align(result, 4u);
        result += sizeof(uint32_t);
        if (mFields[i].type() != store::FieldType::NULLTYPE) {
            const auto& v = boost::any_cast<const crossbow::string&>(mFields[i].value());
            result += v.size();
        }
    }
    return crossbow::align(result, 8u);
}

void Tuple::serialize(char* dest) const {
    const auto& schema = mRecord.schema();
    if (!mRecord.schema().allNotNull()) {
        // set bitmap to null
        auto headerSize = mRecord.headerSize();
        ::memset(dest, 0, headerSize);
        dest += headerSize;
    }
    const auto& fixedSize = schema.fixedSizeFields();
    auto numFixedSize = fixedSize.size();
    auto numFields = mRecord.fieldCount();
    const auto& varSizedFields = schema.varSizeFields();
    for (decltype(numFields) i = 0; i < numFields; ++i) {
        auto isFixedSize = (i < numFixedSize);
        if (!isFixedSize) {
            dest = crossbow::align(dest, 4u);
        }
        if (mFields[i].null()) {
            mRecord.setFieldNull(dest, i, true);
            store::FieldType type;
            if (isFixedSize) {
                type = fixedSize[i].type();
            } else {
                type = varSizedFields[i - numFixedSize].type();
            }
            store::FieldBase f(type);
            auto defaultSize = f.defaultSize();
            memset(dest, 0, defaultSize);
            dest += defaultSize;
        } else {
            dest += mFields[i].serialize(dest);
        }
    }
}

} // namespace db
} // namespace tell

