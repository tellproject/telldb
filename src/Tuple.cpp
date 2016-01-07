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

Field deserialize(const char* data, tell::store::FieldType type, const char* field) {
    using namespace tell::store;
    switch (type) {
    case FieldType::NULLTYPE:
        return nullptr;
    case FieldType::SMALLINT:
        LOG_ASSERT(reinterpret_cast<uintptr_t>(field) % alignof(int16_t) == 0u, "Pointer to field must be aligned");
        return int16_t(*reinterpret_cast<const int16_t*>(field));
    case FieldType::INT:
        LOG_ASSERT(reinterpret_cast<uintptr_t>(field) % alignof(int32_t) == 0u, "Pointer to field must be aligned");
        return int32_t(*reinterpret_cast<const int32_t*>(field));
    case FieldType::BIGINT:
        LOG_ASSERT(reinterpret_cast<uintptr_t>(field) % alignof(int64_t) == 0u, "Pointer to field must be aligned");
        return int64_t(*reinterpret_cast<const int64_t*>(field));
    case FieldType::FLOAT:
        LOG_ASSERT(reinterpret_cast<uintptr_t>(field) % alignof(float) == 0u, "Pointer to field must be aligned");
        return float(*reinterpret_cast<const float*>(field));
    case FieldType::DOUBLE:
        LOG_ASSERT(reinterpret_cast<uintptr_t>(field) % alignof(double) == 0u, "Pointer to field must be aligned");
        return double(*reinterpret_cast<const double*>(field));
    case FieldType::TEXT:
    case FieldType::BLOB: {
        LOG_ASSERT(reinterpret_cast<uintptr_t>(field) % alignof(uint32_t) == 0u, "Pointer to field must be aligned");
        auto offsetData = reinterpret_cast<const uint32_t*>(field);
        auto offset = offsetData[0];
        auto length = offsetData[1] - offset;
        return crossbow::string(data + offset, length);
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
        auto field = record.data(tuple.data(), id_t(i), isNull, &type);
        if (isNull) {
            mFields.emplace_back(nullptr);
        } else {
            mFields.emplace_back(deserialize(tuple.data(), type, field));
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
    auto result = mRecord.staticSize();
    const auto& schema = mRecord.schema();
    for (decltype(mFields.size()) i = schema.fixedSizeFields().size(); i < mFields.size(); ++i) {
        if (mFields[i].type() != store::FieldType::NULLTYPE) {
            const auto& v = mFields[i].value<crossbow::string>();
            result += v.size();
        }
    }
    return crossbow::align(result, 8u);
}

void Tuple::serialize(char* dest) const {
    using namespace tell::store;

    const auto& schema = mRecord.schema();
    if (!schema.allNotNull()) {
        // set bitmap to null
        auto headerSize = mRecord.headerSize();
        ::memset(dest, 0, headerSize);
    }
    auto numFields = mRecord.fieldCount();
    auto varHeapOffset = mRecord.staticSize();
    for (decltype(numFields) i = 0; i < numFields; ++i) {
        auto& fieldMeta = mRecord.getFieldMeta(i);
        auto& field = fieldMeta.field;
        auto& value = mFields[i];
        auto current = dest + fieldMeta.offset;
        if (value.null()) {
            if (field.isNotNull()) {
                throw std::invalid_argument("Invalid null field");
            }
            mRecord.setFieldNull(dest, fieldMeta.nullIdx, true);
            if (field.isFixedSized()) {
                // Write a string of \0 bytes as a default value for fields if the field is null.
                memset(current, 0, field.staticSize());
            } else {
                // If this is a variable sized field we have to set the heap offset
                *reinterpret_cast<uint32_t*>(current) = varHeapOffset;
            }
        } else {
            if (value.type() != field.type()) {
                throw std::invalid_argument("Type does not match");
            }
            // we just need to copy the value to the correct offset.
            switch (value.type()) {
            case FieldType::NOTYPE: {
                throw std::invalid_argument("Can not serialize a notype");
            }
            case FieldType::NULLTYPE: {
                throw std::invalid_argument("Can not serialize a nulltype");
            }
            case FieldType::SMALLINT: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(int16_t) == 0u,
                        "Pointer to field must be aligned");
                *reinterpret_cast<int16_t*>(current) = value.value<int16_t>();
            } break;
            case FieldType::INT: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(int32_t) == 0u,
                        "Pointer to field must be aligned");
                *reinterpret_cast<int32_t*>(current) = value.value<int32_t>();
            } break;
            case FieldType::BIGINT: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(int64_t) == 0u,
                        "Pointer to field must be aligned");
                *reinterpret_cast<int64_t*>(current) = value.value<int64_t>();
            } break;
            case FieldType::FLOAT: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(float) == 0u,
                        "Pointer to field must be aligned");
                *reinterpret_cast<float*>(current) = value.value<float>();
            } break;
            case FieldType::DOUBLE: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(double) == 0u,
                        "Pointer to field must be aligned");
                *reinterpret_cast<double*>(current) = value.value<double>();
            } break;
            case FieldType::TEXT:
            case FieldType::BLOB: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(uint32_t) == 0u,
                        "Pointer to field must be aligned");
                *reinterpret_cast<uint32_t*>(current) = varHeapOffset;

                auto& data = value.value<crossbow::string>();
                memcpy(dest + varHeapOffset, data.c_str(), data.size());
                varHeapOffset += data.size();
            } break;

            default: {
                throw std::runtime_error("This should be unreachable code - something went horribly wrong!!");
            }
            }
        }
    }

    // Set the last offset to the end
    if (!schema.varSizeFields().empty()) {
        auto current = dest + mRecord.staticSize() - sizeof(uint32_t);
        *reinterpret_cast<uint32_t*>(current) = varHeapOffset;
    }
}

} // namespace db
} // namespace tell

