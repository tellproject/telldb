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

namespace tell {
namespace db {

namespace {

boost::any deserialize(tell::store::FieldType type, const char* const data) {
    using namespace tell::store;
    switch (type) {
    case FieldType::NULLTYPE:
        return nullptr;
    case FieldType::SMALLINT:
        return *reinterpret_cast<const int16_t*>(data);
    case FieldType::INT:
        return *reinterpret_cast<const int32_t*>(data);
    case FieldType::BIGINT:
        return *reinterpret_cast<const int64_t*>(data);
    case FieldType::FLOAT:
        return *reinterpret_cast<const float*>(data);
    case FieldType::DOUBLE:
        return *reinterpret_cast<const double*>(data);
    case FieldType::TEXT:
    case FieldType::BLOB:
        {
            auto sz = *reinterpret_cast<const uint32_t*>(data);
            return crossbow::string(data + sizeof(uint32_t), sz);
        }
    case FieldType::NOTYPE:
        LOG_ASSERT(false, "One should never use a field of type NOTYPE");
        return false;
    }
}

} // namespace {}

Tuple::Tuple(
        const tell::store::Record& record,
        const tell::store::Tuple& tuple)
    : mRecord(record)
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

size_t Tuple::size() const {
}

} // namespace db
} // namespace tell

