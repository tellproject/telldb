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
#pragma once

#include <telldb/Field.hpp>
#include <crossbow/serializer/Serializer.hpp>

namespace crossbow {

template<class Archiver>
struct size_policy<Archiver, tell::db::Field>
{
    size_t operator() (Archiver& ar, const tell::db::Field& field) const
    {
        size_t res = sizeof(tell::store::FieldType);
        switch (field.type()) {
        case tell::store::FieldType::NOTYPE:
            return res;
        case tell::store::FieldType::NULLTYPE:
            return res;
        case tell::store::FieldType::SMALLINT:
            return res + 2;
        case tell::store::FieldType::INT:
            return res + 4;
        case tell::store::FieldType::BIGINT:
            return res + 8;
        case tell::store::FieldType::FLOAT:
            return res + 4;
        case tell::store::FieldType::DOUBLE:
            return res + 8;
        case tell::store::FieldType::TEXT:
        case tell::store::FieldType::BLOB:
            return res + 4 + field.value<crossbow::string>().size();
        }
        assert(false);
        throw std::runtime_error("Unreachable code!");
    }
};

template<class Archiver>
struct serialize_policy<Archiver, tell::db::Field>
{
    uint8_t* operator() (Archiver& ar, const tell::db::Field& field, uint8_t* pos) const {
        ar & field.type();
        switch (field.type()) {
        case tell::store::FieldType::NOTYPE:
        case tell::store::FieldType::NULLTYPE:
            break;
        case tell::store::FieldType::SMALLINT:
            ar & field.value<int16_t>();
            break;
        case tell::store::FieldType::INT:
            ar & field.value<int32_t>();
            break;
        case tell::store::FieldType::BIGINT:
            ar & field.value<int64_t>();
            break;
        case tell::store::FieldType::FLOAT:
            ar & field.value<float>();
            break;
        case tell::store::FieldType::DOUBLE:
            ar & field.value<double>();
            break;
        case tell::store::FieldType::TEXT:
        case tell::store::FieldType::BLOB:
            ar & field.value<crossbow::string>();
            break;
        }
        return ar.pos;
    }
};

template<class Archiver>
struct deserialize_policy<Archiver, tell::db::Field>
{
    const uint8_t* operator() (Archiver& ar, tell::db::Field& field, const uint8_t* ptr) const {
        tell::store::FieldType type;
        ar & type;
        switch (type) {
        case tell::store::FieldType::NOTYPE:
            break;
        case tell::store::FieldType::NULLTYPE:
            field = tell::db::Field(nullptr);
            break;
        case tell::store::FieldType::SMALLINT:
            {
                int16_t value;
                ar & value;
                field = tell::db::Field(value);
            }
            break;
        case tell::store::FieldType::INT:
            {
                int32_t value;
                ar & value;
                field = tell::db::Field(value);
            }
            break;
        case tell::store::FieldType::BIGINT:
            {
                int64_t value;
                ar & value;
                field = tell::db::Field(value);
            }
            break;
        case tell::store::FieldType::FLOAT:
            {
                float value;
                ar & value;
                field = tell::db::Field(value);
            }
            break;
        case tell::store::FieldType::DOUBLE:
            {
                double value;
                ar & value;
                field = tell::db::Field(value);
            }
            break;
        case tell::store::FieldType::TEXT:
        case tell::store::FieldType::BLOB:
            {
                crossbow::string value;
                ar & value;
                field = tell::db::Field(value);
            }
            break;
        }
        return ar.pos;
    }
};

} // namespace crossbow
