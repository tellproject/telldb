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
#include <tellstore/StdTypes.hpp>

#include <crossbow/string.hpp>
#include <boost/any.hpp>
#include <cstdint>

namespace tell {
namespace db {

class Field {
    store::FieldType type;
    boost::any value;
    Field(store::FieldType t, boost::any v)
        : type(t)
        , value(v)
    {}
public:
    static Field create(int16_t value) {
        return Field(store::FieldType::SMALLINT, value);
    }
    static Field create(int32_t value) {
        return Field(store::FieldType::INT, value);
    }
    static Field create(int64_t value) {
        return Field(store::FieldType::BIGINT, value);
    }
    static Field create(float value) {
        return Field(store::FieldType::FLOAT, value);
    }
    static Field create(double value) {
        return Field(store::FieldType::DOUBLE, value);
    }
    static Field create(const crossbow::string& value) {
        return Field(store::FieldType::TEXT, value);
    }
    static Field createBlob(const crossbow::string& value) {
        return Field(store::FieldType::BLOB, value);
    }
    static Field createNull() {
        return Field(store::FieldType::NULLTYPE, nullptr);
    }

public:
    bool operator<(const Field& rhs) const;
    bool operator>(const Field& rhs) const;
    bool operator>=(const Field& rhs) const;
    bool operator<=(const Field& rhs) const;
    bool operator==(const Field& rhs) const;
public:
    Field cast(tell::store::FieldType type);
};

} // namespace db
} // namespace tell

