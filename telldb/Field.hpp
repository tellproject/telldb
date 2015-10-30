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

/**
 * @brief General data type for fields
 *
 * This is a dynamic type. It can be used to store any database type
 * and compare values with each other. This is the type returned for
 * queries and the type which should be constructed for updates.
 *
 * Fields are dynamically typed, but their type is still strong. That
 * means, that no implicit casting will occur on any time. Even comparison
 * between two types of integers will fail. To do these kind of
 * operations the user has to cast them to the correct type explicitely.
 * Field provides a function to cast values to other types.
 */
class Field {
    friend class Tuple;
    store::FieldType mType;
    boost::any mValue;
public:
    Field() : mType(store::FieldType::NULLTYPE), mValue(nullptr) {}
    Field(store::FieldType t, boost::any v)
        : mType(t)
        , mValue(v)
    {}
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
        return Field(store::FieldType::NULLTYPE, boost::any());
    }

public:
    bool operator<(const Field& rhs) const;
    bool operator>(const Field& rhs) const;
    bool operator>=(const Field& rhs) const;
    bool operator<=(const Field& rhs) const;
    bool operator==(const Field& rhs) const;
    Field operator+ (const Field& rhs) const;
    Field operator- (const Field& rhs) const;
    Field& operator+= (const Field& rhs);
    Field& operator-= (const Field& rhs);
public:
    /**
     * @brief Cast to another type
     *
     * Fields are strongly typed. To convert a value to another
     * type one can use this cast function. This function might
     * throw std::bad_cast if the cast fails.
     */
    Field cast(tell::store::FieldType type);
    /**
     * @brief Checks whether the field is NULL
     */
    bool null() const {
        return mType == store::FieldType::NULLTYPE;
    }
    /**
     * @brief Get the type of this field.
     */
    store::FieldType type() const {
        return mType;
    }
    /**
     * @brief Return the value of this field.
     */
    const boost::any& value() const {
        return mValue;
    }
private:
    size_t serialize(store::FieldType type, char* dest) const;
};

} // namespace db
} // namespace tell

