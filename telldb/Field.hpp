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
    union {
        crossbow::string str;
        int16_t smallint;
        int32_t normalint;
        int64_t bigint;
        float floatNr;
        double doubleNr;
    };
public:
    Field() : mType(store::FieldType::NULLTYPE) {}
    Field(int16_t value)
        : mType(store::FieldType::SMALLINT)
        , smallint(value)
    {}
    Field(int32_t value)
        : mType(store::FieldType::INT)
        , normalint(value)
    {}
    Field(int64_t value)
        : mType(store::FieldType::BIGINT)
        , bigint(value)
    {}
    Field(float value)
        : mType(store::FieldType::FLOAT)
        , floatNr(value)
    {}
    Field(double value)
        : mType(store::FieldType::DOUBLE)
        , doubleNr(value)
    {}
    Field(const crossbow::string& value)
        : mType(store::FieldType::TEXT)
    {
        new (&str) crossbow::string(value);
    }
    Field(std::nullptr_t)
        : mType(store::FieldType::NULLTYPE)
    {}
    Field(const Field& other)
        : mType(other.mType)
    {
        switch (mType) {
        case store::FieldType::NULLTYPE:
        case store::FieldType::NOTYPE:
            return;
        case store::FieldType::TEXT:
        case store::FieldType::BLOB:
            new (&str) crossbow::string(other.str);
            return;
        case store::FieldType::SMALLINT:
            smallint = other.smallint;
            return;
        case store::FieldType::INT:
            normalint = other.normalint;
            return;
        case store::FieldType::BIGINT:
            bigint = other.bigint;
            return;
        case store::FieldType::FLOAT:
            floatNr = other.floatNr;
            return;
        case store::FieldType::DOUBLE:
            doubleNr = other.doubleNr;
        }
    }
    ~Field() {
        if (mType == store::FieldType::TEXT || mType == store::FieldType::BLOB) {
            str.~basic_string();
        }
    }
    
    Field& operator= (const Field& other) {
        if (mType == tell::store::FieldType::TEXT || mType == tell::store::FieldType::BLOB) {
            if (other.mType == mType) {
                str = other.str;
                return *this;
            }
            str.~basic_string();
        }
        mType = other.mType;
        switch (mType) {
        case store::FieldType::NULLTYPE:
        case store::FieldType::NOTYPE:
            break;
        case store::FieldType::TEXT:
        case store::FieldType::BLOB:
            new (&str) crossbow::string(other.str);
            break;
        case store::FieldType::SMALLINT:
            smallint = other.smallint;
            break;
        case store::FieldType::INT:
            normalint = other.normalint;
            break;
        case store::FieldType::BIGINT:
            bigint = other.bigint;
            break;
        case store::FieldType::FLOAT:
            floatNr = other.floatNr;
            break;
        case store::FieldType::DOUBLE:
            doubleNr = other.doubleNr;
        }
        return *this;
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
    template<class T>
    typename std::enable_if<std::is_same<T, int16_t>::value, int16_t&>::type
    value() {
        return smallint;
    }
    template<class T>
    typename std::enable_if<std::is_same<T, int32_t>::value, int32_t&>::type
    value() {
        return normalint;
    }
    template<class T>
    typename std::enable_if<std::is_same<T, int64_t>::value, int64_t&>::type
    value() {
        return bigint;
    }
    template<class T>
    typename std::enable_if<std::is_same<T, float>::value, float&>::type
    value() {
        return floatNr;
    }
    template<class T>
    typename std::enable_if<std::is_same<T, double>::value, double&>::type
    value() {
        return doubleNr;
    }
    template<class T>
    typename std::enable_if<std::is_same<T, crossbow::string>::value, crossbow::string&>::type
    value() {
        return str;
    }
    template<class T>
    const T& value() const {
        return const_cast<Field*>(this)->value<T>();
    }
};

} // namespace db
} // namespace tell

