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
#include <telldb/Field.hpp>

#include <boost/lexical_cast.hpp>
#include <stdexcept>


using namespace tell::store;

namespace tell {
namespace db {


bool Field::operator< (const Field& rhs) const {
    if (type != rhs.type) {
        throw std::invalid_argument("Can only compare fields of same type");
    }
    switch (type) {
    case FieldType::NULLTYPE:
        return false;
    case FieldType::NOTYPE:
        throw std::invalid_argument("Can not compare fields without types");
    case FieldType::SMALLINT:
        return boost::any_cast<int16_t>(value) < boost::any_cast<int16_t>(rhs.value);
    case FieldType::INT:
        return boost::any_cast<int32_t>(value) < boost::any_cast<int32_t>(rhs.value);
    case FieldType::BIGINT:
        return boost::any_cast<int64_t>(value) < boost::any_cast<int64_t>(rhs.value);
    case FieldType::FLOAT:
        return boost::any_cast<float>(value) < boost::any_cast<float>(rhs.value);
    case FieldType::DOUBLE:
        return boost::any_cast<double>(value) < boost::any_cast<double>(rhs.value);
    case FieldType::TEXT:
        {
            const crossbow::string& l = *boost::any_cast<const crossbow::string*>(value);
            const crossbow::string& r = *boost::any_cast<const crossbow::string*>(rhs.value);
            return l < r;
        }
    case FieldType::BLOB:
        throw std::invalid_argument("Can not compare BLOBs");
    }
}

bool Field::operator<=(const Field& rhs) const {
    return *this < rhs || !(rhs < *this);
}

bool Field::operator>(const Field& rhs) const {
    return rhs < *this;
}

bool Field::operator>=(const Field& rhs) const {
    return rhs < *this || !(*this < rhs);
}

bool Field::operator==(const Field& rhs) const {
    return !(*this < rhs) && !(rhs < *this);
}

template<class T>
boost::any castTo(const T& value, FieldType target) {
    switch (target) {
    case FieldType::NULLTYPE:
    case FieldType::NOTYPE:
    case FieldType::BLOB:
        throw std::bad_cast();
    case FieldType::SMALLINT:
        return boost::lexical_cast<int16_t>(value);
    case FieldType::INT:
        return boost::lexical_cast<int32_t>(value);
    case FieldType::BIGINT:
        return boost::lexical_cast<int64_t>(value);
    case FieldType::FLOAT:
        return boost::lexical_cast<float>(value);
    case FieldType::DOUBLE:
        return boost::lexical_cast<double>(value);
    case FieldType::TEXT:
        return boost::lexical_cast<crossbow::string>(value);
    }
}

Field Field::cast(tell::store::FieldType t) {
    if (t == type) return *this;
    switch (type) {
    case FieldType::NULLTYPE:
    case FieldType::NOTYPE:
    case FieldType::BLOB:
        throw std::bad_cast();
    case FieldType::SMALLINT:
        return Field(t, castTo(boost::any_cast<int16_t>(value), t));
    case FieldType::INT:
        return Field(t, castTo(boost::any_cast<int32_t>(value), t));
    case FieldType::BIGINT:
        return Field(t, castTo(boost::any_cast<int64_t>(value), t));
    case FieldType::FLOAT:
        return Field(t, castTo(boost::any_cast<float>(value), t));
    case FieldType::DOUBLE:
        return Field(t, castTo(boost::any_cast<double>(value), t));
    case FieldType::TEXT:
        return Field(t, castTo(boost::any_cast<crossbow::string>(value), t));
    }
}

} // namespace db
} // namespace tell

