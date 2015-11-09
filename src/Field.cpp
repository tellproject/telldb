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

#include <crossbow/logger.hpp>

#include <boost/lexical_cast.hpp>
#include <stdexcept>


using namespace tell::store;

namespace tell {
namespace db {

bool Field::operator< (const Field& rhs) const {
    if (mType != rhs.mType) {
        throw std::invalid_argument("Can only compare fields of same type");
    }
    switch (mType) {
    case FieldType::NULLTYPE:
        return false;
    case FieldType::NOTYPE:
        throw std::invalid_argument("Can not compare fields without types");
    case FieldType::SMALLINT:
        return boost::any_cast<int16_t>(mValue) < boost::any_cast<int16_t>(rhs.mValue);
    case FieldType::INT:
        return boost::any_cast<int32_t>(mValue) < boost::any_cast<int32_t>(rhs.mValue);
    case FieldType::BIGINT:
        return boost::any_cast<int64_t>(mValue) < boost::any_cast<int64_t>(rhs.mValue);
    case FieldType::FLOAT:
        return boost::any_cast<float>(mValue) < boost::any_cast<float>(rhs.mValue);
    case FieldType::DOUBLE:
        return boost::any_cast<double>(mValue) < boost::any_cast<double>(rhs.mValue);
    case FieldType::TEXT:
        {
            const crossbow::string& l = boost::any_cast<const crossbow::string&>(mValue);
            const crossbow::string& r = boost::any_cast<const crossbow::string&>(rhs.mValue);
            return l < r;
        }
    case FieldType::BLOB:
        throw std::invalid_argument("Can not compare BLOBs");
    }
    assert(false);
    throw std::runtime_error("This should be unreachable code - something went horribly wrong!!");
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

Field& Field::operator+= (const Field& rhs) {
    if (mType != rhs.type()) {
        throw std::invalid_argument("Can only add Fields of same type");
    }
    switch (mType) {
    case FieldType::NULLTYPE:
        return *this;
    case FieldType::NOTYPE:
        throw std::invalid_argument("Can not compare fields without types");
    case FieldType::SMALLINT:
        boost::any_cast<int16_t&>(mValue) += boost::any_cast<int16_t>(rhs.mValue);
        return *this;
    case FieldType::INT:
        boost::any_cast<int32_t&>(mValue) += boost::any_cast<int32_t>(rhs.mValue);
        return *this;
    case FieldType::BIGINT:
        boost::any_cast<int64_t&>(mValue) += boost::any_cast<int64_t>(rhs.mValue);
        return *this;
    case FieldType::FLOAT:
        boost::any_cast<float&>(mValue) += boost::any_cast<float>(rhs.mValue);
        return *this;
    case FieldType::DOUBLE:
        boost::any_cast<double&>(mValue) += boost::any_cast<double>(rhs.mValue);
        return *this;
    case FieldType::TEXT:
    case FieldType::BLOB:
        throw std::invalid_argument("Can not calc minus on TEXT or BLOB");
    }
    assert(false);
    throw std::runtime_error("This should be unreachable code - something went horribly wrong!!");
}

Field& Field::operator-= (const Field& rhs) {
    if (mType != rhs.type()) {
        throw std::invalid_argument("Can only add Fields of same type");
    }
    switch (mType) {
    case FieldType::NULLTYPE:
        return *this;
    case FieldType::NOTYPE:
        throw std::invalid_argument("Can not compare fields without types");
    case FieldType::SMALLINT:
        boost::any_cast<int16_t&>(mValue) -= boost::any_cast<int16_t>(rhs.mValue);
        return *this;
    case FieldType::INT:
        boost::any_cast<int32_t&>(mValue) -= boost::any_cast<int32_t>(rhs.mValue);
        return *this;
    case FieldType::BIGINT:
        boost::any_cast<int64_t&>(mValue) -= boost::any_cast<int64_t>(rhs.mValue);
        return *this;
    case FieldType::FLOAT:
        boost::any_cast<float&>(mValue) -= boost::any_cast<float>(rhs.mValue);
        return *this;
    case FieldType::DOUBLE:
        boost::any_cast<double&>(mValue) -= boost::any_cast<double>(rhs.mValue);
        return *this;
    case FieldType::TEXT:
    case FieldType::BLOB:
        throw std::invalid_argument("Can not calc minus on TEXT or BLOB");
    }
    assert(false);
    throw std::runtime_error("This should be unreachable code - something went horribly wrong!!");
}

bool Field::operator<=(const boost::any rhs)
{
    switch (mType) {
    case FieldType::NULLTYPE:
    case FieldType::NOTYPE:
        throw std::invalid_argument("Cannot compare nullptr or no type!");
    case FieldType::SMALLINT:
        return boost::any_cast<int16_t&>(mValue) <= boost::any_cast<int16_t>(rhs);
    case FieldType::INT:
        return boost::any_cast<int32_t&>(mValue) <= boost::any_cast<int32_t>(rhs);
    case FieldType::BIGINT:
        return boost::any_cast<int64_t&>(mValue) <= boost::any_cast<int64_t>(rhs);
    case FieldType::FLOAT:
        return boost::any_cast<float>(mValue) <= boost::any_cast<float>(rhs);
    case FieldType::DOUBLE:
        return boost::any_cast<double>(mValue) <= boost::any_cast<double>(rhs);
    case FieldType::TEXT:
    case FieldType::BLOB:
        throw std::invalid_argument("Can not compare on TEXT or BLOB");
    }
    assert(false);
    throw std::runtime_error("This should be unreachable code - something went horribly wrong!!");
}

bool Field::operator>=(const boost::any rhs)
{
    switch (mType) {
    case FieldType::NULLTYPE:
    case FieldType::NOTYPE:
        throw std::invalid_argument("Cannot compare nullptr or no type!");
    case FieldType::SMALLINT:
        return boost::any_cast<int16_t&>(mValue) >= boost::any_cast<int16_t>(rhs);
    case FieldType::INT:
        return boost::any_cast<int32_t&>(mValue) >= boost::any_cast<int32_t>(rhs);
    case FieldType::BIGINT:
        return boost::any_cast<int64_t&>(mValue) >= boost::any_cast<int64_t>(rhs);
    case FieldType::FLOAT:
        return boost::any_cast<float>(mValue) >= boost::any_cast<float>(rhs);
    case FieldType::DOUBLE:
        return boost::any_cast<double>(mValue) >= boost::any_cast<double>(rhs);
    case FieldType::TEXT:
    case FieldType::BLOB:
        throw std::invalid_argument("Can not compare on TEXT or BLOB");
    }
    assert(false);
    throw std::runtime_error("This should be unreachable code - something went horribly wrong!!");
}

Field Field::operator+(const Field& rhs) const {
    Field res = *this;
    res += rhs;
    return res;
}

Field Field::operator-(const Field& rhs) const {
    Field res = *this;
    res -= rhs;
    return res;
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
    throw std::runtime_error("This should be unreachable code - something went horribly wrong!!");
}

Field Field::cast(tell::store::FieldType t) {
    if (t == mType) return *this;
    switch (mType) {
    case FieldType::NULLTYPE:
    case FieldType::NOTYPE:
    case FieldType::BLOB:
        throw std::bad_cast();
    case FieldType::SMALLINT:
        return Field(t, castTo(boost::any_cast<int16_t>(mValue), t));
    case FieldType::INT:
        return Field(t, castTo(boost::any_cast<int32_t>(mValue), t));
    case FieldType::BIGINT:
        return Field(t, castTo(boost::any_cast<int64_t>(mValue), t));
    case FieldType::FLOAT:
        return Field(t, castTo(boost::any_cast<float>(mValue), t));
    case FieldType::DOUBLE:
        return Field(t, castTo(boost::any_cast<double>(mValue), t));
    case FieldType::TEXT:
        return Field(t, castTo(boost::any_cast<crossbow::string>(mValue), t));
    }
    throw std::runtime_error("This should be unreachable code - something went horribly wrong!!");
}

size_t Field::serialize(char* dest) const {
    switch (mType) {
    case FieldType::NULLTYPE:
        throw std::invalid_argument("Can not serialize a nulltype");
    case FieldType::NOTYPE:
        throw std::invalid_argument("Can not serialize a notype");
    case FieldType::BLOB:
    case FieldType::TEXT:
        {
            const auto& v = boost::any_cast<const crossbow::string&>(mValue);
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % 4u == 0u,
                    "Pointer to variable sized field must be 4 byte aligned");
            int32_t sz = v.size();
            memcpy(dest, &sz, sizeof(sz));
            memcpy(dest + sizeof(sz), v.data(), v.size());
            auto res = sizeof(sz) + v.size();
            return res;
        }
    case FieldType::SMALLINT:
        {
            int16_t v = boost::any_cast<int16_t>(mValue);
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % alignof(int16_t) == 0u, "Pointer to field must be aligned");
            memcpy(dest, &v, sizeof(v));
            return sizeof(v);
        }
    case FieldType::INT:
        {
            int32_t v = boost::any_cast<int32_t>(mValue);
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % alignof(int32_t) == 0u, "Pointer to field must be aligned");
            memcpy(dest, &v, sizeof(v));
            return sizeof(v);
        }
    case FieldType::BIGINT:
        {
            int64_t v = boost::any_cast<int64_t>(mValue);
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % alignof(int64_t) == 0u, "Pointer to field must be aligned");
            memcpy(dest, &v, sizeof(v));
            return sizeof(v);
        }
    case FieldType::FLOAT:
        {
            float v = boost::any_cast<float>(mValue);
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % alignof(float) == 0u, "Pointer to field must be aligned");
            memcpy(dest, &v, sizeof(v));
            return sizeof(v);
        }
    case FieldType::DOUBLE:
        {
            double v = boost::any_cast<double>(mValue);
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % alignof(double) == 0u, "Pointer to field must be aligned");
            memcpy(dest, &v, sizeof(v));
            return sizeof(v);
        }
    }
    throw std::runtime_error("This should be unreachable code - something went horribly wrong!!");
}

} // namespace db
} // namespace tell

