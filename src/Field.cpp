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
        return smallint < rhs.smallint;
    case FieldType::INT:
        return normalint < rhs.normalint;
    case FieldType::BIGINT:
        return bigint < rhs.bigint;
    case FieldType::FLOAT:
        return floatNr < rhs.floatNr;
    case FieldType::DOUBLE:
        return doubleNr < rhs.doubleNr;
    case FieldType::TEXT:
        return str < rhs.str;
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
        smallint += rhs.smallint;
        return *this;
    case FieldType::INT:
        normalint += rhs.normalint;
        return *this;
    case FieldType::BIGINT:
        bigint += rhs.bigint;
        return *this;
    case FieldType::FLOAT:
        floatNr += rhs.floatNr;
        return *this;
    case FieldType::DOUBLE:
        doubleNr += rhs.doubleNr;
        return *this;
    case FieldType::TEXT:
        str += rhs.str;
        return *this;
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
        smallint -= rhs.smallint;
        return *this;
    case FieldType::INT:
        normalint -= rhs.normalint;
        return *this;
    case FieldType::BIGINT:
        bigint -= rhs.normalint;
        return *this;
    case FieldType::FLOAT:
        floatNr -= rhs.floatNr;
        return *this;
    case FieldType::DOUBLE:
        doubleNr -= rhs.doubleNr;
        return *this;
    case FieldType::TEXT:
    case FieldType::BLOB:
        throw std::invalid_argument("Can not calc minus on TEXT or BLOB");
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

size_t Field::serialize(char* dest) const {
    switch (mType) {
    case FieldType::NULLTYPE:
        throw std::invalid_argument("Can not serialize a nulltype");
    case FieldType::NOTYPE:
        throw std::invalid_argument("Can not serialize a notype");
    case FieldType::BLOB:
    case FieldType::TEXT:
        {
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % 4u == 0u,
                    "Pointer to variable sized field must be 4 byte aligned");
            int32_t sz = str.size();
            memcpy(dest, &sz, sizeof(sz));
            memcpy(dest + sizeof(sz), str.data(), str.size());
            auto res = sizeof(sz) + str.size();
            return res;
        }
    case FieldType::SMALLINT:
        {
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % alignof(int16_t) == 0u, "Pointer to field must be aligned");
            memcpy(dest, &smallint, sizeof(smallint));
            return sizeof(smallint);
        }
    case FieldType::INT:
        {
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % alignof(int32_t) == 0u, "Pointer to field must be aligned");
            memcpy(dest, &normalint, sizeof(normalint));
            return sizeof(normalint);
        }
    case FieldType::BIGINT:
        {
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % alignof(int64_t) == 0u, "Pointer to field must be aligned");
            memcpy(dest, &bigint, sizeof(bigint));
            return sizeof(bigint);
        }
    case FieldType::FLOAT:
        {
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % alignof(float) == 0u, "Pointer to field must be aligned");
            memcpy(dest, &floatNr, sizeof(floatNr));
            return sizeof(floatNr);
        }
    case FieldType::DOUBLE:
        {
            LOG_ASSERT(reinterpret_cast<uintptr_t>(dest) % alignof(double) == 0u, "Pointer to field must be aligned");
            memcpy(dest, &doubleNr, sizeof(doubleNr));
            return sizeof(doubleNr);
        }
    }
    throw std::runtime_error("This should be unreachable code - something went horribly wrong!!");
}

} // namespace db
} // namespace tell

