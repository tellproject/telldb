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
#include <telldb/Exceptions.hpp>
#include <boost/lexical_cast.hpp>
#include <sstream>

namespace tell {
namespace db {

Exception::~Exception() {}
KeyException::~KeyException() {}
OpenTableException::~OpenTableException() {}
TupleExistsException::~TupleExistsException() {}
TupleDoesNotExist::~TupleDoesNotExist() {}
Conflict::~Conflict() {}

// KeyException
key_t KeyException::key() const noexcept {
    return mKey;
}

const char* KeyException::what() const noexcept {
    return mMsg.c_str();
}

// TupleExistsException
TupleExistsException::TupleExistsException(key_t key)
    : KeyException(key, "Key " + boost::lexical_cast<crossbow::string>(key) + " already exists")
{}


// TupleDoesNotExist
TupleDoesNotExist::TupleDoesNotExist(key_t key)
    : KeyException(key, "Key " + boost::lexical_cast<crossbow::string>(key) + " does not exists or got deleted")
{}

// Conflict
Conflict::Conflict(key_t key)
    : KeyException(key, "Conflict on " + boost::lexical_cast<crossbow::string>(key))
{}

IndexConflict::IndexConflict(key_t key, const crossbow::string& idxName)
    : KeyException(key, "Index error on " + idxName)
{}

void Conflicts::init() {
    std::stringstream ss;
    ss << "Conflicts on the following keys:";
    for (auto k : mKeys) {
        ss << std::endl << k.value;
    }
    mMsg = ss.str();
}

const std::vector<key_t>& Conflicts::keys() const noexcept {
    return mKeys;
}

const char* Conflicts::what() const noexcept {
    return mMsg.c_str();
}

} // namespace db
} // namespace tell

