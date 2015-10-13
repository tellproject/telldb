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
#include "Types.hpp"
#include "Field.hpp"

#include <exception>
#include <crossbow/string.hpp>

namespace tell {
namespace db {

class Exception : public std::exception {
public:
    ~Exception();
};

class KeyException : public Exception {
    key_t mKey;
    crossbow::string mMsg;
protected:
    template<class Str>
    KeyException(key_t key, Str&& msg)
        : mKey(key)
        , mMsg(std::forward<Str>(msg))
    {}
    ~KeyException();
public:
    key_t key() const noexcept;
    const char* what() const noexcept override;
};

class OpenTableException : public Exception {
    crossbow::string mMsg;
public:
    template<class Str>
    OpenTableException(Str&& msg)
        : mMsg(std::forward<Str>(msg))
    {}
    OpenTableException(const OpenTableException&) = default;
    OpenTableException(OpenTableException&&) = default;
    ~OpenTableException();
    const char* what() const noexcept override { return mMsg.c_str(); }
};

class TupleExistsException : public KeyException {
public:
    TupleExistsException(key_t key);
    ~TupleExistsException();
};

class TupleDoesNotExist : public KeyException {
public:
    TupleDoesNotExist(key_t key);
    ~TupleDoesNotExist();
};

class Conflict : public KeyException {
public:
    Conflict(key_t key);
    ~Conflict();
};

class UniqueViolation : public Exception {
    Field mField;
public:
    UniqueViolation(const Field& field);
    ~UniqueViolation();
    const char* what() const noexcept override;
    const Field& field() const {
        return mField;
    }
};

} // namespace db
} // namespace tell

