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
#include <vector>
#include <crossbow/string.hpp>

namespace tell {
namespace db {

class Exception : public std::exception {
public:
    virtual ~Exception();
};

class FieldDoesNotExist : public Exception {
    crossbow::string mMsg;
public:
    FieldDoesNotExist(const crossbow::string& fieldName);
    virtual ~FieldDoesNotExist();
    const char* what() const noexcept override ;
 };

class FieldNotSet : public Exception {
    crossbow::string mMsg;
public:
    FieldNotSet(const crossbow::string& fieldName);
    virtual ~FieldNotSet();
    const char* what() const noexcept override ;
};

class WrongFieldType : public Exception {
    crossbow::string mMsg;
public:
    WrongFieldType(const crossbow::string& fieldName);
    virtual ~WrongFieldType();
    const char* what() const noexcept override ;
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

class Conflicts : public Exception {
    void init();
    crossbow::string mMsg;
    std::vector<key_t> mKeys;
public:
    template<class V>
    Conflicts(V&& keys)
        : mKeys(std::forward<V>(keys))
    {
        init();
    }
    const std::vector<key_t>& keys() const noexcept;
    const char* what() const noexcept override;
};

class IndexConflict : public KeyException {
    crossbow::string idxName;
public:
    IndexConflict(key_t key, const crossbow::string& idxName);
    ~IndexConflict();
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

