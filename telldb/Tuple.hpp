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
#include "Field.hpp"

#include <tellstore/AbstractTuple.hpp>
#include <tellstore/Record.hpp>

#include <memory>
#include <unordered_map>

namespace tell {
namespace store {
class Tuple;
} // namespace store
namespace db {

class Tuple : public tell::store::AbstractTuple {
public: // types
    using id_t = tell::store::Schema::id_t;
private: // members
    const tell::store::Record& mRecord;
    std::vector<Field> mFields;
public: // Construction
    Tuple(const tell::store::Record& record,
          const tell::store::Tuple& tuple);
public: // Access
    Field& operator[] (id_t id) {
        return mFields[id];
    }
    const Field& operator[] (id_t id) const {
        return mFields[id];
    }

    Field& operator[] (const crossbow::string& name);
    const Field& operator[] (const crossbow::string& name) const;

    Field& at(id_t id) {
        return mFields.at(id);
    }

    const Field& at(id_t id) const {
        return mFields.at(id);
    }

    Field& at(const crossbow::string& name);

    const Field& at(const crossbow::string& name) const;
public:
    size_t size() const override;
    void serialize(char* dest) const override;
};

} // namespace db
} // namespace tell
