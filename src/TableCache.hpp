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
#include <telldb/Transaction.hpp>

namespace tell {
namespace store {
class Table;
class Tuple;
} // namespace store
namespace db {
namespace impl {

struct TellDBContext;

} // namespace impl

class TableCache {
    using id_t = tell::store::Schema::id_t;
    friend class Future<Tuple>;
    const tell::store::Table& mTable;
    tell::store::ClientTransaction& mTransaction;
    std::unordered_map<key_t, std::pair<Tuple*, bool>> mCache;
    std::unordered_map<key_t, Tuple*> mChanges;
    std::unordered_map<crossbow::string, id_t> mSchema;
public:
    TableCache(const tell::store::Table& table, impl::TellDBContext& context, tell::store::ClientTransaction& transaction);
    Future<Tuple> get(key_t key);
private:
    const Tuple& addTuple(key_t key, const tell::store::Tuple& tuple);
};

} // namespace db
} // namespace tell

