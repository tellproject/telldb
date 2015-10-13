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
#include <bdtree/logical_table_cache.h>
#include <crossbow/ChunkAllocator.hpp>

#include "ChunkUnorderedMap.hpp"
#include "Indexes.hpp"

namespace tell {
namespace store {
class Table;
class Tuple;
} // namespace store
namespace db {
namespace impl {

struct TellDBContext;

} // namespace impl

class Tuple;

class TableCache : public crossbow::ChunkObject {
public: // public types
    enum class Operation {
        Insert, Update, Delete
    };
    using ChangesMap = ChunkUnorderedMap<key_t, std::pair<Tuple*, Operation>>;
private: // private types
    using id_t = tell::store::Schema::id_t;
    friend class Future<Tuple>;
private: // members
    const tell::store::Table& mTable;
    tell::store::ClientTransaction& mTransaction;
    crossbow::ChunkMemoryPool& mPool;
    ChunkUnorderedMap<key_t, std::pair<Tuple*, bool>> mCache;
    ChangesMap mChanges;
    ChunkUnorderedMap<crossbow::string, id_t> mSchema;
public: // Construction and Destruction
    TableCache(const tell::store::Table& table,
            impl::TellDBContext& context,
            tell::store::ClientTransaction& transaction,
            crossbow::ChunkMemoryPool& pool);
    ~TableCache();
public: // operations
    Future<Tuple> get(key_t key);
    void insert(key_t key, const Tuple& tuple);
    void update(key_t key, const Tuple& tuple);
    void remove(key_t key);
    void writeIndexes();
public: // state access
    const ChangesMap& changes() const {
        return mChanges;
    }
    const store::Table& table() const {
        return mTable;
    }
private:
    const Tuple& addTuple(key_t key, const tell::store::Tuple& tuple);
};

} // namespace db
} // namespace tell

