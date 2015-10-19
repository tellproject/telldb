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
#include <telldb/Types.hpp>
#include <telldb/Transaction.hpp>
#include <tellstore/Table.hpp>
#include <crossbow/string.hpp>
#include <crossbow/ChunkAllocator.hpp>

#include "ChunkUnorderedMap.hpp"
#include "Indexes.hpp"

namespace tell {
namespace db {
namespace impl {
struct TellDBContext;
} // namespace impl

class TableCache;

class TransactionCache : public crossbow::ChunkObject {
    friend class Future<table_t>;
    impl::TellDBContext& context;
    store::ClientHandle& mHandle;
    store::ClientTransaction& mTransaction;
    crossbow::ChunkMemoryPool& mPool;
    ChunkUnorderedMap<table_t, TableCache*> mTables;
public:
    TransactionCache(impl::TellDBContext& context,
            store::ClientHandle& handle,
            store::ClientTransaction& transaction,
            crossbow::ChunkMemoryPool& pool);
    ~TransactionCache();
public: // Schema operations
    Future<table_t> openTable(const crossbow::string& name);
    table_t createTable(const crossbow::string& name, const store::Schema& schema);
public: // Get/Put
    Future<Tuple> get(table_t table, key_t key);
    Iterator lower_bound(table_t tableId, const crossbow::string& idxName, const KeyType& key);
    Iterator reverse_lower_bound(table_t tableId, const crossbow::string& idxName, const KeyType& key);
    void insert(table_t table, key_t key, const Tuple& tuple);
    void update(table_t table, key_t key, const Tuple& from, const Tuple& to);
    void remove(table_t table, key_t key, const Tuple& tuple);
public:
    crossbow::basic_string<char, std::char_traits<char>, crossbow::ChunkAllocator<char>> undoLog() const;
    void writeBack();
    void writeIndexes();
private:
    table_t addTable(const tell::store::Table& table, std::unordered_map<crossbow::string, impl::IndexWrapper>&& indexes);
    table_t addTable(const crossbow::string& name, const tell::store::Table& table);
};

} // namespace db
} // namespace tell

