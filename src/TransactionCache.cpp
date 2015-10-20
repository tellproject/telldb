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
#include "TransactionCache.hpp"
#include "TableCache.hpp"
#include "Indexes.hpp"
#include "FieldSerialize.hpp"
#include <telldb/TellDB.hpp>
#include <telldb/Exceptions.hpp>
#include <tellstore/ClientManager.hpp>
#include <crossbow/Serializer.hpp>

using namespace tell::store;

namespace tell {
namespace db {
using namespace impl;

Future<table_t>::Future(std::shared_ptr<GetTableResponse>&& resp, TransactionCache& cache)
    : resp(resp)
    , cache(cache)
{
}

bool Future<table_t>::done() const {
    if (!resp) {
        return true;
    }
    return resp->done();
}

bool Future<table_t>::wait() const {
    if (!resp) {
        return false;
    }
    return resp->wait();
}

table_t Future<table_t>::get() {
    if (!resp) {
        return result;
    }
    auto table = resp->get();
    resp = nullptr;
    result = cache.addTable(name, table);
    return result;
}

Iterator TransactionCache::lower_bound(table_t tableId, const crossbow::string& idxName, const KeyType& key) {
    return mTables[tableId]->lower_bound(idxName, key);
}

Iterator TransactionCache::reverse_lower_bound(table_t tableId, const crossbow::string& idxName, const KeyType& key) {
    return mTables[tableId]->reverse_lower_bound(idxName, key);
}

TransactionCache::TransactionCache(TellDBContext& context,
        store::ClientHandle& handle,
        store::ClientTransaction& transaction,
        crossbow::ChunkMemoryPool& pool)
    : context(context)
    , mHandle(handle)
    , mTransaction(transaction)
    , mPool(pool)
    , mTables(&pool)
{}

Future<table_t> TransactionCache::openTable(const crossbow::string& name) {
    auto iter = context.tableNames.find(name);
    if (iter != context.tableNames.end()) {
        auto tableId = iter->second;
        auto res = Future<table_t>(nullptr, *this);
        res.result.value = tableId.value;
        if (mTables.count(tableId) == 0) {
            const auto& t = context.tables[res.result];
            addTable(*context.tables[res.result], context.indexes->openIndexes(mTransaction.snapshot(), mHandle, *t));
        }
        return res;
    }
    return Future<table_t>(mHandle.getTable(name), *this);
}

table_t TransactionCache::createTable(const crossbow::string& name, const store::Schema& schema) {
    auto table = mHandle.createTable(name, schema);
    table_t tableId{table.tableId()};
    context.indexes->createIndexes(mTransaction.snapshot(), mHandle, table);
    context.tableNames.emplace(name, tableId);
    auto cTable = new Table(table);
    context.tables.emplace(tableId, cTable);
    mTables.emplace(tableId,
            new (&mPool) TableCache(*cTable,
                context,
                mTransaction,
                mPool,
                context.indexes->createIndexes(mTransaction.snapshot(), mHandle, *cTable)));
    return tableId;
}

Future<Tuple> TransactionCache::get(table_t table, key_t key) {
    auto cache = mTables.at(table);
    return cache->get(key);
}

void TransactionCache::insert(table_t table, key_t key, const Tuple& tuple) {
    mTables.at(table)->insert(key, tuple);
}

void TransactionCache::update(table_t table, key_t key, const Tuple& from, const Tuple& to) {
    mTables.at(table)->update(key, from, to);
}

void TransactionCache::remove(table_t table, key_t key, const Tuple& tuple) {
    mTables.at(table)->remove(key, tuple);
}

TransactionCache::~TransactionCache() {
    for (auto& p : mTables) {
        delete p.second;
    }
}

table_t TransactionCache::addTable(const tell::store::Table& table,
        std::unordered_map<crossbow::string,
        impl::IndexWrapper>&& indexes) {
    auto id = table.tableId();
    mTables.emplace(table_t { id }, new (&mPool) TableCache(table, context, mTransaction, mPool, std::move(indexes)));
    table_t res;
    res.value = id;
    return res;
}

table_t TransactionCache::addTable(const crossbow::string& name, const tell::store::Table& table) {
    auto indexes = context.indexes->openIndexes(mTransaction.snapshot(), mHandle, table);
    table_t res{table.tableId()};
    Table* t = nullptr;
    auto iter = context.tables.find(res);
    if (iter == context.tables.end()) {
        context.tableNames.insert(std::make_pair(name, res));
        auto p = context.tables.insert(std::make_pair(res, new Table(table)));
        t = p.first->second;
    } else {
        t = iter->second;
    }
    return addTable(*t, std::move(indexes));
}

void TransactionCache::rollback() {
    for (auto p : mTables) {
        p.second->rollback();
    }
}

void TransactionCache::writeBack() {
    for (auto p : mTables) {
        p.second->writeBack();
    }
}

void TransactionCache::writeIndexes() {
    for (auto p : mTables) {
        p.second->writeIndexes();
    }
}

std::pair<size_t, uint8_t*> TransactionCache::undoLog(bool withIndexes) const {
    crossbow::sizer s;
    if (withIndexes) {
        for (const auto& table : mTables) {
            const auto& indexes = table.second->indexes();
            for (const auto& idx : indexes) {
                s & idx.first;
                s & idx.second.cache();
            }
        }
    }
    for (const auto& t : mTables) {
        const auto& cs = t.second->changes();
        uint32_t numChanges = cs.size();
        s & t.first;
        s & numChanges;
        for (const auto& c : cs) {
            s & c.first;
        }
    }
    auto res = reinterpret_cast<uint8_t*>(s.size);
    crossbow::serializer ser(res);
    for (const auto& t : mTables) {
        ser & t.first;
        const auto& cs = t.second->changes();
        uint32_t numChanges = cs.size();
        ser & numChanges;
        for (const auto& c : cs) {
            ser & c.first;
        }
        if (withIndexes) {
            const auto& indexes = t.second->indexes();
            for (const auto& idx : indexes) {
                s & idx.first;
                s & idx.second.cache();
            }
        }
    }
    return std::make_pair(s.size, res);
}

template class Future<table_t>;
} // namespace db
} // namespace tell

