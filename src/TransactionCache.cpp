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
#include <telldb/TellDB.hpp>
#include <telldb/Exceptions.hpp>
#include <tellstore/ClientManager.hpp>

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

TransactionCache::TransactionCache(TellDBContext& context,
        tell::store::ClientTransaction& transaction,
        crossbow::ChunkMemoryPool& pool)
    : context(context)
    , mTransaction(transaction)
    , mPool(pool)
    , mTables(&pool)
{}

Future<table_t> TransactionCache::openTable(ClientHandle& handle, const crossbow::string& name) {
    if (context.tableNames.count(name)) {
        auto res = Future<table_t>(nullptr, *this);
        res.result.value = context.tableNames[name].value;
        if (mTables.count(res.result) == 0) {
            addTable(*context.tables[res.result]);
        }
        return res;
    }
    return Future<table_t>(handle.getTable(name), *this);
}

Future<Tuple> TransactionCache::get(table_t table, key_t key) {
    auto cache = mTables.at(table);
    return cache->get(key);
}

void TransactionCache::insert(table_t table, key_t key, const Tuple& tuple) {
    mTables.at(table)->insert(key, tuple);
}

void TransactionCache::update(table_t table, key_t key, const Tuple& tuple) {
    mTables.at(table)->update(key, tuple);
}

void TransactionCache::remove(table_t table, key_t key) {
    mTables.at(table)->remove(key);
}

TransactionCache::~TransactionCache() {
    for (auto& p : mTables) {
        delete p.second;
    }
}

table_t TransactionCache::addTable(const tell::store::Table& table) {
    auto id = table.tableId();
    mTables.emplace(table_t { id }, new (&mPool) TableCache(table, context, mTransaction, mPool));
    table_t res;
    res.value = id;
    return res;
}

table_t TransactionCache::addTable(const crossbow::string& name, const tell::store::Table& table) {
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
    return addTable(*t);
}

void TransactionCache::writeBack() {
    using Resp = std::shared_ptr<store::ModificationResponse>;
    std::vector<Resp, crossbow::ChunkAllocator<Resp>> responses(&mPool);
    std::vector<key_t, crossbow::ChunkAllocator<key_t>> keys(&mPool);
    for (auto p : mTables) {
        const auto& table = p.second->table();
        const auto& changes = p.second->changes();
        for (auto change : changes) {
            auto key = change.first;
            auto k = key.value;
            keys.push_back(key);
            switch (change.second.second) {
            case TableCache::Operation::Insert:
                responses.emplace_back(mTransaction.insert(table, k, *change.second.first));
                break;
            case TableCache::Operation::Update:
                responses.emplace_back(mTransaction.update(table, k, *change.second.first));
                break;
            case TableCache::Operation::Delete:
                responses.emplace_back(mTransaction.remove(table, k));
            }
        }
    }
    for (int i = responses.size(); i > 0; --i) {
        if (responses[i - 1]->error()) {
            throw Conflict(keys[i - 1]);
        }
    }
    // at this point we know that there won't be any conflicts
    // and we now write back the indexes
    for (auto p : mTables) {
        p.second->writeIndexes();
    }
}

crossbow::basic_string<char, std::char_traits<char>, crossbow::ChunkAllocator<char>> TransactionCache::undoLog() const {
    size_t numChanges = 0;
    for (const auto& t : mTables) {
        const auto& c = t.second->changes();
        numChanges += c.size();
    }
    crossbow::basic_string<char, std::char_traits<char>, crossbow::ChunkAllocator<char>> res(&mPool);
    res.reserve(numChanges * 16);
    for (const auto& t : mTables) {
        const auto& cs = t.second->changes();
        for (const auto& c : cs) {
            char l[16];
            memcpy(l, &t.first.value, 8);
            memcpy(l + 8, &c.first.value, 8);
            res.append(l, 16);
        }
    }
    return res;
}

template class Future<table_t>;
} // namespace db
} // namespace tell

