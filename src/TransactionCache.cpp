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

TransactionCache::TransactionCache(TellDBContext& context, tell::store::ClientTransaction& transaction)
    : context(context)
    , mTransaction(transaction)
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

TransactionCache::~TransactionCache() {
    for (auto& p : mTables) {
        delete p.second;
    }
}

table_t TransactionCache::addTable(const tell::store::Table& table) {
    auto id = table.tableId();
    mTables.emplace(table_t { id }, new TableCache(table, context, mTransaction));
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


template class Future<table_t>;
} // namespace db
} // namespace tell

