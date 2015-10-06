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

#include <telldb/Transaction.hpp>
#include <tellstore/ClientManager.hpp>

using namespace tell::store;

namespace tell {
namespace db {
using namespace impl;

Transaction::Transaction(ClientHandle& handle, ClientTransaction& tx, TellDBContext& context, TransactionType type)
    : mHandle(handle)
    , mTx(tx)
    , mContext(context)
    , mType(type)
    , mCache(new (&mPool) TransactionCache(context, tx, mPool))
{}

Future<table_t> Transaction::openTable(const crossbow::string& name) {
    return mCache->openTable(mHandle, name);
}

Future<Tuple> Transaction::get(table_t table, key_t key) {
    return mCache->get(table, key);
}

void Transaction::insert(table_t table, key_t key, const Tuple& tuple) {
    return mCache->insert(table, key, tuple);
}

void Transaction::update(table_t table, key_t key, const Tuple& tuple) {
    return mCache->update(table, key, tuple);
}

void Transaction::remove(table_t table, key_t key) {
    return mCache->remove(table, key);
}

} // namespace db
} // namespace tell

