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

#include <telldb/TellDB.hpp>
#include <telldb/Transaction.hpp>
#include <telldb/Exceptions.hpp>
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
    , mCache(new (&mPool) TransactionCache(context, mHandle, tx, mPool))
{
}

Transaction::~Transaction() {
    if (!mCommitted) {
        rollback();
    }
}

Future<table_t> Transaction::openTable(const crossbow::string& name) {
    return mCache->openTable(name);
}

table_t Transaction::createTable(const crossbow::string& name, const store::Schema& schema) {
    return mCache->createTable(name, schema);
}

Future<Tuple> Transaction::get(table_t table, key_t key) {
    return mCache->get(table, key);
}

Iterator Transaction::lower_bound(table_t tableId, const crossbow::string& idxName, const KeyType& key) {
    return mCache->lower_bound(tableId, idxName, key);
}

Iterator Transaction::reverse_lower_bound(table_t tableId, const crossbow::string& idxName, const KeyType& key) {
    return mCache->reverse_lower_bound(tableId, idxName, key);
}

void Transaction::insert(table_t table, key_t key, const Tuple& tuple) {
    return mCache->insert(table, key, tuple);
}

void Transaction::update(table_t table, key_t key, const Tuple& from, const Tuple& to) {
    return mCache->update(table, key, from, to);
}

void Transaction::remove(table_t table, key_t key, const Tuple& tuple) {
    return mCache->remove(table, key, tuple);
}

void Transaction::commit() {
    writeBack();
    // if this succeeds, we can write back the indexes
    mTx.commit();
    mCommitted = true;
}

void Transaction::rollback() {
    mCache->rollback();
    mTx.commit();
    mCommitted = true;
}

void Transaction::writeUndoLog(std::pair<size_t, uint8_t*> log) {
    auto version = mTx.snapshot().version();
    if (mDidWriteBack) {
        auto resp = mHandle.insert(mContext.clientTable->txTable(),
                version, 0,
                store::GenericTuple{std::make_pair("value",
                        crossbow::string(reinterpret_cast<char*>(log.second), log.first))});
        resp->waitForResult();
        mDidWriteBack = true;
    } else {
        auto resp = mHandle.update(mContext.clientTable->txTable(),
                version, 0,
                store::GenericTuple{std::make_pair("value",
                        crossbow::string(reinterpret_cast<char*>(log.second), log.first))});
    }
}

void Transaction::writeBack(bool withIndexes) {
    auto undoLog = mCache->undoLog(withIndexes);
    writeUndoLog(undoLog);
    mCache->writeBack();
    if (withIndexes) {
        mCache->writeIndexes();
    }
}

} // namespace db
} // namespace tell

