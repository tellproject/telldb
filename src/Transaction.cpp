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
#include "RemoteCounter.hpp"

#include <telldb/TellDB.hpp>
#include <telldb/Transaction.hpp>
#include <telldb/Exceptions.hpp>
#include <tellstore/ClientManager.hpp>

using namespace tell::store;

namespace tell {
namespace db {
using namespace impl;


class CounterImpl {
    RemoteCounter remoteCounter;
    ClientHandle& mHandle;
public:
    CounterImpl(RemoteCounter&& counter, ClientHandle& handle)
        : remoteCounter(std::move(counter))
        , mHandle(handle)
    {}
    uint64_t next() {
        return remoteCounter.incrementAndGet(mHandle);
    }
};

Counter::Counter(CounterImpl* impl)
    : impl(impl)
{}
Counter::Counter(Counter&&) = default;
Counter::~Counter() = default;

uint64_t Counter::next() {
    return impl->next();
}

Transaction::Transaction(ClientHandle& handle, TellDBContext& context,
        std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot, TransactionType type)
    : mHandle(handle)
    , mContext(context)
    , mSnapshot(std::move(snapshot))
    , mCache(new (&mPool) TransactionCache(context, mHandle, *mSnapshot, mPool))
    , mType(type)
{
}

crossbow::ChunkMemoryPool& Transaction::pool() {
    return mPool;
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

crossbow::string globalCounterName(const crossbow::string& name) {
    return "__global_counter_" + name;
}

Counter Transaction::createCounter(const crossbow::string& name) {
    RemoteCounter::createTable(mHandle, globalCounterName(name));
    return getCounter(name);
}

Counter Transaction::getCounter(const crossbow::string& name) {
    auto counterName = globalCounterName(name);
    auto iter = mContext.tableNames.find(counterName);
    std::shared_ptr<store::Table> counterTable;
    if (iter != mContext.tableNames.end()) {
        counterTable = std::make_shared<store::Table>(*mContext.tables.at(iter->second));
    } else {
        auto tId = openTable(counterName).get();
        counterTable = std::make_shared<store::Table>(*mContext.tables.at(tId));
    }
    return Counter(new CounterImpl(RemoteCounter(counterTable, 1), mHandle));
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

void Transaction::insert(table_t table, key_t key, const std::unordered_map<crossbow::string, Field>& values) {
    const auto& t = mContext.tables.at(table);
    const auto& rec = t->record();
    const auto& schema = rec.schema();
    Tuple tuple(rec, mPool);
    Schema::id_t i = 0;
    auto numFixedSize = schema.fixedSizeFields().size();
    const auto& fixedSizeFields = schema.fixedSizeFields();
    for (; i < numFixedSize; ++i) {
        const auto& field = fixedSizeFields[i];
        auto iter = values.find(field.name());
        if (iter == values.end()) {
            if (field.isNotNull()) {
                throw FieldNotSet(field.name());
            }
            tuple[i] = nullptr;
        } else {
            tuple[i] = iter->second;
        }
    }
    const auto& varSizeFields = schema.varSizeFields();
    auto numVarSize = schema.varSizeFields().size();
    for (; i < numVarSize + numFixedSize; ++i) {
        const auto& field = varSizeFields[i - numFixedSize];
        auto iter = values.find(field.name());
        if (iter == values.end()) {
            if (field.isNotNull()) {
                throw FieldNotSet(field.name());
            }
            tuple[i] = nullptr;
        } else {
            tuple[i] = iter->second;
        }
    }
    mCache->insert(table, key, tuple);
}

void Transaction::insert(table_t table, key_t key, const Tuple& tuple) {
    mCache->insert(table, key, tuple);
}

void Transaction::update(table_t table, key_t key, const Tuple& from, const Tuple& to) {
    mCache->update(table, key, from, to);
}

void Transaction::remove(table_t table, key_t key, const Tuple& tuple) {
    mCache->remove(table, key, tuple);
}

void Transaction::commit() {
    writeBack();
    // if this succeeds, we can write back the indexes
    mHandle.commit(*mSnapshot);
    mCommitted = true;
}

void Transaction::rollback() {
    if (mCommitted) {
        throw std::logic_error("Transaction has already committed");
    }
    mCache->rollback();
    mHandle.commit(*mSnapshot);
    mCommitted = true;
}

void Transaction::writeUndoLog(std::pair<size_t, uint8_t*> log) {
    uint64_t key = mSnapshot->version() << 16;
    if (log.first > 1024) {
        size_t sizeWritten = 0;
        std::vector<std::shared_ptr<tell::store::ModificationResponse>> responses;
        responses.reserve(log.first / 1024);
        for (uint64_t chunkNum = 0; sizeWritten < log.first; ++chunkNum) {
            key = ((key >> 16) << 16) | chunkNum;
            auto toWrite = std::min(log.first - sizeWritten, size_t(1024));
            responses.emplace_back(mHandle.insert(mContext.clientTable->txTable(), key, 0, {
                        std::make_pair("value", crossbow::string(reinterpret_cast<char*>(log.second), toWrite))
                        }));
            sizeWritten += toWrite;
        }
        for (auto i = responses.rbegin(); i != responses.rend(); ++i) {
            auto resp = *i;
            LOG_ASSERT(resp->waitForResult(), "Writeback did not succeed");
        }
    } else {
        auto resp = mHandle.insert(mContext.clientTable->txTable(), key, 0, {
                std::make_pair("value", crossbow::string(reinterpret_cast<char*>(log.second), log.first))
                });
        LOG_ASSERT(resp->waitForResult(), "Writeback did not succeed");
    }
}

void Transaction::writeBack(bool withIndexes) {
    if (mCommitted) {
        throw std::logic_error("Transaction has already committed");
    }
    auto undoLog = mCache->undoLog(withIndexes);
    if (undoLog.first != 0 && mType != store::TransactionType::READ_WRITE) {
        throw std::logic_error("Transaction is read only");
    }
    writeUndoLog(undoLog);
    mCache->writeBack();
    if (withIndexes) {
        mCache->writeIndexes();
    }
}

const store::Record& Transaction::getRecord(table_t table) const {
    return mCache->record(table);
}

} // namespace db
} // namespace tell

