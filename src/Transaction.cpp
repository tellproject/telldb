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
#include <telldb/ScanQuery.hpp>
#include <telldb/Transaction.hpp>
#include <telldb/Exceptions.hpp>
#include <tellstore/ClientManager.hpp>

using namespace tell::store;

namespace tell {
namespace db {
namespace {

constexpr size_t gMaxUndoLogSize = 16*1024;

} // anonymous namespace

using namespace impl;


class CounterImpl {
    RemoteCounter remoteCounter;
public:
    CounterImpl(RemoteCounter&& counter)
        : remoteCounter(std::move(counter))
    {}
    uint64_t next(store::ClientHandle& handle) {
        return remoteCounter.incrementAndGet(handle);
    }
};

Counter::Counter(CounterImpl* impl, store::ClientHandle& handle)
    : impl(impl)
    , mHandle(handle)
{}
Counter::Counter(Counter&&) = default;
Counter::~Counter() = default;

uint64_t Counter::next() {
    return impl->next(mHandle);
}

TellDBContext::~TellDBContext() {
    for (auto& p : tables) {
        delete p.second;
    }
    for (auto& c : counters) {
        delete c.second;
    }
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

const tell::store::Schema& Transaction::getSchema(table_t table) {
    return mContext.tables[table]->record().schema();
}

crossbow::string globalCounterName(const crossbow::string& name) {
    return "__global_counter_" + name;
}

Counter Transaction::createCounter(const crossbow::string& name) {
    RemoteCounter::createTable(mHandle, globalCounterName(name));
    return getCounter(name);
}

Counter Transaction::getCounter(const crossbow::string& name) {
    auto iter = mContext.counters.find(name);
    CounterImpl* counterImpl;
    if (iter != mContext.counters.end()) {
        counterImpl = iter->second;
    } else {
        auto counterName = globalCounterName(name);
        auto tId = openTable(counterName).get();
        auto table = std::make_shared<store::Table>(*mContext.tables.at(tId));
        counterImpl = new CounterImpl(RemoteCounter(std::move(table), 1));
        auto res = mContext.counters.emplace(name, counterImpl);
        if (!res.second) {
            delete counterImpl;
            counterImpl = res.first->second;
        }
    }
    return Counter(counterImpl, mHandle);
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

Tuple Transaction::newTuple(table_t table) {
    const auto& t = mContext.tables.at(table);
    const auto& rec = t->record();
    return Tuple(rec, mPool);
}

void Transaction::insert(table_t table, key_t key, const std::unordered_map<crossbow::string, Field>& values) {
    const auto& t = mContext.tables.at(table);
    const auto& rec = t->record();
    const auto& schema = rec.schema();
    Tuple tuple(rec, mPool);
    Schema::id_t i = 0;
    auto numFixedSize = schema.fixedSizeFields().size();
    const auto& fixedSizeFields = schema.fixedSizeFields();

    auto setField = [&tuple, &values] (Schema::id_t idx, const store::Field& field) {
        auto iter = values.find(field.name());
        if (iter == values.end() || iter->second.type() == store::FieldType::NULLTYPE) {
            if (field.isNotNull()) {
                throw FieldNotSet(field.name());
            }
            tuple[idx] = nullptr;
        } else {
            if (field.type() != iter->second.type()) {
                throw WrongFieldType(field.name());
            }
            tuple[idx] = iter->second;
        }
    };

    for (; i < numFixedSize; ++i) {
        setField(i, fixedSizeFields[i]);
    }
    const auto& varSizeFields = schema.varSizeFields();
    auto numVarSize = schema.varSizeFields().size();
    for (; i < numVarSize + numFixedSize; ++i) {
        setField(i, varSizeFields[i - numFixedSize]);
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

std::shared_ptr<store::ScanIterator> Transaction::scan(const ScanQuery& scanQuery, store::ScanMemoryManager& memoryManager) {
    if (mType != store::TransactionType::ANALYTICAL) {
        throw std::runtime_error("Scan only supported for analytical transactions");
    }
    const auto& t = mContext.tables.at(scanQuery.table());
    scanQuery.verify(t->record().schema());
    uint32_t selectionLength, queryLength;
    std::unique_ptr<char[]> selection, query;
    scanQuery.serializeQuery(query, queryLength);
    scanQuery.serializeSelection(selection, selectionLength);
    return mHandle.scan(*mContext.tables[scanQuery.table()],
            *mSnapshot,
            memoryManager,
            scanQuery.queryType(),
            selectionLength, selection.get(),
            queryLength, query.get());
}

void Transaction::commit() {
    writeBack();
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
    if (log.first > gMaxUndoLogSize) {
        if ((log.first / gMaxUndoLogSize) >= static_cast<decltype(log.first)>(std::numeric_limits<uint16_t>::max())) {
            throw std::runtime_error("Undo Log is too large");
        }
        size_t sizeWritten = 0;
        std::vector<std::shared_ptr<tell::store::ModificationResponse>> responses;
        responses.reserve((log.first / gMaxUndoLogSize) + 1);
        for (uint64_t chunkNum = 0; sizeWritten < log.first; ++chunkNum) {
            auto chunkKey = (key | chunkNum);
            auto toWrite = std::min(log.first - sizeWritten, gMaxUndoLogSize);
            responses.emplace_back(mHandle.insert(mContext.clientTable->txTable(), chunkKey, 0, {
                        std::make_pair("value", crossbow::string(reinterpret_cast<char*>(log.second) + sizeWritten,
                                toWrite))
                    }));
            sizeWritten += toWrite;
        }
        for (auto i = responses.rbegin(); i != responses.rend(); ++i) {
            __attribute__((unused)) auto res = (*i)->waitForResult();
            LOG_ASSERT(res, "Writeback did not succeed");
        }
    } else {
        auto resp = mHandle.insert(mContext.clientTable->txTable(), key, 0, {
                std::make_pair("value", crossbow::string(reinterpret_cast<char*>(log.second), log.first))
                });
        __attribute__((unused)) auto res = resp->waitForResult();
        LOG_ASSERT(res, "Writeback did not succeed");
    }
}

void Transaction::removeUndoLog(std::pair<size_t, uint8_t*> log) {
    uint64_t key = mSnapshot->version() << 16;
    if (log.first > gMaxUndoLogSize) {
        if ((log.first / gMaxUndoLogSize) >= static_cast<decltype(log.first)>(std::numeric_limits<uint16_t>::max())) {
            throw std::runtime_error("Undo Log is too large");
        }
        size_t sizeWritten = 0;
        std::vector<std::shared_ptr<tell::store::ModificationResponse>> responses;
        responses.reserve((log.first / gMaxUndoLogSize) + 1);
        for (uint64_t chunkNum = 0; sizeWritten < log.first; ++chunkNum) {
            auto chunkKey = (key | chunkNum);
            auto segSize = std::min(log.first - sizeWritten, gMaxUndoLogSize);
            responses.emplace_back(mHandle.remove(mContext.clientTable->txTable(), chunkKey, 1));
            sizeWritten += segSize;
        }
        for (auto i = responses.rbegin(); i != responses.rend(); ++i) {
            __attribute__((unused)) auto res = (*i)->waitForResult();
            LOG_ASSERT(res, "Could not delete undo log");
        }
    } else {
        auto resp = mHandle.remove(mContext.clientTable->txTable(), key, 1);
        __attribute__((unused)) auto res = resp->waitForResult();
        LOG_ASSERT(res, "Could not delete undo log");
    }
}

void Transaction::writeBack(bool withIndexes) {
    if (mCommitted) {
        throw std::logic_error("Transaction has already committed");
    }
    if (!mCache->hasChanges()) {
        return;
    }
    if (mType != store::TransactionType::READ_WRITE) {
        throw std::logic_error("Transaction is read only");
    }
    auto undoLog = mCache->undoLog(withIndexes);
    writeUndoLog(undoLog);
    mCache->writeBack();
    if (withIndexes) {
        mCache->writeIndexes();
    }
    removeUndoLog(undoLog);
}

const store::Record& Transaction::getRecord(table_t table) const {
    return mCache->record(table);
}

} // namespace db
} // namespace tell
