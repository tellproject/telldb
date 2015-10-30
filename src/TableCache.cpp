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
#include "TableCache.hpp"
#include <tellstore/ClientManager.hpp>
#include <telldb/Exceptions.hpp>

#include <boost/lexical_cast.hpp>
#include <memory>

namespace tell {
namespace db {

TableCache::TableCache(const tell::store::Table& table,
        tell::store::ClientHandle& handle,
        const commitmanager::SnapshotDescriptor& snapshot,
        crossbow::ChunkMemoryPool& pool,
        std::unordered_map<crossbow::string, impl::IndexWrapper>&& indexes)
    : mTable(table)
    , mHandle(handle)
    , mSnapshot(snapshot)
    , mPool(pool)
    , mCache(&pool)
    , mChanges(&pool)
    , mSchema(&pool)
    , mIndexes(std::move(indexes))
{
    id_t currId = 0;
    const auto& schema = table.record().schema();
    {
        const auto& fields = schema.fixedSizeFields();
        for (const auto& field : fields) {
            mSchema.emplace(field.name(), currId++);
        }
    }
    {
        const auto& fields = schema.varSizeFields();
        for (const auto& field : fields) {
            mSchema.emplace(field.name(), currId++);
        }
    }
}

TableCache::~TableCache() {
    for (auto& p : mCache) {
        delete p.second.first;
    }
    for (auto& p : mChanges) {
        if (std::get<1>(p.second) != Operation::Delete) {
            delete std::get<0>(p.second);
        }
    }
}

Future<Tuple> TableCache::get(key_t key) {
    {
        auto iter = mChanges.find(key);
        if (iter != mChanges.end()) {
            if (std::get<1>(iter->second) == Operation::Delete) {
                throw TupleExistsException(key);
            }
            return Future<Tuple>(key, std::get<0>(iter->second));
        }
    }
    {
        auto iter = mCache.find(key);
        if (iter != mCache.end()) {
            return Future<Tuple>(key, iter->second.first);
        }
    }
    return Future<Tuple>(key, this, mHandle.get(mTable, key.value, mSnapshot));
}

Iterator TableCache::lower_bound(const crossbow::string& name, const KeyType& key) {
    return mIndexes.at(name).lower_bound(key);
}

Iterator TableCache::reverse_lower_bound(const crossbow::string& name, const KeyType& key) {
    return mIndexes.at(name).lower_bound(key);
}

void TableCache::insert(key_t key, const Tuple& tuple) {
    auto c = mChanges.find(key);
    if (c == mChanges.end()) {
        if (mCache.count(key) != 0) {
            throw TupleExistsException(key);
        }
        mChanges.emplace(key, std::make_tuple(new (&mPool) Tuple(tuple), Operation::Insert, false));
    } else if (std::get<1>(c->second) == Operation::Delete) {
        std::get<1>(c->second) = Operation::Update;
        std::get<0>(c->second) = new (&mPool) Tuple(tuple);
    } else {
        throw TupleExistsException(key);
    }
    for (auto& idx : mIndexes) {
        idx.second.insert(key, tuple);
    }
}

void TableCache::update(key_t key, const Tuple& from, const Tuple& to) {
    {
        auto i = mChanges.find(key);
        if (i != mChanges.end()) {
            if (std::get<1>(i->second) == Operation::Delete) {
                throw TupleDoesNotExist(key);
            }
            delete std::get<0>(i->second);
            std::get<0>(i->second) = new (&mPool) Tuple(to);
            goto END;
        }
    }
    {
        auto i = mCache.find(key);
        if (i != mCache.end()) {
            if (i->second.second) {
                throw Conflict(key);
            }
        } 
        // We do an optimistic update - if the tuple is not cached, we assume that there
        // won't be an update
        mChanges.emplace(key, std::make_tuple(new (&mPool) Tuple(to), Operation::Update, false));
    }
END:
    for (auto& idx : mIndexes) {
        idx.second.update(key, from, to);
    }
}

void TableCache::remove(key_t key, const Tuple& tuple) {
    {
        auto i = mChanges.find(key);
        if (i != mChanges.end()) {
            if (std::get<1>(i->second) == Operation::Delete) {
                throw TupleDoesNotExist(key);
            } else if (std::get<1>(i->second)== Operation::Insert) {
                mChanges.erase(i);
                goto END;
            }
            delete std::get<0>(i->second);
            std::get<0>(i->second) = nullptr;
            std::get<1>(i->second) = Operation::Delete;
            goto END;
        }
    }
    {
        auto i = mCache.find(key);
        if (i != mCache.end()) {
            if (i->second.second) {
                throw Conflict(key);
            }
        } 
        // We do an optimistic update - if the tuple is not cached, we assume that there
        // won't be an update
        mChanges.emplace(key, std::make_tuple(nullptr, Operation::Delete, false));
    }
END:
    for (auto& idx : mIndexes) {
        idx.second.remove(key, tuple);
    }
}

void TableCache::writeBack() {
    using Resp = std::shared_ptr<store::ModificationResponse>;
    using ChangeResp = std::pair<Resp, ChangesMap::iterator>;
    std::vector<ChangeResp, crossbow::ChunkAllocator<ChangeResp>> responses(&mPool);
    responses.reserve(mCache.size());
    for (auto iter = mChanges.begin(); iter != mChanges.end(); ++iter) {
        auto& change = *iter;
        bool& didChange = std::get<2>(change.second);
        if (didChange) continue;
        auto tuple = std::get<0>(change.second);
        switch (std::get<1>(change.second)) {
        case Operation::Insert:
            responses.emplace_back(std::make_pair(mHandle.insert(mTable, change.first, mSnapshot, *tuple), iter));
            break;
        case Operation::Update:
            responses.emplace_back(std::make_pair(mHandle.update(mTable, change.first, mSnapshot, *tuple), iter));
            break;
        case Operation::Delete:
            responses.emplace_back(std::make_pair(mHandle.remove(mTable, change.first, mSnapshot), iter));
        }
    }
    bool hadError = false;
    // we put this into the stack, because in normal case the vector should stay
    // empty (and we optimise for the normal case). The unique pointer makes sure
    // that the object gets deleted after moving it into the exception object
    std::unique_ptr<std::vector<key_t>> conflicts = nullptr;
    for (auto i = responses.rbegin(); i != responses.rend(); ++i) {
        if (i->first->error()) {
            hadError = true;
            if (conflicts.get() == nullptr) {
                conflicts.reset(new std::vector<key_t>());
            }
            conflicts->push_back(i->second->first);
        } else {
            std::get<2>(i->second->second) = true;
        }
    }
    if (hadError) {
        throw Conflicts(std::move(*conflicts));
    }
}

void TableCache::rollback() {
    using Resp = std::shared_ptr<store::ModificationResponse>;
    std::vector<Resp, crossbow::ChunkAllocator<Resp>> responses(&mPool);
    responses.reserve(mCache.size());
    for (auto& change : mChanges) {
        if (!std::get<2>(change.second)) continue;
        responses.emplace_back(mHandle.revert(mTable, change.first, mSnapshot));
    }
    for (auto iter = responses.rbegin(); iter != responses.rend(); ++iter) {
        if ((*iter)->error()) {
            // TODO: not clear what to do in this case
            assert(false);
        }
    }
}

void TableCache::writeIndexes() {
    for (auto& idx : mIndexes) {
        idx.second.writeBack();
    }
}

void TableCache::undoIndexes() {
    for (auto& idx : mIndexes) {
        idx.second.undo();
    }
}

const Tuple& TableCache::addTuple(key_t key, const tell::store::Tuple& tuple) {
    auto res = new (&mPool) Tuple(mTable.record(), tuple, mPool);
    mCache.insert(std::make_pair(key, std::make_pair(res, tuple.isNewest())));
    return *res;
}

Future<Tuple>::Future(key_t key, const Tuple* result)
    : key(key)
    , result(result)
    , cache(nullptr)
{}

Future<Tuple>::Future(key_t key, TableCache* cache, std::shared_ptr<store::GetResponse>&& response)
    : key(key)
    , result(nullptr)
    , cache(cache)
    , response(std::move(response))
{}

bool Future<Tuple>::done() const {
    if (result) return true;
    return response->done();
}

bool Future<Tuple>::wait() const {
    if (result) return true;
    return response->wait();
}

const Tuple& Future<Tuple>::get() {
    if (result) return *result;
    else {
        auto resp = response->get();
        if (!resp->found()) {
            crossbow::string msg = "Tuple with key ";
            msg += boost::lexical_cast<crossbow::string>(key);
            msg += " does not exist";
            throw std::range_error(msg.data());
        }
        result = &cache->addTuple(key, *resp);
        return *result;
    }
}

template class Future<Tuple>;
} // namespace db
} // namespace tell

