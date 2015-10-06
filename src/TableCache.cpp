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

namespace tell {
namespace db {

TableCache::TableCache(const tell::store::Table& table,
        impl::TellDBContext& context,
        tell::store::ClientTransaction& transaction,
        crossbow::ChunkMemoryPool& pool)
    : mTable(table)
    , mTransaction(transaction)
    , mPool(pool)
    , mCache(&pool)
    , mChanges(&pool)
    , mSchema(&pool)
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
        if (p.second.second != Operation::Delete) {
            delete p.second.first;
        }
    }
}

Future<Tuple> TableCache::get(key_t key) {
    {
        auto iter = mChanges.find(key);
        if (iter != mChanges.end()) {
            if (iter->second.second == Operation::Delete) {
                throw TupleExistsException(key);
            }
            return Future<Tuple>(key, iter->second.first);
        }
    }
    {
        auto iter = mCache.find(key);
        if (iter != mCache.end()) {
            return Future<Tuple>(key, iter->second.first);
        }
    }
    return Future<Tuple>(key, this, mTransaction.get(mTable, key.value));
}

void TableCache::insert(key_t key, const Tuple& tuple) {
    auto c = mChanges.find(key);
    if (c == mChanges.end()) {
        if (mCache.count(key) != 0) {
            throw TupleExistsException(key);
        }
    } else if (c->second.second == Operation::Delete) {
        c->second.second = Operation::Update;
        c->second.first = new (&mPool) Tuple(tuple);
    } else {
        throw TupleExistsException(key);
    }
}

void TableCache::update(key_t key, const Tuple& tuple) {
    {
        auto i = mChanges.find(key);
        if (i != mChanges.end()) {
            if (i->second.second == Operation::Delete) {
                throw TupleDoesNotExist(key);
            }
            delete i->second.first;
            i->second.first = new (&mPool) Tuple(tuple);
            return;
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
        mChanges.emplace(key, std::make_pair(new (&mPool) Tuple(tuple), Operation::Update));
    }
}

void TableCache::remove(key_t key) {
    {
        auto i = mChanges.find(key);
        if (i != mChanges.end()) {
            if (i->second.second == Operation::Delete) {
                throw TupleDoesNotExist(key);
            } else if (i->second.second == Operation::Insert) {
                mChanges.erase(i);
                return;
            }
            delete i->second.first;
            i->second.first = nullptr;
            i->second.second = Operation::Delete;
            return;
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
        mChanges.emplace(key, std::make_pair(nullptr, Operation::Delete));
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

