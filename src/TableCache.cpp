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

#include <boost/lexical_cast.hpp>

using namespace tell::store;

namespace tell {
namespace db {

TableCache::TableCache(const tell::store::Table& table,
        impl::TellDBContext& context,
        tell::store::ClientTransaction& transaction)
    : mTable(table)
    , mContext(context)
    , mTransaction(transaction)
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

Future<Tuple> TableCache::get(key_t key) {
    {
        auto iter = mChanges.find(key);
        if (iter != mChanges.end()) {
            return Future<Tuple>(key, iter->second);
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

const Tuple& TableCache::addTuple(key_t key, const tell::store::Tuple& tuple) {
    auto res = new Tuple(mSchema, mTable.record(), tuple);
    mCache.insert(std::make_pair(key, std::make_pair(res, tuple.isNewest())));
    return *res;
}

Future<Tuple>::Future(key_t key, const Tuple* result)
    : key(key)
    , result(result)
    , cache(nullptr)
{}

Future<Tuple>::Future(key_t key, TableCache* cache, std::shared_ptr<GetResponse>&& response)
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

