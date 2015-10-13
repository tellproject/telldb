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
#pragma once
#include "TableData.hpp"
#include "BdTreeBackend.hpp"
#include <telldb/Field.hpp>
#include <telldb/Types.hpp>
#include <bdtree/bdtree.h>
#include <telldb/Tuple.hpp>

#include <map>

namespace tell {
namespace db {
namespace impl {

/**
 * The key type used in Indexes:
 *  - A list of fields (to support multivalue indexes)
 *  - A version number. This is set to uint64_max by default,
 *    on deletion it is set to the version of the transaction
 *    that made the deletion
 *  - A counter - 0 by default. This is used for non-unique
 *    indexes. If a index is non-unique, we just set this to
 *    a random number until the whole key is unique.
 */
using KeyType = std::tuple<std::vector<Field>, uint64_t, uint32_t>;
/**
 * The value for an index map is simply the key of of the tuple
 */
using ValueType = key_t;

} // namespace impl
} // namespace db
} // namespace tell

namespace bdtree {

extern template class bdtree::map<tell::db::impl::KeyType, tell::db::impl::ValueType, tell::db::BdTreeBackend>;

} // bdtree

namespace tell {
namespace store {

class ClientHandle;

} // namespace store
namespace db {
namespace impl {

enum class IndexOperation {
    Insert, Delete
};

class IndexIterator {
public:
    using Cache = std::multimap<KeyType, std::pair<IndexOperation, ValueType>>;
    using Index = bdtree::map<KeyType, ValueType, BdTreeBackend>;
private:
    Index::iterator idxIter;
    Index::iterator idxEnd;
    Cache::iterator cacheIter;
    Cache::iterator cacheEnd;
public:
    IndexIterator(
            Index::iterator idxBegin,
            Index::iterator idxEnd,
            Cache::iterator cacheBegin,
            Cache::iterator cacheEnd
            )
        : idxIter(idxBegin)
        , idxEnd(idxEnd)
        , cacheIter(cacheBegin)
        , cacheEnd(cacheEnd)
    {}
    bool done() const;
    void advance();
    key_t value() const;
};

class IndexWrapper {
public: // Types
    using Iterator = IndexIterator;
    using Index = Iterator::Index;
    using IndexCache = bdtree::logical_table_cache<KeyType, ValueType, BdTreeBackend>;
    using Cache = Iterator::Cache;
private:
    std::vector<store::Schema::id_t> mFields;
    BdTreeBackend mBackend;
    Index mBdTree;
    Cache mCache;
    uint64_t mTxId;
public:
    IndexWrapper(
            const std::vector<store::Schema::id_t>& fieds,
            BdTreeBackend&& backend,
            IndexCache& cache,
            uint64_t txId,
            bool init = false);
public: // Access
    Iterator lower_bound(const std::vector<tell::db::Field>& key);
public: // Modifications
    void insert(key_t key, const Tuple& tuple);
    void update(key_t key, const Tuple& old, const Tuple& next);
    void remove(key_t key, const Tuple& tuple);
private:
    std::vector<Field> keyOf(const Tuple& tuple);
};

class Indexes {
public: // types
    using Index = typename IndexWrapper::Index;
    struct IndexTables {
        std::vector<store::Schema::id_t> fields;
        TableData ptrTable;
        TableData nodeTable;
    };
private: // members
    std::shared_ptr<store::Table> mCounterTable;
    std::unordered_map<table_t, std::unordered_map<crossbow::string, IndexTables*>> mIndexes;
    // TODO: Caching is turned off for now - later we need to redesign cache handling
    IndexWrapper::IndexCache mBdTreeCache;
public:
    Indexes(store::ClientHandle& handle);
public:
    std::unordered_map<crossbow::string, IndexWrapper> openIndexes(
            uint64_t txId,
            store::ClientHandle& handle,
            const store::Table& table);
    std::unordered_map<crossbow::string, IndexWrapper> createIndexes(
            uint64_t txId,
            store::ClientHandle& handle,
            const store::Table& table);
};

} // namespace impl
} // namespace db
} // namespace tell
