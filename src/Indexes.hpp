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
#include <commitmanager/SnapshotDescriptor.hpp>

#include <map>

namespace tell {
namespace db {
namespace impl {

using KeyType = std::vector<Field>;
/**
 * The key type used in Indexes:
 *  - A list of fields (to support multivalue indexes)
 *  - A version number. This is set to uint64_max by default,
 *    on deletion it is set to the version of the transaction
 *    that made the deletion
 */
using UniqueKeyType = std::tuple<KeyType, uint64_t>;
using NonUniqueKeyType = decltype(std::tuple_cat(std::declval<UniqueKeyType>(), std::declval<std::tuple<key_t>>()));
/**
 * The value for an index map is simply the key of of the tuple
 */
using ValueType = key_t;
using UniqueValueType = ValueType;
// something small - we won't use it
using NonUniqueValueType = bool;

} // namespace impl
} // namespace db
} // namespace tell

namespace bdtree {

extern template class map<tell::db::impl::UniqueKeyType, tell::db::impl::UniqueValueType, tell::db::BdTreeBackend>;
extern template class map<tell::db::impl::NonUniqueKeyType, tell::db::impl::NonUniqueValueType, tell::db::BdTreeBackend>;

} // bdtree

namespace tell {
namespace store {

class ClientHandle;

} // namespace store
namespace db {
namespace impl {

class BdTree {
protected:
    const commitmanager::SnapshotDescriptor& mSnapshot;
public:
    BdTree(const commitmanager::SnapshotDescriptor& snapshot) : mSnapshot(snapshot) {}
    virtual ~BdTree();
    virtual bool insert(const KeyType& key, const ValueType& value) = 0;
    virtual bool erase(const KeyType& key) = 0;
};

class UniqueBdTree : public BdTree {
    using IndexCache = bdtree::logical_table_cache<UniqueKeyType, UniqueValueType, BdTreeBackend>;
private:
    IndexCache mCache;
    bdtree::map<UniqueKeyType, UniqueValueType, BdTreeBackend> mMap;
public:
    UniqueBdTree(const commitmanager::SnapshotDescriptor& snapshot, BdTreeBackend& backend, bool doInit = false);
    bool insert(const KeyType& key, const ValueType& value) override;
    bool erase(const KeyType& key) override;
};

class NonUniqueBdTree : public BdTree {
    using IndexCache = bdtree::logical_table_cache<NonUniqueKeyType, NonUniqueValueType, BdTreeBackend>;
private:
    IndexCache mCache;
    bdtree::map<NonUniqueKeyType, NonUniqueValueType, BdTreeBackend> mMap;
public:
    NonUniqueBdTree(const commitmanager::SnapshotDescriptor& snapshot, BdTreeBackend& backend, bool doInit = false);
    bool insert(const KeyType& key, const ValueType& value) override;
    bool erase(const KeyType& key) override;
};

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
    using Cache = Iterator::Cache;
private:
    std::vector<store::Schema::id_t> mFields;
    BdTreeBackend mBackend;
    const commitmanager::SnapshotDescriptor& mSnapshot;
    std::unique_ptr<BdTree> mBdTree;
    Cache mCache;
public:
    IndexWrapper(
            bool uniqueIndex,
            const std::vector<store::Schema::id_t>& fieds,
            BdTreeBackend&& backend,
            const commitmanager::SnapshotDescriptor& snapshot,
            bool init = false);
public: // Access
    Iterator lower_bound(const std::vector<tell::db::Field>& key);
public: // Modifications
    void insert(key_t key, const Tuple& tuple);
    void update(key_t key, const Tuple& old, const Tuple& next);
    void remove(key_t key, const Tuple& tuple);
public: // commit helper functions
    crossbow::string undoLog() const;
    void writeBack();
private:
    std::vector<Field> keyOf(const Tuple& tuple);
};

class Indexes {
public: // types
    using Index = typename IndexWrapper::Index;
    using IndexDescriptor = store::Schema::IndexMap::mapped_type;
    struct IndexTables {
        IndexDescriptor fields;
        TableData ptrTable;
        TableData nodeTable;
    };
private: // members
    std::shared_ptr<store::Table> mCounterTable;
    std::unordered_map<table_t, std::unordered_map<crossbow::string, IndexTables*>> mIndexes;
    // TODO: Caching is turned off for now - later we need to redesign cache handling
public:
    Indexes(store::ClientHandle& handle);
public:
    std::unordered_map<crossbow::string, IndexWrapper> openIndexes(
            const commitmanager::SnapshotDescriptor& snapshot,
            store::ClientHandle& handle,
            const store::Table& table);
    std::unordered_map<crossbow::string, IndexWrapper> createIndexes(
            const commitmanager::SnapshotDescriptor& snapshot,
            store::ClientHandle& handle,
            const store::Table& table);
};

} // namespace impl
} // namespace db
} // namespace tell
