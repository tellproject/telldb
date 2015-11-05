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
#include "Indexes.hpp"
#include "FieldSerialize.hpp"
#include <telldb/Exceptions.hpp>
#include <exception>

using namespace tell::db;
using namespace tell::db::impl;

namespace bdtree {

template<>
struct null_key<tell::db::impl::UniqueKeyType> {
    static tell::db::impl::UniqueKeyType value() {
        return std::make_tuple(std::vector<Field>{},
                std::numeric_limits<uint64_t>::max());
    }
};

template<>
struct null_key<tell::db::impl::NonUniqueKeyType> {
    static tell::db::impl::NonUniqueKeyType value() {
        return std::make_tuple(std::vector<Field>{},
                std::numeric_limits<uint64_t>::max(),
                tell::db::key_t{0});
    }
};

} // namespace bdtree


namespace bdtree {

template class map<tell::db::impl::UniqueKeyType, tell::db::impl::UniqueValueType, tell::db::BdTreeBackend>;
template class map<tell::db::impl::NonUniqueKeyType, tell::db::impl::NonUniqueValueType, tell::db::BdTreeBackend>;

} // namespace bdtree

namespace tell {
namespace db {

Iterator::Iterator(std::unique_ptr<IteratorImpl> impl)
    : mImpl(std::move(impl))
{}

Iterator::Iterator(const Iterator& other)
    : mImpl(other.mImpl->copy())
{
}

Iterator::Iterator(Iterator&&) = default;

Iterator& Iterator::operator=(const Iterator& other) {
    mImpl.reset(other.mImpl->copy());
    return *this;
}

Iterator::~Iterator() {}

bool Iterator::done() const {
    return mImpl->done();
}

void Iterator::next() {
    mImpl->next();
}

const KeyType& Iterator::key() const {
    return mImpl->key();
}

ValueType Iterator::value() const {
    return mImpl->value();
}

IteratorDirection Iterator::direction() const {
    return mImpl->direction();
}

namespace impl {

IteratorImpl::~IteratorImpl() {}
BdTree::~BdTree() {}

UniqueBdTree::UniqueBdTree(const commitmanager::SnapshotDescriptor& snapshot, BdTreeBackend& backend, bool doInit)
        : BdTree(snapshot)
        , mMap(backend, mCache, mSnapshot.version(), doInit)
    {}

NonUniqueBdTree::NonUniqueBdTree(const commitmanager::SnapshotDescriptor& snapshot, BdTreeBackend& backend, bool doInit)
        : BdTree(snapshot)
        , mMap(backend, mCache, mSnapshot.version(), doInit)
    {}

bool UniqueBdTree::insert(const KeyType& key, const ValueType& value) {
    return mMap.insert(std::make_tuple(key, std::numeric_limits<uint64_t>::max()), value);
}

bool UniqueBdTree::erase(const KeyType& key, const ValueType& value) {
    if (!mMap.insert(std::make_tuple(key, mSnapshot.version()), value)) {
        return false;
    }
    if (!mMap.erase(std::make_tuple(key, std::numeric_limits<uint64_t>::max()))) {
        mMap.erase(std::make_tuple(key, mSnapshot.version()));
        return false;
    }
    return true;
}

void UniqueBdTree::revertInsert(const KeyType& key, ValueType) {
    mMap.erase(std::make_tuple(key, std::numeric_limits<uint64_t>::max()));
}

void UniqueBdTree::revertErase(const KeyType& key, ValueType value) {
    mMap.insert(std::make_tuple(key, std::numeric_limits<uint64_t>::max()), value);
    mMap.erase(std::make_tuple(key, mSnapshot.version()));
}

auto UniqueBdTree::lower_bound(const KeyType& key) -> Iterator {
    return Iterator(std::unique_ptr<IteratorImpl>(ForwardIterator<Map>::create(mSnapshot,
                    mMap.find(std::make_tuple(key, 0)), mMap)));
}

auto UniqueBdTree::reverse_lower_bound(const KeyType& key) -> Iterator {
    auto end = mMap.end();
    auto iter = mMap.find(std::make_tuple(key, std::numeric_limits<uint64_t>::max()));
    while (iter != end && std::get<0>(iter->first) > key) {
        --iter;
    }
    return std::unique_ptr<IteratorImpl>(BackwardIterator<Map>::create(mSnapshot, iter, mMap));
}

bool NonUniqueBdTree::insert(const KeyType& key, const ValueType& value) {
    return mMap.insert(std::make_tuple(key, std::numeric_limits<uint64_t>::max(), value));
}

bool NonUniqueBdTree::erase(const KeyType& key, const ValueType& value) {
    if (!mMap.insert(std::make_tuple(key, mSnapshot.version(), value))) {
        return false;
    }
    if (!mMap.erase(std::make_tuple(key, std::numeric_limits<uint64_t>::max(), value))) {
        mMap.erase(std::make_tuple(key, mSnapshot.version(), value));
        return false;
    }
    return true;
}

void NonUniqueBdTree::revertInsert(const KeyType& key, ValueType value) {
    mMap.erase(std::make_tuple(key, std::numeric_limits<uint64_t>::max(), value));
}

void NonUniqueBdTree::revertErase(const KeyType& key, ValueType value) {
    mMap.insert(std::make_tuple(key, std::numeric_limits<uint64_t>::max(), value));
    mMap.erase(std::make_tuple(key, mSnapshot.version(), value));
}

auto NonUniqueBdTree::lower_bound(const KeyType& key) -> Iterator {
    return std::unique_ptr<IteratorImpl>(ForwardIterator<Map>::create(mSnapshot, mMap.find(std::make_tuple(key, 0, key_t{0})), mMap));
}

auto NonUniqueBdTree::reverse_lower_bound(const KeyType& key) -> Iterator {
    auto end = mMap.end();
    auto iter = mMap.find(std::make_tuple(key, std::numeric_limits<uint64_t>::max(), key_t{std::numeric_limits<uint64_t>::max()}));
    while (iter != end && std::get<0>(*iter) > key) {
        --iter;
    }
    return std::unique_ptr<IteratorImpl>(BackwardIterator<Map>::create(mSnapshot, iter, mMap));
}

using namespace commitmanager;

IndexWrapper::IndexWrapper(
        const crossbow::string& name,
        bool uniqueIndex,
        const std::vector<store::Schema::id_t>& fields,
        BdTreeBackend&& backend,
        const SnapshotDescriptor& snapshot,
        bool init)
    : mName(name)
    , mFields(fields)
    , mBackend(new BdTreeBackend(std::move(backend)))
    , mSnapshot(snapshot)
    , mBdTree(uniqueIndex ?
            static_cast<BdTree*>(new UniqueBdTree(mSnapshot, *mBackend, init)) :
            static_cast<BdTree*>(new NonUniqueBdTree(mSnapshot, *mBackend, init)))
{
}

void IndexWrapper::insert(key_t k, const Tuple& tuple) {
    mCache.emplace(keyOf(tuple), std::make_tuple(IndexOperation::Insert, k, false));
}

void IndexWrapper::update(key_t key, const Tuple& old, const Tuple& next) {
    auto oldKey = keyOf(old);
    auto newKey = keyOf(next);
    if (oldKey != newKey) {
        mCache.emplace(
                keyOf(old),
                std::make_tuple(IndexOperation::Delete, key, false));
        mCache.emplace(
                keyOf(next),
                std::make_tuple(IndexOperation::Insert, key, false));
    }
}

void IndexWrapper::remove(key_t key, const Tuple& tuple) {
    mCache.emplace(keyOf(tuple), std::make_tuple(IndexOperation::Delete, key, false));
}

auto IndexWrapper::lower_bound(const KeyType& key) -> tell::db::Iterator {
    std::unique_ptr<CacheIteratorImpl> cIter(new BdTree::StdIter<Cache::iterator>(
                IteratorDirection::Forward,
                mCache.lower_bound(key),
                mCache.end()));
    return std::unique_ptr<IteratorImpl>(new Iterator(IteratorDirection::Forward,
                mBdTree->lower_bound(key), std::move(cIter)));
}

auto IndexWrapper::reverse_lower_bound(const KeyType& key) -> tell::db::Iterator {
    auto iter = mCache.lower_bound(key);
    auto rIter = std::reverse_iterator<Cache::iterator>(iter);
    if (iter == mCache.end()) {
        rIter = mCache.rend();
    }
    std::unique_ptr<CacheIteratorImpl> cIter(
            new BdTree::StdIter<decltype(rIter)>(
                IteratorDirection::Backward,
                rIter,
                mCache.rend()));
    return std::unique_ptr<IteratorImpl>(new Iterator(IteratorDirection::Backward,
                mBdTree->reverse_lower_bound(key), std::move(cIter)));
}

void IndexWrapper::writeBack() {
    for (auto& op : mCache) {
        bool res;
        if (std::get<2>(op.second)) continue;
        switch (std::get<0>(op.second)) {
        case IndexOperation::Insert:
            res = mBdTree->insert(op.first, std::get<1>(op.second));
            break;
        case IndexOperation::Delete:
            res = mBdTree->erase(op.first, std::get<1>(op.second));
            break;
        }
        if (!res) {
            throw IndexConflict(std::get<1>(op.second), mName);
        }
        std::get<2>(op.second) = true;
    }
}

void IndexWrapper::undo() {
    for (auto& op : mCache) {
        if (!std::get<2>(op.second)) continue;
        switch (std::get<0>(op.second)) {
        case IndexOperation::Insert:
            mBdTree->revertInsert(op.first, std::get<1>(op.second));
            break;
        case IndexOperation::Delete:
            mBdTree->revertErase(op.first, std::get<1>(op.second));
            break;
        }
    }
}

std::vector<Field> IndexWrapper::keyOf(const Tuple& tuple) {
    std::vector<Field> key;
    key.reserve(mFields.size());
    for (auto f : mFields) {
        key.emplace_back(tuple[f]);
    }
    return key;
}

Indexes::IndexTables::~IndexTables() = default;

Indexes::Indexes(store::ClientHandle& handle) {
    auto tableRes = handle.getTable("__counter");
    if (tableRes->error()) {
        mCounterTable = RemoteCounter::createTable(handle, "__counter");
    } else {
        mCounterTable = std::make_shared<store::Table>(tableRes->get());
    }
}

std::unordered_map<crossbow::string, IndexWrapper>
Indexes::openIndexes(const SnapshotDescriptor& snapshot, store::ClientHandle& handle, const store::Table& table) {
    std::unordered_map<crossbow::string, IndexWrapper> res;
    auto iter = mIndexes.find(table_t{table.tableId()});
    if (iter != mIndexes.end()) {
        for (auto& idx : iter->second) {
            res.emplace(idx.first,
                    IndexWrapper(
                        idx.first,
                        idx.second->fields.first, // TODO: Unique
                        idx.second->fields.second,
                        BdTreeBackend(
                            handle,
                            idx.second->ptrTable,
                            idx.second->nodeTable),
                        snapshot,
                        false));
        }
        return res;
    }
    const auto& indexes = table.record().schema().indexes();
    std::vector<std::tuple<crossbow::string,
        const IndexDescriptor*,
        std::shared_ptr<store::GetTableResponse>,
        std::shared_ptr<store::GetTableResponse>>> responses;
    for (const auto& idx : indexes) {
        crossbow::string nodeTableName = "__index_nodes_" + idx.first;
        crossbow::string ptrTableName = "__index_ptrs_" + idx.first;
        responses.emplace_back(std::make_tuple(idx.first,
                    &idx.second,
                    handle.getTable(nodeTableName),
                    handle.getTable(ptrTableName)));
    }
    std::unordered_map<crossbow::string, IndexTables*> indexMap;
    for (auto it = responses.rbegin(); it != responses.rend(); ++it) {
        {
            const auto& ec = std::get<2>(*it)->error();
            if (ec) {
                const auto& str = ec.message();
                throw OpenTableException(crossbow::string(str.c_str(), str.size()));
            }
        }
        {
            const auto& ec = std::get<2>(*it)->error();
            if (ec) {
                const auto& str = ec.message();
                throw OpenTableException(crossbow::string(str.c_str(), str.size()));
            }
        }
        auto insRes = indexMap.emplace(std::get<0>(*it),
                new IndexTables{
                    *std::get<1>(*it),
                    TableData(std::get<3>(*it)->get(), mCounterTable),
                    TableData(std::get<2>(*it)->get(), mCounterTable)
                });
        res.emplace(std::get<0>(*it),
                IndexWrapper(
                    std::get<0>(*it),
                    std::get<1>(*it)->first, // TODO: Unique
                    std::get<1>(*it)->second,
                    BdTreeBackend(
                        handle,
                        insRes.first->second->ptrTable,
                        insRes.first->second->nodeTable),
                    snapshot,
                    false));
    }
    mIndexes.emplace(table_t{table.tableId()}, std::move(indexMap));
    return res;
}

std::unordered_map<crossbow::string, IndexWrapper>
Indexes::createIndexes(const SnapshotDescriptor& snapshot, store::ClientHandle& handle, const store::Table& table) {
    std::unordered_map<crossbow::string, IndexWrapper> res;
    const auto& indexes = table.record().schema().indexes();
    std::unordered_map<crossbow::string, IndexTables*> indexMap;
    for (const auto& idx : indexes) {
        crossbow::string nodeTableName = "__index_nodes_" + idx.first;
        crossbow::string ptrTableName = "__index_ptrs_" + idx.first;
        auto insRes = indexMap.emplace(idx.first,
                new IndexTables{idx.second,
                                TableData(BdTreePointerTable::createTable(handle, ptrTableName), mCounterTable),
                                TableData(BdTreeNodeTable::createTable(handle, nodeTableName), mCounterTable)});
        res.emplace(idx.first,
                IndexWrapper(
                    idx.first,
                    idx.second.first, // TODO: Unique
                    idx.second.second,
                    BdTreeBackend(
                        handle,
                        insRes.first->second->ptrTable,
                        insRes.first->second->nodeTable),
                    snapshot,
                    true));
    }
    mIndexes.emplace(table_t{table.tableId()}, indexMap);
    return res;
}

} // namespace impl
} // namespace db
} // namespace tell


