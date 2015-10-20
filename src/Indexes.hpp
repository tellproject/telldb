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
#include <telldb/TellDB.hpp>
#include <bdtree/bdtree.h>
#include <telldb/Tuple.hpp>
#include <telldb/Iterator.hpp>
#include <commitmanager/SnapshotDescriptor.hpp>

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
 */
using UniqueKeyType = std::tuple<KeyType, uint64_t>;
using NonUniqueKeyType = decltype(std::tuple_cat(std::declval<UniqueKeyType>(), std::declval<std::tuple<key_t>>()));
/**
 * The value for an index map is simply the key of of the tuple
 */
using UniqueValueType = ValueType;
// something small - we won't use it
using NonUniqueValueType = bdtree::empty_t;

/**
 * Used for index caching.
 */
enum class IndexOperation : uint8_t {
    Insert, Delete
};

using Cache = std::multimap<KeyType, std::tuple<IndexOperation, ValueType, bool>>;

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

using UniqueMap = bdtree::map<UniqueKeyType, UniqueValueType, BdTreeBackend>;
using NonUniqueMap = bdtree::map<NonUniqueKeyType, NonUniqueValueType, BdTreeBackend>;

template<class Map>
struct KeyOf;
template<class Map>
struct ValueOf;
template<class Map>
struct ValidTo;

template<>
struct KeyOf<UniqueMap> {
    using type = UniqueKeyType;

    const KeyType& operator() (const std::pair<UniqueKeyType, UniqueValueType>& p) const {
        return std::get<0>(p.first);
    }

    const UniqueKeyType& mapKey(const std::pair<UniqueKeyType, UniqueValueType>& p) const {
        return p.first;
    }
};

template<>
struct ValueOf<UniqueMap> {
    const ValueType& operator() (const std::pair<UniqueKeyType, UniqueValueType>& p) const {
        return p.second;
    }
};

template<>
struct ValidTo<UniqueMap> {
    uint64_t operator() (const std::pair<UniqueKeyType, UniqueValueType>& p) const {
        return std::get<1>(p.first);
    }
};

template<>
struct KeyOf<NonUniqueMap> {
    using type = NonUniqueKeyType;

    const KeyType& operator() (const NonUniqueKeyType& p) const {
        return std::get<0>(p);
    }

    const NonUniqueKeyType& mapKey(const NonUniqueKeyType& k) const {
        return k;
    }
};

template<>
struct ValueOf<NonUniqueMap> {
    const ValueType& operator() (const NonUniqueKeyType& p) const {
        return std::get<2>(p);
    }
};

template<>
struct ValidTo<NonUniqueMap> {
    uint64_t operator() (const NonUniqueKeyType& p) const {
        return std::get<1>(p);
    }
};

class IteratorImpl {
public:
    virtual ~IteratorImpl();
    virtual bool done() const = 0;
    virtual void next() = 0;
    virtual const KeyType& key() const = 0;
    virtual ValueType value() const = 0;
    virtual IteratorDirection direction() const = 0;
    virtual void init() = 0;
    virtual IteratorImpl* copy() const = 0;
};

class CacheIteratorImpl : public IteratorImpl {
public:
    virtual IndexOperation operation() const = 0;
};


class BdTree {
public:

    class CacheIterator {
        std::unique_ptr<CacheIteratorImpl> mImpl;
    public:
        CacheIterator(std::unique_ptr<CacheIteratorImpl> impl) : mImpl(std::move(impl)) {}
        CacheIterator(const CacheIterator& other)
            : mImpl(dynamic_cast<CacheIteratorImpl*>(other.mImpl->copy()))
        {}
        bool done() const {
            return mImpl->done();
        }
        void next() {
            mImpl->next();
        }
        const KeyType& key() const {
            return mImpl->key();
        }
        ValueType value() const {
            return mImpl->value();
        }
        IteratorDirection direction() const {
            return mImpl->direction();
        }
        IndexOperation operation() const {
            return mImpl->operation();
        }
    };

    template<class Iter>
    class StdIter : public CacheIteratorImpl {
    public: // types
        using iterator = Iter;
    private:
        IteratorDirection mDirection;
        iterator iter;
        iterator end;
    public:
        StdIter(IteratorDirection direction, iterator begin, iterator end)
            : mDirection(direction)
            , iter(begin)
            , end(end)
        {}
        virtual bool done() const override {
            return iter == end;
        }
        virtual void next() override {
            ++iter;
        }
        virtual const KeyType& key() const override {
            return iter->first;
        }
        virtual ValueType value() const override {
            return std::get<1>(iter->second);
        }
        virtual IndexOperation operation() const override {
            return std::get<0>(iter->second);
        }
        virtual IteratorDirection direction() const override {
            return mDirection;
        }

        virtual void init() override {}
        virtual IteratorImpl* copy() const override {
            return new StdIter(mDirection, iter, end);
        }
    };

    template<class Map>
    class TreeCleaner {
    public:
        using key_type = typename KeyOf<Map>::type;
    private:
        Map& map;
        std::vector<key_type> garbage;
    public:
        TreeCleaner(Map& map) : map(map) {}
        ~TreeCleaner() {
            for (const auto& g : garbage) {
                map.erase(g);
            }
        }
        void add(const key_type& k) {
            garbage.push_back(k);
        }
    };
    template<class Map>
    class BaseIterator : public IteratorImpl {
    public:
        using Direction = IteratorDirection;
    protected:
        const commitmanager::SnapshotDescriptor& mSnapshot;
        KeyOf<Map> mKeyOf;
        ValueOf<Map> mValueOf;
        ValidTo<Map> mValidTo;
        typename Map::iterator mapIter;
        typename Map::iterator mapEnd;
        std::shared_ptr<TreeCleaner<Map>> cleaner;
    public:
        BaseIterator(const commitmanager::SnapshotDescriptor& snapshot, typename Map::iterator iter, Map& map)
            : mSnapshot(snapshot)
            , mapIter(iter)
            , cleaner(std::make_shared<TreeCleaner<Map>>(map))
        {
        }
        virtual void init() override {
            while (this->mapIter != this->mapEnd) {
                if (this->validTo() < this->mSnapshot.lowestActiveVersion()) {
                    this->cleaner->add(mKeyOf.mapKey(*this->mapIter));
                } else {
                    break;
                }
                forward();
            }
        }
    public:
        virtual bool done() const  override {
            return mapIter == mapEnd;
        }
        virtual const KeyType& key() const override {
            return mKeyOf(*mapIter);
        }
        virtual ValueType value() const override {
            return mValueOf(*mapIter);
        }
        virtual void next() override {
            while (this->mapIter != this->mapEnd) {
                this->forward();
                if (this->validTo() < this->mSnapshot.lowestActiveVersion()) {
                    this->cleaner->add(mKeyOf.mapKey(*this->mapIter));
                } else {
                    break;
                }
            }
        }
    protected:
        uint64_t validTo() const {
            return mValidTo(*mapIter);
        }
        virtual void forward() = 0;
    };

    template<class Map>
    class ForwardIterator : public BaseIterator<Map> {
        ForwardIterator(const commitmanager::SnapshotDescriptor& snapshot, typename Map::iterator iter , Map& map)
            : BaseIterator<Map>(snapshot, iter, map) {}
    public:
        static ForwardIterator* create(const commitmanager::SnapshotDescriptor& snapshot, typename Map::iterator iter , Map& map) {
            auto res = new ForwardIterator(snapshot, iter, map);
            res->init();
            return res;
        }

        virtual IteratorDirection direction() const override {
            return IteratorDirection::Forward;
        }
        virtual IteratorImpl* copy() const override {
            return new ForwardIterator<Map>(*this);
        }
    protected:
        virtual void forward() override {
            ++this->mapIter;
        }
    };
    template<class Map>
    class BackwardIterator : public BaseIterator<Map> {
        BackwardIterator(const commitmanager::SnapshotDescriptor& snapshot, typename Map::iterator iter, Map& map)
            : BaseIterator<Map>(snapshot, iter, map) {}
    public:
        static BackwardIterator* create(const commitmanager::SnapshotDescriptor& snapshot, typename Map::iterator iter, Map& map) {
           auto res = new BackwardIterator(snapshot, iter, map); 
           res->init();
           return res;
        }
        virtual IteratorDirection direction() const override {
            return IteratorDirection::Backward;
        }
        virtual IteratorImpl* copy() const override {
            return new BackwardIterator<Map>(*this);
        }
    protected:
        virtual void forward() override {
            --this->mapIter;
        }
    };
protected:
    const commitmanager::SnapshotDescriptor& mSnapshot;
public:
    BdTree(const commitmanager::SnapshotDescriptor& snapshot) : mSnapshot(snapshot) {}
    virtual ~BdTree();
    virtual bool insert(const KeyType& key, const ValueType& value) = 0;
    virtual void revertInsert(const KeyType& key, ValueType value) = 0;
    virtual bool erase(const KeyType& key, const ValueType& value) = 0;
    virtual void revertErase(const KeyType& key, ValueType value) = 0;
    virtual Iterator lower_bound(const KeyType& key) = 0;
    virtual Iterator reverse_lower_bound(const KeyType& key) = 0;
};

class UniqueBdTree : public BdTree {
    using IndexCache = bdtree::logical_table_cache<UniqueKeyType, UniqueValueType, BdTreeBackend>;
    using Map = bdtree::map<UniqueKeyType, UniqueValueType, BdTreeBackend>;
private:
    IndexCache mCache;
    Map mMap;
private: // Types
public:
    UniqueBdTree(const commitmanager::SnapshotDescriptor& snapshot, BdTreeBackend& backend, bool doInit = false);
    bool insert(const KeyType& key, const ValueType& value) override;
    bool erase(const KeyType& key, const ValueType& value) override;
    virtual void revertInsert(const KeyType& key, ValueType value) override;
    virtual void revertErase(const KeyType& key, ValueType value) override;
    virtual Iterator lower_bound(const KeyType& key) override;
    virtual Iterator reverse_lower_bound(const KeyType& key) override;
};

class NonUniqueBdTree : public BdTree {
    using IndexCache = bdtree::logical_table_cache<NonUniqueKeyType, NonUniqueValueType, BdTreeBackend>;
    using Map = bdtree::map<NonUniqueKeyType, NonUniqueValueType, BdTreeBackend>;
private:
    IndexCache mCache;
    Map mMap;
public:
    NonUniqueBdTree(const commitmanager::SnapshotDescriptor& snapshot, BdTreeBackend& backend, bool doInit = false);
    bool insert(const KeyType& key, const ValueType& value) override;
    bool erase(const KeyType& key, const ValueType& value) override;
    virtual void revertInsert(const KeyType& key, ValueType value) override;
    virtual void revertErase(const KeyType& key, ValueType value) override;
    virtual Iterator lower_bound(const KeyType& key) override;
    virtual Iterator reverse_lower_bound(const KeyType& key) override;
};


class IndexWrapper {
public: // Types
    class Iterator : public IteratorImpl {
    public: // types
        using TreeIter = tell::db::Iterator;
        using CacheIter = BdTree::CacheIterator;
    private: // members
        IteratorDirection mDirection;
        TreeIter treeIter;
        CacheIter cacheIter;
        bool readFromCache = false;
        bool isNull(const std::vector<Field>& entry) const {
            for (const auto& f : entry) {
                if (!f.null()) {
                    return false;
                }
            }
            return true;
        }
        void doSet() {
            if (cacheIter.done()) { 
                // In this case we can just iterate over the tree
                readFromCache = false;
                return;
            }
            if (treeIter.done()) {
                assert(cacheIter.operation() == IndexOperation::Insert);
                readFromCache = true;
                return;
            }
            if (cacheIter.value() == treeIter.value()) {
                assert(cacheIter.operation() == IndexOperation::Delete);
                treeIter.next();
                cacheIter.next();
            }
            if (cacheIter.key() < treeIter.key()) {
                readFromCache = true;
            } else {
                readFromCache = false;
            }
        }
    public:
        template<class TI, class CI>
        Iterator(IteratorDirection direction, TI&& treeIter, CI&& cacheIter)
            : mDirection(direction)
            , treeIter(std::forward<TI>(treeIter))
            , cacheIter(std::forward<CI>(cacheIter)) 
        {
            doSet();
        }
    public:
        void init() override {}
        bool done() const override {
            return treeIter.done() && cacheIter.done();
        }
        void next() override {
            if (readFromCache) {
                cacheIter.next();
            } else {
                treeIter.next();
            }
            doSet();
        }
        const KeyType& key() const override {
            if (readFromCache) {
                return cacheIter.key();
            } else {
                return treeIter.key();
            }
        }
        ValueType value() const override {
            if (readFromCache) {
                return cacheIter.value();
            } else {
                return treeIter.value();
            }
        }
        IteratorDirection direction() const override {
            return mDirection;
        }
        IteratorImpl* copy() const override {
            return new Iterator(*this);
        }
    };
private:
    crossbow::string mName;
    std::vector<store::Schema::id_t> mFields;
    BdTreeBackend mBackend;
    const commitmanager::SnapshotDescriptor& mSnapshot;
    std::unique_ptr<BdTree> mBdTree;
    Cache mCache;
public:
    IndexWrapper(
            const crossbow::string& name,
            bool uniqueIndex,
            const std::vector<store::Schema::id_t>& fieds,
            BdTreeBackend&& backend,
            const commitmanager::SnapshotDescriptor& snapshot,
            bool init = false);
public: // Modifications
    void insert(key_t key, const Tuple& tuple);
    void update(key_t key, const Tuple& old, const Tuple& next);
    void remove(key_t key, const Tuple& tuple);
public: // find
    tell::db::Iterator lower_bound(const KeyType& key);
    tell::db::Iterator reverse_lower_bound(const KeyType& key);
public: // commit helper functions
    void writeBack();
    void undo();
    const Cache& cache() const {
        return mCache;
    }
    template<class C>
    void setCache(C&& c) {
        mCache = std::forward<C>(c);
    }
private:
    std::vector<Field> keyOf(const Tuple& tuple);
};

class Indexes {
public: // types
    using IndexDescriptor = store::Schema::IndexMap::mapped_type;
    struct IndexTables {
        IndexDescriptor fields;
        TableData ptrTable;
        TableData nodeTable;
    };
private: // members
    std::shared_ptr<store::Table> mCounterTable;
    std::unordered_map<table_t, std::unordered_map<crossbow::string, IndexTables*>> mIndexes;
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
