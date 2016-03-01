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
#include <telldb/TellDB.hpp>

namespace tell {
namespace db {

using AggregationType = store::AggregationType;

class Conjunct {
public: // types
    using Predicate = std::tuple<tell::store::PredicateType, Tuple::id_t, Field>;
    using PredicateList = std::vector<Predicate>;
private:
    PredicateList mPredicates;
public:
    Conjunct(Predicate&& predicate) {
        mPredicates.emplace_back(std::move(predicate));
    }

    Conjunct(const Predicate& predicate) {
        mPredicates.push_back(predicate);
    }

    Conjunct& operator|| (const Predicate& predicate) {
        mPredicates.push_back(predicate);
        return *this;
    }

    Conjunct& operator|| (const Conjunct& c) {
        for (auto& p : c.mPredicates) {
            mPredicates.emplace_back(p);
        }
        return *this;
    }

    const PredicateList& predicates() const {
        return mPredicates;
    }
};

class ScanQuery {
    friend class Transaction;
private: // members
    table_t mTable;
    bool mDoPartition = false;
    uint16_t mKeyShift = 0;
    uint32_t mPartitionKey= 0;
    uint32_t mPartitionValue = 0;
    tell::store::ScanQueryType mQueryType;
    std::vector<Tuple::id_t> mProjectedFields;
    std::vector<std::pair<AggregationType, Tuple::id_t>> mAggregations;
    std::vector<Conjunct> mConjuncts;
protected:
    ScanQuery(table_t table);
    ScanQuery(table_t table, const std::vector<Tuple::id_t>& projectedFields);
    ScanQuery(table_t table, const std::vector<std::pair<AggregationType, Tuple::id_t>>& aggregations);
public:
    void setPartition(uint16_t keyShift, uint32_t partitionKey, uint32_t partitionValue);
    ScanQuery& operator&& (const Conjunct& conjunct) {
        mConjuncts.push_back(conjunct);
        return *this;
    }
    ScanQuery& operator&& (Conjunct&& conjunct) {
        mConjuncts.emplace_back(conjunct);
        return *this;
    }
public: // Access info
    table_t table() const { return mTable; }
    store::ScanQueryType queryType() const { return mQueryType; }
private: // Serialization
    void serializeQuery(std::unique_ptr<char[]>& result, uint32_t& size) const;
    void serializeSelection(std::unique_ptr<char[]>& result, uint32_t& size) const;
};

class FullScan : public ScanQuery {
public:
    FullScan(table_t table);
};

class Projection : public ScanQuery {
public:
    Projection(table_t table, const std::vector<Tuple::id_t>& fields);
};

class Aggregation : public ScanQuery {
public:
    Aggregation(table_t table, const std::vector<std::pair<AggregationType, Tuple::id_t>>& aggregation);
};

} // namespace db
} // namespace tell
