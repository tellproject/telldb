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
#include <telldb/ScanQuery.hpp>
#include <telldb/Exceptions.hpp>

namespace tell {
namespace db {

ScanQuery::ScanQuery(table_t table)
    : mTable(table)
    , mQueryType(tell::store::ScanQueryType::FULL)
{}
ScanQuery::ScanQuery(table_t table, const std::vector<Tuple::id_t>& fields)
    : mTable(table)
    , mQueryType(tell::store::ScanQueryType::PROJECTION)
    , mProjectedFields(fields)
{}

ScanQuery::ScanQuery(table_t table, const std::vector<std::pair<AggregationType, Tuple::id_t>>& aggregations)
    : mTable(table)
    , mQueryType(tell::store::ScanQueryType::AGGREGATION)
    , mAggregations(aggregations)
{}

void ScanQuery::verify(const store::Schema& schema) const {
    for (auto& c : mConjuncts) {
        for (auto& p : c.predicates()) {
            auto& field = schema[std::get<1>(p)];
            if (field.type() != std::get<2>(p).type()) {
                throw WrongFieldType(field.name());
            }
        }
    }
    for (auto& a : mAggregations) {
        switch (schema[a.second].type()) {
        case store::FieldType::BLOB:
        case store::FieldType::TEXT:
            throw WrongFieldType("Can not aggregate over string and blob types");
        default:
            break;
        }
    }
}

void ScanQuery::setPartition(uint16_t keyShift, uint32_t partitionKey, uint32_t partitionValue) {
    mDoPartition = true;
    mKeyShift = keyShift;
    mPartitionKey = partitionKey;
    mPartitionValue = partitionValue;
}

void ScanQuery::serializeQuery(std::unique_ptr<char[]>& result, uint32_t& size) const {
    if (mQueryType == tell::store::ScanQueryType::FULL) {
        size = 0;
        result.reset(nullptr);
    } else if (mQueryType == tell::store::ScanQueryType::PROJECTION) {
        size = mProjectedFields.size() * 2;
        result.reset(new char[size]);
        crossbow::buffer_writer writer(result.get(), size);
        auto projectedFields = mProjectedFields;
        std::sort(projectedFields.begin(), projectedFields.end());
        for (auto p : projectedFields) {
            writer.write(p);
        }
    } else {
        size = mAggregations.size() * 4;
        result.reset(new char[size]);
        crossbow::buffer_writer writer(result.get(), size);
        for (auto& aggr : mAggregations) {
            writer.write(aggr.second);
            writer.write(aggr.first);
        }
    }
}

void ScanQuery::serializeSelection(std::unique_ptr<char[]>& result, uint32_t& size) const {
    size = 16;
    std::unordered_map<Tuple::id_t, std::vector<std::pair<uint8_t, Conjunct::Predicate>>> predicateMap;
    uint8_t conjunctNum = 0;
    for (auto& conjunct : mConjuncts) {
        for (auto& predicate : conjunct.predicates()) {
            auto ins = predicateMap.emplace(std::get<1>(predicate), std::vector<std::pair<uint8_t, Conjunct::Predicate>>());
            if (ins.second) size += 8; // 8 bytes for the column
            size += 8; // 8 bytes at least for the predicate
            ins.first->second.emplace_back(conjunctNum, predicate);
            auto& field = std::get<2>(predicate);
            switch (field.type()) {
            case store::FieldType::SMALLINT:
            case store::FieldType::INT:
            case store::FieldType::FLOAT:
                // 4 byte types can be added without overhead
                break;
            case store::FieldType::BIGINT:
            case store::FieldType::DOUBLE:
                // another 8 bytes
                size += 8;
                break;
            case store::FieldType::BLOB:
            case store::FieldType::TEXT:
                size += field.value<crossbow::string>().size();
                size += (size % 8 == 0 ? 0 : 8 - (size % 8));
                break;
            case store::FieldType::NULLTYPE:
            case store::FieldType::NOTYPE:
                LOG_ERROR("Can not serialize this type");
                throw std::runtime_error("serialization error");
            }
        }
        ++conjunctNum;
    }
    result.reset(new char[size]);
    crossbow::buffer_writer w(result.get(), size);
    w.write(uint32_t(predicateMap.size()));
    w.write(uint16_t(mConjuncts.size()));
    w.write(mKeyShift);
    w.write(mPartitionKey);
    w.write(mPartitionValue);
    for (auto& co : predicateMap) {
        w.write(co.first);
        w.write(uint16_t(co.second.size()));
        w.set(0, sizeof(uint32_t));
        for (auto& p : co.second) {
            auto& pred = p.second;
            w.write(std::get<0>(pred));
            w.write(p.first);
            auto& field = std::get<2>(pred);
            switch (field.type()) {
            case store::FieldType::SMALLINT:
                w.write(field.value<int16_t>());
                w.set(0, 4);
                break;
            case store::FieldType::INT:
                w.set(0, 2);
                w.write(field.value<int32_t>());
                break;
            case store::FieldType::FLOAT:
                w.set(0, 2);
                w.write(field.value<float>());
                break;
            case store::FieldType::BIGINT:
                w.set(0, 4);
                w.write(field.value<int64_t>());
                break;
            case store::FieldType::DOUBLE:
                w.set(0, 4);
                w.write(field.value<double>());
                break;
            case store::FieldType::BLOB:
            case store::FieldType::TEXT:
                w.set(0, 2);
                {
                    auto& str = field.value<crossbow::string>();
                    auto strLen = str.size();
                    w.write(uint32_t(strLen));
                    w.write(str.data(), strLen);
                    if (strLen % 8 != 0) {
                        w.set(0, 8 - (strLen % 8));
                    }
                }
                break;
            case store::FieldType::NULLTYPE:
            case store::FieldType::NOTYPE:
                LOG_ERROR("Can not serialize this type");
                throw std::runtime_error("serialization error");
            }
        }
    }
}

FullScan::FullScan(table_t table)
    : ScanQuery(table)
{}

Projection::Projection(table_t table, const std::vector<Tuple::id_t>& fields)
    : ScanQuery(table, fields)
{
}

Aggregation::Aggregation(table_t table, const std::vector<std::pair<AggregationType, Tuple::id_t>>& aggregations)
    : ScanQuery(table, aggregations)
{}

} // namespace db
} // namespace tell

