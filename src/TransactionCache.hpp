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
#include <telldb/Types.hpp>
#include <telldb/Transaction.hpp>
#include <tellstore/Table.hpp>
#include <crossbow/string.hpp>

#include <unordered_map>

namespace tell {
namespace db {
namespace impl {
struct TellDBContext;
} // namespace impl

class TableCache;
class TransactionCache {
    friend class Future<table_t>;
    std::unordered_map<table_t, TableCache*> mTables;
    impl::TellDBContext& context;
    tell::store::ClientTransaction& mTransaction;
public:
    TransactionCache(impl::TellDBContext& context, tell::store::ClientTransaction& transaction);
    ~TransactionCache();
public:
    Future<table_t> openTable(tell::store::ClientHandle& handle, const crossbow::string& name);
    Future<Tuple> get(table_t table, key_t key);
private:
    table_t addTable(const tell::store::Table& table);
    table_t addTable(const crossbow::string& name, const tell::store::Table& table);
};

} // namespace db
} // namespace tell

