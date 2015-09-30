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
#include "Tuple.hpp"
#include "Types.hpp"

#include <tellstore/TransactionType.hpp>

namespace tell {
namespace store {

class ClientTransaction;
class ClientHandle;
class GetTableResponse;

} // namespace store

namespace db {

class TellDBContext;
class TransactionCache;

template<class T>
class Future;

template<>
class Future<table_t> {
    friend class TransactionCache;
    std::shared_ptr<tell::store::GetTableResponse> resp;
    TransactionCache& cache;
    table_t result;
    Future(std::shared_ptr<tell::store::GetTableResponse>&& resp, TransactionCache& cache);
public:
    bool valid() const;
    bool wait() const;
    table_t get();
};

template<>
class Future<Tuple> {
};

extern template class Future<table_t>;
extern template class Future<Tuple>;

class Transaction {
    tell::store::ClientHandle& mHandle;
    tell::store::ClientTransaction& mTx;
    TellDBContext& mContext;
    tell::store::TransactionType mType;
    std::unique_ptr<TransactionCache> mCache;
public:
    Transaction(tell::store::ClientHandle& handle,
            tell::store::ClientTransaction& tx,
            TellDBContext& context,
            tell::store::TransactionType type);
public: // table operation
    Future<table_t> openTable(const crossbow::string& name);
public: // read-write operations
    Future<Tuple> get(table_t tableId, key_t key);
public: // finish
    /**
     * @brief Aborts the current transaction
     *
     * This will abort the current transaction and will
     * delete the state of the current transaction. After
     * a call to rollback it is illegal to call any functions
     * on the transaction object again.
     */
    void rollback();
    /**
     * @brief Commits the transaction
     *
     * This tries to commit the transaction. A commit
     * might fail if there is a write-write conflict
     * with another transaction.
     *
     * @returns true iff the transaction committed successfully
     */
    bool commit();
};

} // namespace db
} // namespace tell

