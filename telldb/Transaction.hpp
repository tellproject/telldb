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

/**
 * @mainpage TellDB - Running transactions on Tell
 *
 * TellDB is the main interface of Tell. If you are unsure what to use to implement
 * a transactional workload, this is probably it.
 *
 */

namespace tell {
namespace store {

class ClientTransaction;
class ClientHandle;
class GetTableResponse;
class GetResponse;

} // namespace store

namespace db {

namespace impl {
struct TellDBContext;
} // namespace impl
class TransactionCache;

#ifdef DOXYGEN

/**
 * @brief Objects returned from transactions that will hold the result.
 *
 * A future is an object that will eventually store the result to an asynchronous
 * call. Whenever the user calls asynchronous functions, the function might not
 * block but immediately return with a future. The future might already contain
 * the result (for example if the requested answer is found in the local cache).
 *
 * It is important to note that the function might block: crossbow infinio will
 * fill up buffers with requests and will block the call and send the buffers if
 * the request buffer runs full. In that case it will schedule another transaction
 * after sending the request. The client can check whether the result is available
 * by calling Future<T>::valid() which will return true iff a call to Future<T>::get
 * will bo non-blocking. Future<T>::valid() will never block.
 *
 * If the clients needs to read the result, it can call Future<T>::wait(). This
 * call will block iff Future<T>::valid() is false. It will return true if there
 * is a valid result, false otherwise - there might not be a result, for example
 * because there was an error (for example after a get request on a non-existant
 * key).
 */
template<class T>
class Future {
public:
    /**
     * @brief Check whether there is a valid result.
     * @return true iff Future holds a valid answer.
     *
     * This function will return true iff the client received an answer from the server.
     * This function can be used to check whether the request
     * has already been issued. If this function returns false, following calls to valid
     * will return false as well if there were no succeeding requests on a transaction.
     * Therefore this function must not be used to wait for an answer.
     */
    bool done() const;
    /**
     * @brief Waits for a result.
     * @return true if the result is valid
     *
     * This function blocks the fiber and waits for a response from the server. It will
     * return true if the result is valid, false otherwise.
     */
    bool wait() const;
    /**
     * @brief Get the response object.
     * @return The result of the request.
     * @exception std::system_error The result is invalid.
     * 
     * A call might block if the Future does not already hold the response. Otherwise it
     * will return immediately. Calling get several times is safe. If the response is
     * invalid (for example due to an error), a call to get will throw a system_error.
     */
    const T& get();
};
#else
template<class T>
class Future;

template<>
class Future<table_t> {
    friend class TransactionCache;
    std::shared_ptr<tell::store::GetTableResponse> resp;
    TransactionCache& cache;
    crossbow::string name;
    table_t result;
    Future(std::shared_ptr<tell::store::GetTableResponse>&& resp, TransactionCache& cache);
public:
    bool done() const;
    bool wait() const;
    table_t get();
};

class TableCache;
template<>
class Future<Tuple> {
    friend class TableCache;
    key_t key;
    const Tuple* result;
    TableCache* cache;
    std::shared_ptr<tell::store::GetResponse> response;
    Future(key_t key, const Tuple* result);
    Future(key_t key, TableCache* cache, std::shared_ptr<tell::store::GetResponse>&& response);
public:
    bool done() const;
    bool wait() const;
    const Tuple& get();
};

extern template class Future<table_t>;
extern template class Future<Tuple>;

#endif // DOXYGEN

class Transaction {
    tell::store::ClientHandle& mHandle;
    tell::store::ClientTransaction& mTx;
    impl::TellDBContext& mContext;
    tell::store::TransactionType mType;
    std::unique_ptr<TransactionCache> mCache;
public:
    Transaction(tell::store::ClientHandle& handle,
            tell::store::ClientTransaction& tx,
            impl::TellDBContext& context,
            tell::store::TransactionType type);
public: // table operation
    /**
     * @brief Opens the table, gets the schema and prepares the caches.
     * @return A future to the table id.
     *
     * This function will prepare the usage of the given table
     * within this transaction. If the table has not been opened
     * before, it will send a request to the storage to get the
     * schema and the table id. Otherwise it will just return the
     * table id.
     */
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

