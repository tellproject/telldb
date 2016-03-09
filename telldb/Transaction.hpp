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
#include "Iterator.hpp"

#include <tellstore/TransactionType.hpp>
#include <tellstore/ClientSocket.hpp>
#include <crossbow/ChunkAllocator.hpp>
#include <tuple>

/**
 * @mainpage TellDB - Running transactions on Tell
 *
 * TellDB is the main interface of Tell. If you are unsure what to use to implement
 * a transactional workload, this is probably it.
 *
 */

namespace tell {
namespace commitmanager {

class SnapshotDescriptor;

} // namespace commitmanager

namespace store {

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

class CounterImpl;
class Counter {
    friend class Transaction;
    CounterImpl* impl;
    store::ClientHandle& mHandle;
    Counter(CounterImpl* impl, store::ClientHandle& handle);
public:
    uint64_t next();
    Counter(Counter&&);
    ~Counter();
};

class ScanQuery;

class Transaction {
public: // Types
    /**
     *  A string which has a life time equal to the lifetime of the transaction
     */ 
    using ChunkString = crossbow::basic_string<char, std::char_traits<char>, crossbow::ChunkAllocator<char>>;
private:
    tell::store::ClientHandle& mHandle;
    impl::TellDBContext& mContext;
    crossbow::ChunkMemoryPool mPool;
    std::unique_ptr<commitmanager::SnapshotDescriptor> mSnapshot;
    std::unique_ptr<TransactionCache> mCache;
    // will be set to true if there is any data
    // written to the storage
    store::TransactionType mType;
    bool mCommitted = false;
public:
    Transaction(tell::store::ClientHandle& handle,
            impl::TellDBContext& context,
            std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot,
            tell::store::TransactionType type);
    ~Transaction();
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
    const tell::store::Schema& getSchema(table_t table);
    /**
     * @brief Creates a new table with the given schema.
     *
     * This creates a new table with the given schema. It is important,
     * that tables get created with this method. Do not use the lower
     * level TellStore interface to do this! This interface will also create
     * the tables necassary to store the indexes.
     *
     * @param name The name of the table
     * @param schema The table schema and the indexes
     */
    table_t createTable(const crossbow::string& name, const store::Schema& schema);
    /**
     * @brief creates a server-side counter.
     *
     * @param name The name of the counter to be created
     * @return A reference object to the counter
     */
    Counter createCounter(const crossbow::string& name);
    /**
     * @brief Get a reference to an existing counter.
     *
     * @param name The name of the counter to get
     * @return A referance to a counter
     */
    Counter getCounter(const crossbow::string& name);
public: // read-write operations
    /**
     * @brief Gets a tuple from the storage
     *
     * This will get a tuple with a given key and table id from
     * the storage. This function returns a future, which means
     * that it returns immediately but the user can wait on it
     * for the result. If the tuple is cached, this function
     * will return immediately.
     *
     * @param table The table id
     * @param key   The key of the tuple
     * @return A future holding the result
     */
    Future<Tuple> get(table_t tableId, key_t key);
    Iterator lower_bound(table_t tableId, const crossbow::string& idxName, const KeyType& key);
    Iterator reverse_lower_bound(table_t tableId, const crossbow::string& idxName, const KeyType& key);
    /**
     * @brief Create a new empty tuple
     */
    Tuple newTuple(table_t table);
    /**
     * @brief Inserts a new tuple
     *
     * This will insert a new tuple into the storage. In praxis,
     * this will only write into the local cache, so no communication
     * with the storage does happen here. Furthermore, only basic conflict
     * detection will happen. It will only check whether the given
     * key already exists in the local cache, in which case it will
     * throw an exception. Otherwise, conflict detection will occur
     * during the commit phase.
     *
     * @param table The table id
     * @param key   The key of the tuple
     * @param tuple The tuple to insert
     * @throws TupleExistsException If the key is already in the local cache.
     */
    void insert(table_t table, key_t key, const Tuple& tuple);
    /**
     * @brief Inserts a new tuple
     *
     * This will insert a new tuple with the values passed. The values
     * are saved in a map. Its key is the name of the field and its value
     * the value to set the field to.
     *
     * @param table  The table id
     * @param key    The key of the tuple
     * @param values The tuple to insert
     * @throws TupleExistsException If the key is already in the local cache.
     * @throws FieldDoesNotExist If a field does not exist in the schema.
     * @throws FieldNotSet If a required field is not set in the tuple.
     */
    void insert(table_t table, key_t key, const std::unordered_map<crossbow::string, Field>& values);
    /**
     * @brief Updates a tuple
     *
     * This will update an existing tuple into the storage. In praxis,
     * this will only write into the local cache, so no communication
     * with the storage does happen here. Furthermore, only basic conflict
     * detection will happen. It will only check whether the given
     * key already exists in the local cache, in which case it will
     * check whether the last read did return the newest version. If
     * this is not the case, we already know that a conflict occurred
     * and the function will throw an exception.  Otherwise, conflict
     * detection will occur during the commit phase.
     *
     * @param table The table id
     * @param key   The key of the tuple
     * @param from  The current version of the tuple
     * @param to    The new version of the tuple
     * @throws Conflict If a conflict is detected.
     */
    void update(table_t table, key_t key, const Tuple& from, const Tuple& to);
    /**
     * @brief Deletes a tuple
     *
     * This will delete an existing tuple. In praxis,
     * this will only write into the local cache, so no communication
     * with the storage does happen here. Furthermore, only basic conflict
     * detection will happen. It will only check whether the given
     * key already exists in the local cache, in which case it will
     * check whether the last read did return the newest version. If
     * this is not the case, we already know that a conflict occurred
     * and the function will throw an exception.  Otherwise, conflict
     * detection will occur during the commit phase.
     *
     * @param table The table id
     * @param key   The key of the tuple
     * @param tuple The tuple to delete
     * @throws Conflict If a conflict is detected.
     */
    void remove(table_t table, key_t key, const Tuple& tuple);
    /**
     * @brief Starts a new scan on the storage
     *
     * WARNING: This is currently only supported for analytical queries
     */
    std::shared_ptr<store::ScanIterator> scan(const ScanQuery& query, store::ScanMemoryManager& memoryManager);
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
     * @throws Conflict if a conflict gets detected.
     */
    void commit();
private:
    void writeBack(bool withIndexes = true);
    void writeUndoLog(std::pair<size_t, uint8_t*> log);
    void removeUndoLog(std::pair<size_t, uint8_t*> log);
    const store::Record& getRecord(table_t tableId) const;
public: // non-commands
    /**
     * @brief Gets the memory pool of this transaction
     *
     * TellDB transactions make use of the fact that transactions
     * have a limited run time. Therefore they use ChunkAllocators
     * to allocate and deallocate memory. A ChunkAllocator allocates
     * memory block-wise and then gives out chunks of memory from
     * these blocks. Therefore allocations on the heap get nearly
     * as cheap as stack allocations. Deleting an object allocated
     * from this pool does not have any effects. As soon as the
     * transaction gets destroyed, all memory from this pool gets
     * freed.
     *
     * The user can use the same pool to allocate objects and bind
     * the object lifetime to the lifetime of the transaction. This
     * should be done carefully, as using it incorrectly might lead
     * to memory errors.
     */
    crossbow::ChunkMemoryPool& pool();
    const tell::commitmanager::SnapshotDescriptor& snapshot() const {
        return *mSnapshot;
    }
    tell::store::ClientHandle& getHandle() {
        return mHandle;
    }
};

template<id_t id, class... T>
struct tuple_set {
    static void set(const std::tuple<T...>& tuple, Tuple& t) {
        t[id] = Field(std::get<id>(tuple));
        tuple_set<id - 1, T...>::set(tuple, t);
    }
};

template<class... T>
struct tuple_set<0, T...> {
    static void set(const std::tuple<T...>& tuple, Tuple& t) {
        t[0] = Field(std::get<0>(tuple));
    }
};

} // namespace db
} // namespace tell

