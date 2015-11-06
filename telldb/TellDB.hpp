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
#include <type_traits>
#include <memory>

#include <crossbow/singleton.hpp>
#include <tellstore/ClientConfig.hpp>
#include <tellstore/ClientManager.hpp>
#include <tellstore/TransactionRunner.hpp>

#include "Transaction.hpp"

namespace tell {
namespace db {

template<class Context>
class TransactionFiber;
template<class Context>
class ClientManager;

class CounterImpl;

namespace impl {

class ClientTable {
    template<class T> friend class ::tell::db::ClientManager;
    ClientTable() {}
    void init(store::ClientHandle& handle);
    void destroy(store::ClientHandle& handle);
    uint64_t mClientId = 0;
    std::unique_ptr<store::Table> mClientsTable = nullptr;
    std::unique_ptr<store::Table> mTransactionsTable = nullptr;
public:
    /**
     * @brief Table where clients register themselves
     */
    const store::Table& clientsTable() const {
        return *mClientsTable;
    }

    const store::Table& txTable() const {
        return *mTransactionsTable;
    }
};

class Indexes;
Indexes* createIndexes(store::ClientHandle& handle);
struct TellDBContext {
    TellDBContext(ClientTable* table);
    ~TellDBContext();
    void setIndexes(Indexes* idxs);
    std::unordered_map<table_t, tell::store::Table*> tables;
    std::unordered_map<crossbow::string, table_t> tableNames;
    std::unique_ptr<Indexes> indexes;
    ClientTable* clientTable;
};

template<class Context>
struct FiberContext {
    typename std::conditional<std::is_void<Context>::value, char[0], Context>::type mUserContext;
    TellDBContext mContext;

    template<class Fun, class C = Context>
    typename std::enable_if<!std::is_void<C>::value, void>::type executeHandler(Fun& fun, Transaction& transaction) {
        fun(transaction, mUserContext);
    }

    template<class Fun, class C = Context>
    typename std::enable_if<std::is_void<C>::value, void>::type executeHandler(Fun& fun, Transaction& transaction) {
        fun(transaction);
    }

    template<class... Args>
    FiberContext(ClientTable* table, Args&&... args)
        : mUserContext(std::forward<Args>(args)...)
        , mContext(table)
    {}
};

class IteratorImpl;

} // namespace impl

/**
 * @brief Wrapper class around a transaction context.
 *
 * This class is used to run transactions. It will run in a fiber and can be used
 * to share information between the fiber and the main thread. The fiber can use
 * Transaction<Context>::block to block its execution and the main thread can use
 * Transaction<Context>::wait and Transaction<Context>::unblock to wait for the
 * fiber and unblock its execution
 */
template<class Context>
class TransactionFiber {
private: // types
    using telldb_context = typename impl::FiberContext<Context>;
    friend class ClientManager<Context>;
private: // members
    std::unique_ptr<tell::store::SingleTransactionRunner<telldb_context>> mTxRunner;
    tell::store::TransactionType mTxType;
private: // construction
    TransactionFiber(tell::store::ClientManager<telldb_context>& clientManager, tell::store::TransactionType txType)
        : mTxRunner(new tell::store::SingleTransactionRunner<telldb_context>(clientManager))
        , mTxType(txType)
    {}
private: // private access
    template<class Fun>
    void exec(Fun fun) {
        mTxRunner->execute([this, fun](tell::store::ClientHandle& handle, telldb_context& context) {
            if (context.mContext.indexes == nullptr) {
                context.mContext.setIndexes(impl::createIndexes(handle));
            }
            try {
                auto snapshot = handle.startTransaction(mTxType);
                Transaction transaction(handle, context.mContext, std::move(snapshot), mTxType);
                context.executeHandler(fun, transaction);
            } catch (std::exception& e) {
                std::cerr << "Exception: " << e.what() << std::endl;
            } catch (...) {
                // This should never happen
                std::cerr << "Got an unknown error" << std::endl;
            }
        });
    }
public: // construction
    TransactionFiber(const TransactionFiber&) = delete;
    TransactionFiber(TransactionFiber&& other)
        : mTxRunner(std::move(other.mTxRunner))
    {}
    TransactionFiber& operator=(const TransactionFiber&) = delete;
    TransactionFiber& operator=(TransactionFiber&& other) {
        mTxRunner = std::move(other.mTxRunner);
    }
public:
    /**
     * @brief Wait until the transaction completes or blocks
     *
     * Must only be called from outside the transaction thread.
     *
     * @return True if the transaction completed, false if it blocked
     */
    bool wait() {
        return mTxRunner->wait();
    }
    /**
     * @brief Blocks the transaction (and notifies the issuing thread)
     *
     * Must only be called from inside the transaction.
     */
    void block() {
        mTxRunner->block();
    }
    /**
     * @brief Unblocks the blocked transaction
     *
     * Must only be called from outside the transaction.
     *
     * @return Whether the transaction was unblocked
     */
    bool unblock() {
        return mTxRunner->unblock();
    }
};

/**
 * @brief ClientManager is the main class. It should be instantiated only once.
 *
 * This class is used to initialize the network, set up the cashes, start the
 * OS threads, opens connections to TellStore and the commit manager etc. It
 * prepares everything so that the user can run transactions. This class takes
 * a context class as template argument (which can be void if it is not needed).
 * This Context type will be created for each OS thread and passed to all fibers.
 * It will act like a thread local variable. This context will be passed to all
 * callback functions.
 */
template<class Context>
class ClientManager {
private:
    tell::store::ClientManager<impl::FiberContext<Context>> mClientManager;
    impl::ClientTable mClientTable;
public:
    /**
     * @brief Constructor
     *
     * This will construct a new ClientManager.
     *
     * @param[in] clientConfig
     * @parblock
     * The Configuration of the client. It contains information TellDB needs in
     * order to connect to TellStore and the commit manager.
     * @endparblock
     * @param[in] args
     * @parblock
     * For each processor (hardware level thread), the client manager will create a
     * new context object. To do so, it will pass the arguments to the Context class.
     * @endparblock
     */
    template<class... Args>
    ClientManager(tell::store::ClientConfig& clientConfig, Args... args)
        : mClientManager(clientConfig, &mClientTable, args...)
    {
        store::TransactionRunner::executeBlocking(mClientManager,
                [this](store::ClientHandle &handle, impl::FiberContext<Context>&){
            mClientTable.init(handle);
        });
    }

    ~ClientManager() {
        store::TransactionRunner::executeBlocking(mClientManager,
                [this](store::ClientHandle &handle, impl::FiberContext<Context>&){
            mClientTable.destroy(handle);
        });
    }

    /**
     * @brief starts a new transaction and executes fun within its context
     *
     * This function will create a new fiber which runs in the context of a new Transaction.
     * The function will get a context (if the context is not void) and a transaction object.
     *
     * @param[in] type The type of the transaction
     * @param[in] fun
     * @parblock
     * The function to call. This function will run in the new fiber. Therefore
     * it must not use any posix mutexes or other kinds of blocks. Using blocking within a
     * fiber might result in a dead lock, since blocking on a lock will not result in a
     * fiber rescheduling (since the underlying thread will block).
     * Furthermore it should not:
     *   - Call into system calls (yield).
     *   - Call any blocking operations.
     * In general it should not do anything that will schedule the underlying OS thread. 
     * @endparblock
     */
    template<class Fun>
    TransactionFiber<Context> startTransaction(
            Fun& fun,
            tell::store::TransactionType type = tell::store::TransactionType::READ_WRITE)
    {
        TransactionFiber<Context> fiber(mClientManager, type);
        fiber.exec(fun);
        return fiber;
    }

    /**
     * @brief Shutdown everything
     *
     * This functions has to get called before the client application quits. After
     * calling shutdown, no other functionality within TellDB can be used - otherwise
     * the results will be non-deterministic (and it might crash).
     */
    void shutdown() {
        mClientManager->shutdown();
    }

    std::unique_ptr<store::ScanMemoryManager> allocateScanMemory(size_t chunkCount, size_t chunkLength) {
        return mClientManager.allocateScanMemory(chunkCount, chunkLength);
    }
};

} // namespace db
} // namespace tell

