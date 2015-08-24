// !b = ../src/TellClient.cpp
#pragma once
#include <tellstore/ClientManager.hpp>
#include "Transaction.hpp"

namespace tell {
namespace store {

class ClientHandle;

} // namespace store
} // namespace tell

namespace telldb {

class TellClient {
    friend class TellDBImpl;
    tell::store::ClientHandle& _handle;
private:
    TellClient(tell::store::ClientHandle& handle)
        : _handle(handle)
    {}

public:
    Transaction startTransaction();

public: // interface to work outside of a transaction

    tell::store::Table createTable(const crossbow::string& name, const tell::store::Schema& schema);

    std::shared_ptr<tell::store::GetTableResponse> getTable(const crossbow::string& name);

    std::shared_ptr<tell::store::GetResponse> get(const tell::store::Table& table, uint64_t key);

    std::shared_ptr<tell::store::ModificationResponse> insert(const tell::store::Table& table, uint64_t key, uint64_t version,
            const tell::store::GenericTuple& tuple, bool hasSucceeded = true);

    std::shared_ptr<tell::store::ModificationResponse> update(const tell::store::Table& table, uint64_t key, uint64_t version,
            const tell::store::GenericTuple& tuple);

    std::shared_ptr<tell::store::ModificationResponse> remove(const tell::store::Table& table, uint64_t key, uint64_t version);

    std::shared_ptr<tell::store::ScanResponse> scan(const tell::store::Table& table, tell::store::ScanQueryType queryType, uint32_t selectionLength,
            const char* selection, uint32_t queryLength, const char* query);
};

} // namespace telldb
