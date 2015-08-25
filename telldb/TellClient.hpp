#pragma once
#include <tellstore/ClientManager.hpp>
#include <crossbow/string.hpp>
#include <bdtree/logical_table_cache.h>
#include <bdtree/util.h>

#include <boost/variant.hpp>
#include <boost/optional.hpp>

#include <vector>

#include "Transaction.hpp"
#include "FieldData.hpp"

namespace tell {
namespace store {

class ClientHandle;

} // namespace store
} // namespace tell

namespace tell {
namespace db {

class BdTreeBackend;

class TellClient {
public: // types
    using tree_cache = bdtree::logical_table_cache<std::vector<FieldData>, uint64_t, BdTreeBackend>;
private:
    friend class TellDBImpl;
    tell::store::ClientHandle& _handle;
    tree_cache& mCache;
private:
    TellClient(tell::store::ClientHandle& handle, tree_cache& cache)
        : _handle(handle)
        , mCache(cache)
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

} // namespace db
} // namespace telldb
