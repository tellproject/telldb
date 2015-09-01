#pragma once
#include <tellstore/ClientManager.hpp>
#include <bdtree/logical_table_cache.h>
#include <telldb/FieldData.hpp>

#include <memory>

#include "BdTreeBackend.hpp"

namespace tell {
namespace db {

struct IndexDescriptor {
    using cache_t = bdtree::logical_table_cache<std::vector<FieldData>, uint64_t, BdTreeBackend>;
    cache_t cache;
    TableData ptrTable;
    TableData nodeTable;
    IndexDescriptor(TableData&& ptrTable, TableData&& nodeTable)
        : ptrTable(std::move(ptrTable)), nodeTable(std::move(nodeTable))
    {}
};

struct TableDescriptor {
};

class TableCache {
    tell::store::ClientHandle& mHandle;
    std::shared_ptr<tell::store::Table> mCounterTable;
public:
    TableCache(tell::store::ClientHandle& handle);
    void createTable(const crossbow::string& name, const tell::store::Schema& schema);
};

} // namespace db
} // namespace tell

