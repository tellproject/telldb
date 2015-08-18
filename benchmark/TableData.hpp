#pragma once

#include "RemoteCounter.hpp"

#include <tellstore/ClientManager.hpp>
#include <tellstore/Table.hpp>

#include <crossbow/non_copyable.hpp>
#include <crossbow/string.hpp>

#include <cstdint>
#include <memory>

namespace tell {
namespace db {

/**
 * @brief Class storing shared data of a table
 *
 * Provides access to the underlying TellStore table and stores a counter assigning unique key IDs for each element.
 */
class TableData : crossbow::non_copyable, crossbow::non_movable {
public:
    TableData(store::Table table, std::shared_ptr<store::Table> counterTable)
            : mTable(std::move(table)),
              mKeyCounter(std::move(counterTable), mTable.tableId()) {
    }

    operator const store::Table&() const {
        return mTable;
    }

    uint64_t nextKey(store::ClientHandle& handle) {
        return mKeyCounter.incrementAndGet(handle);
    }

    uint64_t remoteKey(store::ClientHandle& handle) const {
        return mKeyCounter.remoteValue(handle);
    }

    store::Table& table() {
        return mTable;
    }

private:
    store::Table mTable;

    RemoteCounter mKeyCounter;
};

} // namespace db
} // namespace tell
