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
class TableData {
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
