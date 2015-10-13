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

#include <tellstore/ClientManager.hpp>
#include <tellstore/Table.hpp>

#include <crossbow/infinio/Fiber.hpp>
#include <crossbow/non_copyable.hpp>
#include <crossbow/string.hpp>

#include <cstdint>
#include <memory>

namespace tell {
namespace db {

/**
 * @brief Provdes a remote counter assigning unique values
 *
 * Reserves a range of unique values from the table and returns them on request.
 */
class RemoteCounter {
public:
    /**
     * @brief Creates a new counter table with the associated name
     */
    static std::shared_ptr<store::Table> createTable(store::ClientHandle& handle, const crossbow::string& name);

    RemoteCounter(std::shared_ptr<store::Table> counterTable, uint64_t counterId);

    /**
     * @brief Increments the counter value by one and returns the value
     */
    uint64_t incrementAndGet(store::ClientHandle& handle);

    /**
     * @brief Reads the counter's remote value from the database
     */
    uint64_t remoteValue(store::ClientHandle& handle) const;

private:
    static constexpr uint64_t RESERVED_BATCH = 1000;

    static constexpr uint64_t THRESHOLD = 100;

    static_assert(RESERVED_BATCH > THRESHOLD, "Number of reserved keys must be larger than the threshold");

    void requestNewBatch(store::ClientHandle& handle);

    std::shared_ptr<store::Table> mCounterTable;
    uint64_t mCounterId;

    bool mInit;
    uint64_t mCounter;
    uint64_t mReserved;

    uint64_t mNextCounter;

    crossbow::infinio::ConditionVariable mFreshKeys;
};

} // namespace db
} // namespace tell
