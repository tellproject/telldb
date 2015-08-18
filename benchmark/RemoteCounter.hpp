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
class RemoteCounter : crossbow::non_copyable, crossbow::non_movable {
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
