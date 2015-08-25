#include "RemoteCounter.hpp"

namespace tell {
namespace db {
namespace {

const crossbow::string gCounterFieldName = crossbow::string("counter");

store::GenericTuple createCounterTuple(uint64_t counter) {
    return store::GenericTuple({
        std::make_pair(gCounterFieldName, static_cast<int64_t>(counter))
    });
}

} // anonymous namespace

std::shared_ptr<store::Table> RemoteCounter::createTable(store::ClientHandle& handle, const crossbow::string& name) {
    store::Schema schema(store::TableType::NON_TRANSACTIONAL);
    schema.addField(store::FieldType::BIGINT, gCounterFieldName, true);

    return std::make_shared<store::Table>(handle.createTable(name, std::move(schema)));
}

RemoteCounter::RemoteCounter(std::shared_ptr<store::Table> counterTable, uint64_t counterId)
        : mCounterTable(std::move(counterTable)),
          mCounterId(counterId),
          mInit(false),
          mCounter(0x0u),
          mReserved(0x0u),
          mNextCounter(0x0u) {
}

uint64_t RemoteCounter::incrementAndGet(store::ClientHandle& handle) {
    if (mCounter == 0x0u && !mInit) {
        mInit = true;
        requestNewBatch(handle);
    }

    mFreshKeys.wait(handle.fiber(), [this] () {
        return (mCounter != mReserved) || (mNextCounter != 0x0u);
    });

    if (mCounter == mReserved) {
        LOG_ASSERT(mNextCounter != 0x0u, "Next counter must be non 0");
        mCounter = mNextCounter;
        mReserved = mNextCounter + RESERVED_BATCH;
        mNextCounter = 0x0u;
    }

    auto key = ++mCounter;
    if (mCounter + THRESHOLD == mReserved) {
        requestNewBatch(handle);
    }
    return key;
}

uint64_t RemoteCounter::remoteValue(store::ClientHandle& handle) const {
    auto getFuture = handle.get(*mCounterTable, mCounterId);
    auto tuple = getFuture->get();
    if (!tuple->found()) {
        return 0x0u;
    }
    return static_cast<uint64_t>(mCounterTable->field<int64_t>(gCounterFieldName, tuple->data()));
}

void RemoteCounter::requestNewBatch(store::ClientHandle& handle) {
    uint64_t nextCounter;
    std::shared_ptr<store::ModificationResponse> counterFuture;
    do {
        auto getFuture = handle.get(*mCounterTable, mCounterId);
        auto tuple = getFuture->get();

        if (tuple->found()) {
            nextCounter = static_cast<uint64_t>(mCounterTable->field<int64_t>(gCounterFieldName, tuple->data()));
            counterFuture = handle.update(*mCounterTable, mCounterId, tuple->version(),
                    createCounterTuple(nextCounter + RESERVED_BATCH));
        } else {
            nextCounter = 0x0u;
            counterFuture = handle.insert(*mCounterTable, mCounterId, 0x0u, createCounterTuple(RESERVED_BATCH), true);
        }
    } while (!counterFuture->get());

    if (mCounter == mReserved) {
        mCounter = nextCounter;
        mReserved = nextCounter + RESERVED_BATCH;
    } else {
        mNextCounter = nextCounter;
    }

    mFreshKeys.notify_all();
}

} // namespace db
} // namespace tell
