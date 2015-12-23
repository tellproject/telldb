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
#include "BdTreeBackend.hpp"

#include <bdtree/error_code.h>

#include <stdexcept>
#include <utility>

namespace tell {
namespace db {
namespace {

const crossbow::string gPointerFieldName = "pptr";

store::GenericTuple createPtrTuple(bdtree::physical_pointer pptr) {
    return store::GenericTuple({
        std::make_pair(gPointerFieldName, static_cast<int64_t>(pptr.value))
    });
}

const crossbow::string gNodeFieldName = "node";

store::GenericTuple createNodeTuple(const char* data, size_t length) {
    return store::GenericTuple({
        std::make_pair(gNodeFieldName, crossbow::string(data, data + length))
    });
}

} // anonymous namespace

BdTreeNodeData::BdTreeNodeData(store::Table& table, store::Record::id_t id, std::unique_ptr<store::Tuple> tuple)
        : mTuple(std::move(tuple)),
          mSize(0x0u),
          mData(nullptr) {
    bool isNull = false;
    store::FieldType type;
    auto field = table.record().data(mTuple->data(), id, isNull, &type);
    if (isNull || type != store::FieldType::BLOB) {
        throw std::logic_error("Invalid field");
    }

    auto offsetData = reinterpret_cast<const uint32_t*>(field);
    auto offset = offsetData[0];
    mSize = offsetData[1] - offset;
    mData = mTuple->data() + offset;
}

std::unique_ptr<store::Tuple> BdTreeBaseTable::doRead(uint64_t key, std::error_code& ec) {
    auto getFuture = mHandle.get(mTable.table(), key);
    if (getFuture->waitForResult()) {
        return getFuture->get();
    } else if (getFuture->error() == store::error::not_found) {
        ec = make_error_code(bdtree::error::object_doesnt_exist);
        return {nullptr};
    } else {
        ec = getFuture->error();
        return {nullptr};
    }
}

bool BdTreeBaseTable::doInsert(uint64_t key, store::GenericTuple tuple, std::error_code& ec) {
    auto insertFuture = mHandle.insert(mTable.table(), key, 0x0u, std::move(tuple));
    if (insertFuture->waitForResult()) {
        return true;
    }

    ec = insertFuture->error();
    if (ec == store::error::invalid_write || ec == store::error::not_in_snapshot) {
        ec = make_error_code(bdtree::error::object_exists);
    }
    return false;
}

bool BdTreeBaseTable::doUpdate(uint64_t key, store::GenericTuple tuple, uint64_t version, std::error_code& ec) {
    auto updateFuture = mHandle.update(mTable.table(), key, version, std::move(tuple));
    if (updateFuture->waitForResult()) {
        return true;
    }

    ec = updateFuture->error();
    if (ec == store::error::invalid_write) {
        ec = make_error_code(bdtree::error::object_doesnt_exist);
    } else if (ec == store::error::not_in_snapshot) {
        ec = make_error_code(bdtree::error::wrong_version);
    }
    return false;
}

bool BdTreeBaseTable::doRemove(uint64_t key, uint64_t version, std::error_code& ec) {
    auto removeFuture = mHandle.remove(mTable.table(), key, version);
    if (removeFuture->waitForResult()) {
        return true;
    }

    ec = removeFuture->error();
    if (ec == store::error::invalid_write) {
        ec = make_error_code(bdtree::error::object_doesnt_exist);
    } else if (ec == store::error::not_in_snapshot) {
        ec = make_error_code(bdtree::error::wrong_version);
    }
    return false;
}

store::Table BdTreePointerTable::createTable(store::ClientHandle& handle, const crossbow::string& name) {
    store::Schema schema(store::TableType::NON_TRANSACTIONAL);
    schema.addField(store::FieldType::BIGINT, gPointerFieldName, true);

    return handle.createTable(name, std::move(schema));
}

std::tuple<bdtree::physical_pointer, uint64_t> BdTreePointerTable::read(bdtree::logical_pointer lptr,
        std::error_code& ec) {
    auto tuple = doRead(lptr.value, ec);
    if (!tuple)
        return std::make_tuple(bdtree::physical_pointer{0x0u}, 0x0u);

    auto pptr = mTable.table().field<int64_t>(gPointerFieldName, tuple->data());
    return std::make_tuple(bdtree::physical_pointer{static_cast<uint64_t>(pptr)}, tuple->version());
}

uint64_t BdTreePointerTable::insert(bdtree::logical_pointer lptr, bdtree::physical_pointer pptr, std::error_code& ec) {
    return (doInsert(lptr.value, createPtrTuple(pptr), ec) ? 0x1u : 0x0u);
}

uint64_t BdTreePointerTable::update(bdtree::logical_pointer lptr, bdtree::physical_pointer pptr, uint64_t version,
        std::error_code& ec) {
    return (doUpdate(lptr.value, createPtrTuple(pptr), version, ec) ? version + 1 : 0x0u);
}

void BdTreePointerTable::remove(bdtree::logical_pointer lptr, uint64_t version, std::error_code& ec) {
    // In the case we have no version the bdtree tries to erase with version max
    // This is invalid in the TellStore as version max denotes the active version
    if (version == std::numeric_limits<uint64_t>::max()) {
        version = std::numeric_limits<uint64_t>::max() - 2;
    }
    doRemove(lptr.value, version, ec);
}

store::Table BdTreeNodeTable::createTable(store::ClientHandle& handle, const crossbow::string& name) {
    store::Schema schema(store::TableType::NON_TRANSACTIONAL);
    schema.addField(store::FieldType::BLOB, gNodeFieldName, true);

    return handle.createTable(name, std::move(schema));
}

BdTreeNodeTable::BdTreeNodeTable(store::ClientHandle& handle, TableData& table)
        : BdTreeBaseTable(handle, table) {
    if (!mTable.table().record().idOf(gNodeFieldName, mNodeDataId)) {
        throw std::logic_error("Node field not found");
    }
}

BdTreeNodeData BdTreeNodeTable::read(bdtree::physical_pointer pptr, std::error_code& ec) {
    auto tuple = doRead(pptr.value, ec);
    if (!tuple)
        return BdTreeNodeData();

    return BdTreeNodeData(mTable.table(), mNodeDataId, std::move(tuple));
}

void BdTreeNodeTable::insert(bdtree::physical_pointer pptr, const char* data, size_t length, std::error_code& ec) {
    doInsert(pptr.value, createNodeTuple(data, length), ec);
}

void BdTreeNodeTable::remove(bdtree::physical_pointer pptr, std::error_code& ec) {
    doRemove(pptr.value, 0x1u, ec);
}

} // namespace db
} // namespace tell
