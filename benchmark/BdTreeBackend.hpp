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

#include "TableData.hpp"

#include <tellstore/ClientManager.hpp>
#include <tellstore/GenericTuple.hpp>
#include <tellstore/Table.hpp>

#include <bdtree/base_backend.h>

#include <crossbow/non_copyable.hpp>
#include <crossbow/string.hpp>

#include <cstdint>
#include <memory>

namespace tell {
namespace db {

class BdTreeNodeData {
public:
    BdTreeNodeData()
            : mSize(0x0u),
              mData(nullptr) {
    }

    BdTreeNodeData(store::Table& table, store::Record::id_t id, std::unique_ptr<store::Tuple> tuple);

    const char* data() const {
        return mData;
    }

    size_t length() const {
        return mSize;
    }

private:
    std::unique_ptr<store::Tuple> mTuple;
    uint32_t mSize;
    const char* mData;
};

/**
 * @brief Base class for shared functionality between BdTreePointerTable and BdTreeNodeTable
 */
class BdTreeBaseTable : crossbow::non_copyable, crossbow::non_movable {
protected:
    BdTreeBaseTable(store::ClientHandle& handle, TableData& table)
            : mTable(table),
              mHandle(handle) {
    }

    ~BdTreeBaseTable() = default;

    uint64_t nextKey() {
        return mTable.nextKey(mHandle);
    }

    uint64_t remoteKey() {
        return mTable.remoteKey(mHandle);
    }

    std::unique_ptr<store::Tuple> doRead(uint64_t key, std::error_code& ec);

    bool doInsert(uint64_t key, store::GenericTuple tuple, std::error_code& ec);

    bool doUpdate(uint64_t key, store::GenericTuple tuple, uint64_t version, std::error_code& ec);

    bool doRemove(uint64_t key, uint64_t version, std::error_code& ec);

    TableData& mTable;

private:
    store::ClientHandle& mHandle;
};

/**
 * @brief Pointer table for the Bd-Tree mapping logical keys to the physical keys of the Bd-Tree
 */
class BdTreePointerTable : public bdtree::base_ptr_table<BdTreePointerTable>, private BdTreeBaseTable {
public:
    static store::Table createTable(store::ClientHandle& handle, const crossbow::string& name);

    BdTreePointerTable(store::ClientHandle& handle, TableData& table)
            : BdTreeBaseTable(handle, table) {
    }

    bdtree::logical_pointer get_next_ptr() {
        return bdtree::logical_pointer{nextKey()};
    }

    bdtree::logical_pointer get_remote_ptr() {
        return bdtree::logical_pointer{remoteKey()};
    }

    std::tuple<bdtree::physical_pointer, uint64_t> read(bdtree::logical_pointer lptr, std::error_code& ec);

    using bdtree::base_ptr_table<BdTreePointerTable>::read;

    uint64_t insert(bdtree::logical_pointer lptr, bdtree::physical_pointer pptr, std::error_code& ec);

    using bdtree::base_ptr_table<BdTreePointerTable>::insert;

    uint64_t update(bdtree::logical_pointer lptr, bdtree::physical_pointer pptr, uint64_t version, std::error_code& ec);

    using bdtree::base_ptr_table<BdTreePointerTable>::update;

    void remove(bdtree::logical_pointer lptr, uint64_t version, std::error_code& ec);

    using bdtree::base_ptr_table<BdTreePointerTable>::remove;
};

/**
 * @brief Node table for the Bd-Tree storing the physical Bd-Tree pages
 */
class BdTreeNodeTable : public bdtree::base_node_table<BdTreeNodeTable, BdTreeNodeData>, private BdTreeBaseTable  {
public:
    static store::Table createTable(store::ClientHandle& handle, const crossbow::string& name);

    BdTreeNodeTable(store::ClientHandle& handle, TableData& table);

    bdtree::physical_pointer get_next_ptr() {
        return bdtree::physical_pointer{nextKey()};
    }

    bdtree::physical_pointer get_remote_ptr() {
        return bdtree::physical_pointer{remoteKey()};
    }

    BdTreeNodeData read(bdtree::physical_pointer pptr, std::error_code& ec);

    using bdtree::base_node_table<BdTreeNodeTable, BdTreeNodeData>::read;

    void insert(bdtree::physical_pointer pptr, const char* data, size_t length, std::error_code& ec);

    using bdtree::base_node_table<BdTreeNodeTable, BdTreeNodeData>::insert;

    void remove(bdtree::physical_pointer pptr, std::error_code& ec);

    using bdtree::base_node_table<BdTreeNodeTable, BdTreeNodeData>::remove;

private:
    store::Record::id_t mNodeDataId;
};

/**
 * @brief TellStore backend for the Bd-Tree
 */
class BdTreeBackend : crossbow::non_copyable, crossbow::non_movable {
public:
    using ptr_table = BdTreePointerTable;

    using node_table = BdTreeNodeTable;

    BdTreeBackend(store::ClientHandle& handle, TableData& ptrTable, TableData& nodeTable)
            : mPtr(handle, ptrTable),
              mNode(handle, nodeTable) {
    }

    ptr_table& get_ptr_table() {
        return mPtr;
    }

    node_table& get_node_table() {
        return mNode;
    }

private:
    ptr_table mPtr;
    node_table mNode;
};

} // namespace db
} // namespace tell
