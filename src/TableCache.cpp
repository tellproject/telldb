#include "TableCache.hpp"
#include "TableData.hpp"
#include "BdTreeBackend.hpp"
#include <telldb/FieldData.hpp>
#include <bdtree/bdtree.h>

#include <boost/lexical_cast.hpp>

namespace tell {
namespace db {

using namespace tell::store;

TableCache::TableCache(tell::store::ClientHandle& handle)
    : mHandle(handle)
{
    crossbow::string counterTableName = "__counter_table";
    auto cRes = mHandle.getTable(counterTableName);
    std::shared_ptr<Table> counterTable;
    if (cRes->wait()) {
        counterTable = std::make_shared<Table>(cRes->get());
    } else {
        Schema counterSchema(TableType::NON_TRANSACTIONAL);
        counterSchema.addField(FieldType::BIGINT, "value", true);
        counterTable = std::make_shared<Table>(
                mHandle.createTable(counterTableName, counterSchema));
    }
    mCounterTable = std::move(counterTable);
}

void TableCache::createTable(const crossbow::string& name, const Schema& schema)
{
    Schema idxSchema(TableType::NON_TRANSACTIONAL);
    idxSchema.addField(FieldType::BLOB, "value", true);
    auto res = mHandle.createTable(name, schema);
    // create b-trees
    const auto& indexes = schema.indexes();
    for (int i = 0; i < indexes.size(); ++i) {
        auto idxName = name + "_idx_" + boost::lexical_cast<crossbow::string>(i);
        TableData ptrTable(mHandle.createTable(idxName + "_ptr", idxSchema), mCounterTable);
        TableData nodeTable(mHandle.createTable(idxName + "_node", idxSchema), mCounterTable);
        IndexDescriptor idxDesc(std::move(ptrTable), std::move(nodeTable));
        tell::db::BdTreeBackend backend(
                mHandle,
                idxDesc.ptrTable, // ptr table
                idxDesc.nodeTable // node table
        );
        bdtree::map<std::vector<FieldData>, uint64_t, tell::db::BdTreeBackend> _(backend, idxDesc.cache, 0, true);
    }
}

} // namespace db
} // namespace tell

