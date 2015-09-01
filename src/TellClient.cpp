#include <telldb/TellClient.hpp>
#include <tellstore/ClientManager.hpp>
#include <crossbow/logger.hpp>
#include <bdtree/bdtree.h>
#include <boost/lexical_cast.hpp>

#include "BdTreeBackend.hpp"

namespace tell {
namespace db {

using namespace tell::store;
using namespace std;

Transaction TellClient::startTransaction()
{
    return _handle.startTransaction();
}

Table TellClient::createTable(const crossbow::string& name, const Schema& schema)
{
    Schema idxSchema(TableType::NON_TRANSACTIONAL);
    idxSchema.addField(FieldType::BLOB, "value", true);
    Schema cntSchema(TableType::NON_TRANSACTIONAL);
    cntSchema.addField(FieldType::BIGINT, "value", true);
    auto res = _handle.createTable(name, schema);
    // create b-trees
    const auto& indexes = schema.indexes();
    for (int i = 0; i < indexes.size(); ++i) {
        auto idxName = name + "_idx_" + boost::lexical_cast<crossbow::string>(i);
        auto cntPtrTable = std::make_shared<Table>(std::move(_handle.createTable(idxName + "_ptr_cnt", cntSchema)));
        auto cntNodeTable = std::make_shared<Table>(std::move(_handle.createTable(idxName + "_node_cnt", cntSchema)));
        TableData ptrTable(_handle.createTable(idxName + "_ptr", idxSchema), std::move(cntPtrTable));
        TableData nodeTable(_handle.createTable(idxName + "_node", idxSchema), std::move(cntNodeTable));
        tell::db::BdTreeBackend backend(
                _handle,
                ptrTable, // ptr table
                nodeTable // node table
        );
        bdtree::map<std::vector<FieldData>, uint64_t, tell::db::BdTreeBackend> _(backend, mCache, 0, true);
    }
    return res;
}


} // namespace db
} // namespace tell
