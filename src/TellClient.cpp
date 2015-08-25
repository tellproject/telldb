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

namespace {

struct EqualVisitor : public boost::static_visitor<bool>
{
    template<class T, class U>
    bool operator()(const T&, const U&) const {
        // this must never happen
        LOG_ERROR("Type error, comparing two different types");
        return false;
    }

    template<class T>
    bool operator()(const T& a, const T& b) const {
        return a == b;
    }
};

struct LessVisitor : public boost::static_visitor<bool>
{
    template<class T, class U>
    bool operator()(const T&, const U&) const {
        // this must never happen
        LOG_ERROR("Type error, comparing two different types");
        return false;
    }

    template<class T>
    bool operator()(const T& a, const T& b) const {
        return a < b;
    }
};

} // anonymous namespace

bool FieldData::operator==(const FieldData& other) const
{
    if (mData == boost::none || other.mData == boost::none) {
        return false;
    }
    return boost::apply_visitor(EqualVisitor(), mData.get(), other.mData.get());
}

bool FieldData::operator< (const FieldData& other) const
{
    if (mData == boost::none || other.mData == boost::none) {
        return false;
    }
    return boost::apply_visitor(LessVisitor(), mData.get(), other.mData.get());
}

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
