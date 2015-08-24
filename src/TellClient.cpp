#include <telldb/TellClient.hpp>
#include <tellstore/ClientManager.hpp>

namespace telldb {

using namespace tell::store;
using namespace std;

Transaction TellClient::startTransaction()
{
    return _handle.startTransaction();
}

Table TellClient::createTable(const crossbow::string& name, const Schema& schema)
{
    auto res = _handle.createTable(name, schema);
    // create b-trees
    return res;
}


} // namespace telldb
