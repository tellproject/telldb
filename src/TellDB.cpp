#include <telldb/TellDB.hpp>
#include <telldb/TellClient.hpp>
#include <tellstore/ClientManager.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <bdtree/logical_table_cache.h>

namespace tell {
namespace db {

class TellDBImpl {
    crossbow::infinio::InfinibandService _service;
    tell::store::ClientManager _clientManager;
    bdtree::logical_table_cache<std::vector<FieldData>, uint64_t, BdTreeBackend> mTableCache; 
public:
    TellDBImpl(const tell::store::ClientConfig& config)
        : _clientManager(config)
    {}
    void execute(const std::function<void(TellClient&)>& fun);
};

void TellDBImpl::execute(const std::function<void(TellClient&)>& fun)
{
    _clientManager.execute([fun, this](tell::store::ClientHandle& handle) {
        TellClient client(handle, mTableCache);
        fun(client);
    });
}

TellDB::TellDB(const tell::store::ClientConfig& config)
    : _impl(new TellDBImpl(config))
{}

void TellDB::execute(const std::function<void(TellClient&)>& fun)
{
    _impl->execute(fun);
}

} // namespace db
} // namespace tell
