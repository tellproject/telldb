#include <telldb/TellDB.hpp>
#include <tellstore/ClientManager.hpp>
#include <crossbow/infinio/InfinibandService.hpp>

namespace telldb {

class TellDBImpl {
    crossbow::infinio::InfinibandService _service;
    tell::store::ClientManager _clientManager;
public:
    TellDBImpl(const tell::store::ClientConfig& config)
        : _clientManager(_service, config)
    {}
    void execute(const std::function<void(TellClient&)>& fun);
};

void TellDBImpl::execute(const std::function<void(TellClient&)>& fun)
{
    _clientManager.execute([fun](tell::store::ClientHandle& handle) {
        TellClient client(handle);
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

} // namespace telldb
