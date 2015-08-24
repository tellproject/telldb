// !b = ../src/TellDB.cpp
#pragma once
#include <memory>
#include <functional>

#include "TellClient.hpp"

namespace tell {
namespace store {

struct ClientConfig;

} // namespace store
} // namespace tell

namespace telldb {

class TellDBImpl;

class TellDB {
    std::unique_ptr<TellDBImpl> _impl;
public:
    TellDB(const tell::store::ClientConfig& config);
    void execute(const std::function<void(TellClient&)>& fun);
};

}
