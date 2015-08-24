// !b = ../src/Transaction.cpp
#pragma once
#include <tellstore/ClientManager.hpp>

namespace telldb {

class Transaction {
    tell::store::ClientTransaction _transaction;
public:
    Transaction(tell::store::ClientTransaction&& transaction)
        : _transaction(std::move(transaction))
    {}
public:
    uint64_t version() const {
        return _transaction.version();
    }
};

} // namespace telldb
