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

#undef NDEBUG

#include <telldb/TellDB.hpp>
#include <telldb/Transaction.hpp>

#include <crossbow/allocator.hpp>
#include <crossbow/program_options.hpp>

using namespace crossbow::program_options;

int main(int argc, const char** argv) {
    bool help = false;
    crossbow::string commitManager;
    crossbow::string storageNodes;
    auto opts = create_options("tpcc_server",
            value<'h'>("help", &help, tag::description{"print help"}),
            value<'c'>("commit-manager", &commitManager, tag::description{"Address to the commit manager"}),
            value<'s'>("storage-nodes", &storageNodes, tag::description{"Semicolon-separated list of storage node addresses"})
            );
    try {
        parse(opts, argc, argv);
    } catch (argument_not_found& e) {
        std::cerr << e.what() << std::endl << std::endl;
        print_help(std::cout, opts);
        return 1;
    }
    if (help) {
        print_help(std::cout, opts);
        return 0;
    }

    crossbow::allocator::init();

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString("DEBUG");
    tell::store::ClientConfig config;
    config.commitManager = config.parseCommitManager(commitManager);
    config.tellStore = config.parseTellStore(storageNodes);
    tell::db::ClientManager<void> clientManager(config);

    // Populate simple test db
    {
        auto transaction = [](tell::db::Transaction& tx) {
            tell::store::Schema schema(tell::store::TableType::TRANSACTIONAL);
            schema.addField(tell::store::FieldType::INT, "foo", true);
            schema.addField(tell::store::FieldType::TEXT, "bar", false);
            auto tid = tx.createTable("foo", schema);
            for (int32_t i = 0; i < 100; ++i) {
                tx.insert(tid, tell::db::key_t{uint64_t(i)},
                        {{
                        {"foo", i},
                        {"bar", i % 5 == 0 ? tell::db::Field(nullptr) : tell::db::Field("foobar")}
                        }});
            }
            tx.commit();
        };
        auto fiber = clientManager.startTransaction(transaction);
        fiber.wait();
    }
    // simple read
    {
        auto transaction = [](tell::db::Transaction& tx) {
            std::vector<tell::db::Future<tell::db::Tuple>> responses;
            auto tid = tx.openTable("foo").get();
            for (int i = 0; i < 100; ++i) {
                responses.emplace_back(tx.get(tid, tell::db::key_t{uint64_t(i)}));
            }
            for (int i = 0; i < 100; ++i) {
                auto resp = responses[i].get();
                auto foo = resp.at("foo").value<int32_t>();
                if (i != foo) {
                    std::cerr << "got " << foo << " for foo instead of " << i << std::endl;
                }
                if (i % 5 == 0) {
                    if (!resp.at("bar").null()) {
                        std::cerr << "got " << resp.at("bar").value<crossbow::string>() << " for bar instead of nullptr\n";
                    }
                } else {
                    if (resp.at("bar").null()) {
                        std::cerr << "bar is not supposed to be null\n";
                    }
                    auto bar = resp.at("bar").value<crossbow::string>();
                    if (bar != "foobar") {
                        std::cerr << "got " << bar << " for bar instead of foobar\n";
                    }
                }
            }
        };
        auto fiber = clientManager.startTransaction(transaction);
        fiber.wait();
    }
    // Test range queries
    {
        auto transaction = [](tell::db::Transaction& tx) {
            tell::store::Schema schema(tell::store::TableType::TRANSACTIONAL);
            schema.addField(tell::store::FieldType::INT, "field", true);
            schema.addIndex("idx", std::make_pair(true, std::vector<tell::store::Schema::id_t>{schema.idOf("field")}));
            auto tid = tx.createTable("idx_table", schema);
            for (int32_t i = 0; i < 1000; ++i) {
                tx.insert(tid, tell::db::key_t{uint64_t(i)},
                        {{
                        {"field", i}
                        }});
            }
            int32_t currKey = 132;
            auto iter = tx.lower_bound(tid, "idx", {tell::db::Field(currKey)});
            for (int i = 0; i < 200; ++i) {
                LOG_ASSERT(!iter.done(), "ERROR: Should not be out of range");
                auto k = iter.key()[0].value<int32_t>();
                LOG_ASSERT(k == currKey, "range broken");
                LOG_ASSERT(uint64_t(k) == iter.value().value, "Index does not point to correct value");
                iter.next();
                ++currKey;
            }
            tx.commit();
        };
        auto fiber = clientManager.startTransaction(transaction);
        fiber.wait();
    }
    {
        auto transaction = [](tell::db::Transaction& tx) {
            auto tid = tx.openTable("idx_table").get();
            int32_t currKey = 132;
            auto iter = tx.lower_bound(tid, "idx", {tell::db::Field(currKey)});
            for (int i = 0; i < 200; ++i) {
                LOG_ASSERT(!iter.done(), "ERROR: Should not be out of range");
                auto k = iter.key()[0].value<int32_t>();
                LOG_ASSERT(k == currKey, "range broken");
                LOG_ASSERT(uint64_t(k) == iter.value().value, "Index does not point to correct value");
                iter.next();
                ++currKey;
            }
            tx.commit();
        };
        auto fiber = clientManager.startTransaction(transaction);
        fiber.wait();
    }
}
