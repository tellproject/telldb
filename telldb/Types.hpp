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
#include <cstdint>
#include <type_traits>
#include <functional>

namespace tell {
namespace db {

struct table_t {
    uint64_t value;
    operator uint64_t() const {
        return value;
    }
};

struct key_t {
    uint64_t value;
    operator uint64_t() const {
        return value;
    }
};

static_assert(std::is_pod<table_t>::value, "table_t is not a POD");
static_assert(std::is_pod<key_t>::value, "key_t is not a POD");
} // namespace db
} // namespace tell

namespace std {

template<>
struct hash<tell::db::key_t> {
    using argument_type = tell::db::key_t;
    using result_type = size_t;
    std::hash<uint64_t> mImpl;
    size_t operator() (const argument_type& arg) const {
        return mImpl(arg.value);
    }
};

template<>
struct hash<tell::db::table_t> {
    using argument_type = tell::db::table_t;
    using result_type = size_t;
    std::hash<uint64_t> mImpl;
    size_t operator() (const argument_type& arg) const {
        return mImpl(arg.value);
    }
};

}

