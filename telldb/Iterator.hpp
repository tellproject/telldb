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
#include "Types.hpp"
#include "Field.hpp"

namespace tell {
namespace db {

namespace impl {
class IteratorImpl;
} // namespace impl

using KeyType = std::vector<Field>;
using ValueType = key_t;

enum class IteratorDirection {
    Forward, Backward
};

/**
 * @brief Iterator class used for range queries.
 */
class Iterator {
    std::unique_ptr<impl::IteratorImpl> mImpl;
public:
    Iterator(std::unique_ptr<impl::IteratorImpl> impl);
    Iterator(Iterator&&) = default;
    Iterator(const Iterator& other);
    ~Iterator();
    Iterator& operator=(Iterator&&) = default;
    Iterator& operator=(const Iterator& other);
    /**
     * @brief Checks whether the iterator is past its last element
     */
    bool done() const;
    /**
     * @brief Moves the iterator to the next position
     *
     * @req !done()
     */
    void next();
    /**
     * @brief Key of the current position
     *
     * @req !done()
     */
    const KeyType& key() const;
    /**
     * @brief Value of the current position
     *
     * @req !done()
     */
    ValueType value() const;
    /**
     * @brief Direction of the iterator
     */
    IteratorDirection direction() const;
};


} // namespace db
} // namespace tell
