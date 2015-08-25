#pragma once

#include <crossbow/string.hpp>
#include <bdtree/util.h>
#include <bdtree/serializer.h>
#include <vector>

#include <boost/optional.hpp>
#include <boost/variant.hpp>

namespace tell {
namespace db {

class FieldData {
    boost::optional<boost::variant<bool,
        int16_t,
        uint16_t,
        int32_t,
        uint32_t,
        int64_t,
        uint64_t,
        float,
        double,
        crossbow::string>> mData;
    FieldData() {}
public:
    static FieldData null_value() {
        FieldData res;
        return res;
    }
    template<class T>
    FieldData(T&& data) : mData(std::forward<T>(data)) {}
    bool operator==(const FieldData& other) const;
    bool operator< (const FieldData& other) const;
};

} // namespace db
} // namespace tell

namespace bdtree {

template<>
struct null_key<tell::db::FieldData> {
    static tell::db::FieldData value() {
        return tell::db::FieldData::null_value();
    }
};

template<>
struct null_key<std::vector<tell::db::FieldData>> {
    static std::vector<tell::db::FieldData> value() {
        return std::vector<tell::db::FieldData>();
    }
};

} // namespace bdtree

template<class Archiver>
struct size_policy<Archiver, tell::db::FieldData>
{
    std::size_t operator() (Archiver& ar, const tell::db::FieldData& field) const {
        return 0;
    }
};

template<class Archiver>
struct serialize_policy<Archiver, tell::db::FieldData>
{
    uint8_t* operator() (Archiver& ar, const tell::db::FieldData& data, uint8_t* pos) const {
        return pos;
    }
};

template<class Archiver>
struct deserialize_policy<Archiver, tell::db::FieldData>
{
    const uint8_t* operator() (Archiver& ar, tell::db::FieldData& out, const uint8_t* ptr) const {
        return ptr;
    }
};

