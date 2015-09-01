#pragma once

#include <crossbow/string.hpp>
#include <bdtree/util.h>
#include <bdtree/serializer.h>
#include <vector>

#include <boost/optional.hpp>
#include <boost/variant.hpp>

namespace tell {
namespace db {

namespace impl {

template<class Archiver>
struct SerializeVisitor : public boost::static_visitor<void> {
    Archiver& ar;
    uint8_t* pos;
    SerializeVisitor(Archiver& ar, uint8_t* pos) : ar(ar), pos(pos) {}
    template<class T>
    void operator()(const T& val) const {
        serialize_policy<Archiver, T> policy;
        policy(ar, val, pos);
    }
};

} // namespace impl

class FieldData {
public:
    enum class type : uint8_t {
        Null,
        Bool,
        Int16,
        Uint16,
        Int32,
        Uint32,
        Int64,
        Uint64,
        Float,
        Double,
        String
    };
private:
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
public:
    FieldData() {}
    static FieldData null_value() {
        FieldData res;
        return res;
    }
    template<class T>
    FieldData(T&& data) : mData(std::forward<T>(data)) {}
    bool operator==(const FieldData& other) const;
    bool operator< (const FieldData& other) const;
public: // serialization
    size_t size() const;
    type getType() const;
    template<class Archiver>
    uint8_t* serialize(Archiver& ar, uint8_t* pos) const {
        if (mData == boost::none) {
            auto t = type::Null;
            ar & t;
            return pos;
        }
        ar & getType();
        boost::apply_visitor(impl::SerializeVisitor<Archiver>(ar, pos), mData.get());
        return pos;
    }
    template<class Archiver>
    const uint8_t* deserialize(Archiver& ar, uint8_t* ptr) {
        type t;
        ar & t;
        if (t == type::Null) {
            mData = boost::none;
            return ptr;
        }
        switch (t) {
        case type::Null:
            mData = boost::none;
            break;
        case type::Bool:
            {
                bool res;
                ar & res;
                mData = res;
            }
            break;
        case type::Int16:
            {
                int16_t res;
                ar & res;
                mData = res;
            }
            break;
        case type::Uint16:
            {
                uint16_t res;
                ar & res;
                mData = res;
            }
            break;
        case type::Int32:
            {
                int32_t res;
                ar & res;
                mData = res;
            }
            break;
        case type::Uint32:
            {
                uint32_t res;
                ar & res;
                mData = res;
            }
            break;
        case type::Int64:
            {
                int64_t res;
                ar & res;
                mData = res;
            }
            break;
        case type::Uint64:
            {
                uint64_t res;
                ar & res;
                mData = res;
            }
            break;
        case type::Float:
            {
                float res;
                ar & res;
                mData = res;
            }
            break;
        case type::Double:
            {
                double res;
                ar & res;
                mData = res;
            }
            break;
        case type::String:
            {
                crossbow::string res;
                ar & res;
                mData = res;
            }
            break;
        }
        return ptr;
    }
};

using IndexEntry = std::vector<FieldData>;

} // namespace db
} // namespace tell

namespace bdtree {

template<>
struct null_key<tell::db::IndexEntry> {
    static tell::db::IndexEntry value() {
        return tell::db::IndexEntry{};
    }
};

} // namespace bdtree

template<class Archiver>
struct size_policy<Archiver, tell::db::FieldData>
{
    std::size_t operator() (Archiver& ar, const tell::db::FieldData& entry) const {
        return 1 + entry.size();
    }
};

template<class Archiver>
struct serialize_policy<Archiver, tell::db::FieldData>
{
    uint8_t* operator() (Archiver& ar, const tell::db::FieldData& entry, uint8_t* pos) const {
        return entry.serialize(ar, pos);
    }
};

template<class Archiver>
struct deserialize_policy<Archiver, tell::db::FieldData>
{
    const uint8_t* operator() (Archiver& ar, tell::db::FieldData& out, const uint8_t* ptr) const {
        return out.deserialize(ar, ptr);
    }
};

template<class Archiver>
struct size_policy<Archiver, crossbow::string>
{
    std::size_t operator() (Archiver& ar, const crossbow::string& str) const {
        return 4 + str.size();
    }
};

template<class Archiver>
struct serialize_policy<Archiver, crossbow::string>
{
    uint8_t* operator() (Archiver& ar, const crossbow::string& str, uint8_t* pos) const {
        uint32_t sz = uint32_t(str.size());
        memcpy(pos, str.data(), sz);
        pos += sz;
        return pos;
    }
};

template<class Archiver>
struct deserialize_policy<Archiver, crossbow::string>
{
    const uint8_t* operator() (Archiver& ar, crossbow::string& str, const uint8_t* ptr) {
        uint32_t sz;
        ar & sz;
        str = crossbow::string(reinterpret_cast<const char*>(ptr), sz);
        ptr += sz;
        return ptr;
    }
};

