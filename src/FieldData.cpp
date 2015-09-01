#include  <telldb/FieldData.hpp>
#include <crossbow/logger.hpp>


namespace tell {
namespace db {

namespace {

struct EqualVisitor : public boost::static_visitor<bool>
{
    template<class T, class U>
    bool operator()(const T&, const U&) const {
        // this must never happen
        LOG_ERROR("Type error, comparing two different types");
        return false;
    }

    template<class T>
    bool operator()(const T& a, const T& b) const {
        return a == b;
    }
};

struct LessVisitor : public boost::static_visitor<bool>
{
    template<class T, class U>
    bool operator()(const T&, const U&) const {
        // this must never happen
        LOG_ERROR("Type error, comparing two different types");
        return false;
    }

    template<class T>
    bool operator()(const T& a, const T& b) const {
        return a < b;
    }
};

template<class T>
struct CalcSize
{
    size_t operator() (T a) {
        return sizeof(a);
    }
};

template<>
struct CalcSize<crossbow::string>
{
    size_t operator() (const crossbow::string& a) {
        return 4 + a.size();
    }
};

struct SizeVisitor : public boost::static_visitor<size_t>
{
    template<class T>
    size_t operator()(T a) const {
        CalcSize<T> sz;
        return sz(a);
    }
};

} // anonymous namespace

bool FieldData::operator==(const FieldData& other) const
{
    if (mData == boost::none || other.mData == boost::none) {
        return false;
    }
    return boost::apply_visitor(EqualVisitor(), mData.get(), other.mData.get());
}

bool FieldData::operator< (const FieldData& other) const
{
    if (mData == boost::none || other.mData == boost::none) {
        return false;
    }
    return boost::apply_visitor(LessVisitor(), mData.get(), other.mData.get());
}

size_t FieldData::size() const
{
    if (mData == boost::none) {
        return 0;
    }
    return boost::apply_visitor(SizeVisitor(), mData.get());
}

auto FieldData::getType() const -> type {
    if (mData == boost::none) {
        return type::Null;
    }
    struct FieldTypeVisitor : public boost::static_visitor<type> {
        type operator() (bool val) const {
            return type::Bool;
        }
        type operator() (int16_t val) const {
            return type::Int16;
        }
        type operator() (uint16_t val) const {
            return type::Uint16;
        }
        type operator() (int32_t val) const {
            return type::Int32;
        }
        type operator() (uint32_t val) const {
            return type::Uint32;
        }
        type operator() (int64_t val) const {
            return type::Int64;
        }
        type operator() (uint64_t val) const {
            return type::Uint64;
        }
        type operator() (float val) const {
            return type::Float;
        }
        type operator() (double val) const {
            return type::Double;
        }
        type operator() (const crossbow::string& val) const {
            return type::String;
        }
    };
    return boost::apply_visitor(FieldTypeVisitor(), mData.get());
}

} // namespace db
} // namespace tell

