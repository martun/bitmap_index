#ifndef __ATTRIBUTE_H__
#define __ATTRIBUTE_H__

#include <limits>
#include <string>
#include <cassert>
#include <vector>
#include <map>
#include <memory>
#include <iostream>
#include <sstream>
#include <fstream>
#include <boost/serialization/map.hpp>
#include <boost/serialization/nvp.hpp>
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/smart_ptr/make_shared.hpp>

namespace GaneshaDB {

class IndexAttributeBase {
	friend class boost::serialization::access;
public:
	enum class Type {
		INT32,
		INT64,
		DOUBLE,
	};

	IndexAttributeBase(): name_(""), type_(Type::INT32) {}
	
	IndexAttributeBase(std::string&& name, Type type) :
			name_(std::move(name)), type_(type) {

	}

	virtual ~IndexAttributeBase() {

	}

	virtual const std::string& name() const {
		return name_;
	}

	virtual Type type() const {
		return type_;
	}
	
private:
	std::string name_;
	Type        type_;
private:
	template<class Archive>
	void serialize(Archive & ar, const unsigned int version)
	{
		ar & boost::serialization::make_nvp("name_", name_);
		ar & boost::serialization::make_nvp("type_", type_);
	}
};
BOOST_SERIALIZATION_ASSUME_ABSTRACT(IndexAttributeBase);
template <
	typename T,
	typename = typename std::enable_if<std::is_arithmetic<T>::value, T>::type
> class IndexAttribute : public IndexAttributeBase {
	friend class boost::serialization::access;
public:
	IndexAttribute():IndexAttributeBase(),nvalues_(0), sum_(0) {}
		
	IndexAttribute(std::string&& name, IndexAttributeBase::Type type) :
			IndexAttributeBase(std::forward<std::string>(name), type),
			nvalues_(0), sum_(0) {

	}

	std::pair<double, double> ToDouble() const {
		return std::make_pair((double) min_, (double) max_);
	}
	void printData() const {
		VLOG(10)<< " Column: " << name() << " data: (" << min_ << " ," << max_ << ")";
	}
	void AddValue(const T& value) {
		if (min_ > value) {
			min_ = value;
		}

		if (max_ < value) {
			max_ = value;
		}

		sum_    += value;
		nvalues_++;
	}

	void MinMax(T& min, T& max) const {
		min = min_;
		max = max_;
	}

	T Sum() const {
		return sum_;
	}

	uint64_t NumValues() const {
		return nvalues_;
	}

private:
	uint64_t nvalues_{0};
	T        sum_;
	T        min_{std::numeric_limits<T>::max()};
	T        max_{std::numeric_limits<T>::min()};
private:
	template<class Archive>
	void serialize(Archive & ar, const unsigned int version)
	{
		ar.template register_type<IndexAttributeBase>();
		ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(IndexAttributeBase);
		ar & boost::serialization::make_nvp("nvalues_", nvalues_);
		ar & boost::serialization::make_nvp("sum_", sum_);
		ar & boost::serialization::make_nvp("min_", min_);
		ar & boost::serialization::make_nvp("max_", max_);
	}
};
} // namespace

BOOST_CLASS_EXPORT_KEY(GaneshaDB::IndexAttributeBase);

BOOST_CLASS_EXPORT_KEY(GaneshaDB::IndexAttribute<double>);
BOOST_CLASS_EXPORT_KEY(GaneshaDB::IndexAttribute<int32_t>);
BOOST_CLASS_EXPORT_KEY(GaneshaDB::IndexAttribute<int64_t>);


#endif
