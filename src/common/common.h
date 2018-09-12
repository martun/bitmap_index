#ifndef COMMON_COMMON_H
#define COMMON_COMMON_H

#include <string>
#include <vector>
#include <memory>
#include <cstring>
#include <algorithm>

#include "types.h"

struct SnapshotRange {
  SnapshotRange(); // this constructor should not be used
  SnapshotRange(SnapshotID min_snapshot_id, SnapshotID max_snapshot_id);

  bool intersects(const SnapshotRange& other) const;

  SnapshotID min_snapshot_id;
  SnapshotID max_snapshot_id;
};

bool operator == (const SnapshotRange& lhs, const SnapshotRange& rhs);

struct RowGroupInfo {
  RowGroupInfo(); // this constructor should not be used
  RowGroupInfo(RowGroupID id, int32_t num_docs);
      
  RowGroupID id;
  // Total number of documents.
  int32_t num_docs;
};

struct BatchInfo {
  BatchInfo(); // this constructor should not be used
  BatchInfo(BatchID id,
      SnapshotRange range,
      const std::vector<RowGroupInfo>& rg_infos);

  /*
   * "physical" "dense" schema
   * This schema is as dense as possible.
   * It contains only the columns found in batch.
   * Optional is used only if some documents lack some column. Otherwise we use rquired for this column.
   * Numerical data types are as narrow as possible.
   * This schema is derived from the data found in this batch during batch construction.
   */
  // dce::Schema schema;
  BatchID id;
  int64_t batch_size;
  SnapshotRange snapshot_range;
  std::vector<RowGroupInfo> rg_info;

};

/*
 * TODO Make this class a proxy to protobuf/thrift object. Something like this:
 *      class MetabatchInfo {
 *      public:
 *        int num_batches() { return info_.batch_size(); }
 *        State state() { return (State) info_.state(); }
 *
 *        save() { info_.SerializeTo(path);
 *        load() { info_.DeserializeFrom(path); }
 *
 *        class Builder { ... };
 *
 *      private:
 *        proto::MetabatchInfo info_;
 *      };
 *
 */

class MetabatchInfo {
public:
  //TODO replace with builder
  static std::shared_ptr<MetabatchInfo> make(const std::string& path,
      MetabatchID id,
      const std::vector<BatchInfo>& batch_infos);

  // save/load metabatch information from filesystem
  void save() const;
  static std::shared_ptr<MetabatchInfo> load(const std::string& path);
  static std::shared_ptr<MetabatchInfo> empty_metabatch();
  
  static std::string construct_db_path(const std::string& prefix,
      CustomerID customer_id,
      const std::string& table_name,
      PartitionID partition_id);
  static std::string metabatch_info_path(const std::string& path);

  enum State {
    New,
    Completed,
    WrittenToOVS,
    ExportedToLeaf,
  };
  enum IndexState {
    NotIndexed,
    IndexingInProgress,
    Indexed
  };

private:
  // private as it is used when loading metabatch info from file
  MetabatchInfo() = default;
  MetabatchInfo(const std::string& path, MetabatchID id, const std::vector<BatchInfo>& batch_infos);

public: //TODO make private
  std::string metabatch_path;
  MetabatchID id;
  State state;
  IndexState index_state;
  LeafID leaf_id;
  std::vector<BatchInfo> batch_infos;

  // dump metabatch information
  void dump() const;

  const BatchInfo* find_batch_info(BatchID batch_id) const;
};

enum Operator {
  OP_OR,
  OP_AND,
  OP_NOT,
  OP_EQUAL_TO,
  OP_NOT_EQUAL_TO,
  OP_GT,
  OP_GTE,
  OP_LT,
  OP_LTE,
  OP_STRING_ENDS_WITH,
  OP_STRING_NOT_ENDS_WITH,
  OP_STRING_STARTS_WITH,
  OP_STRING_NOT_STARTS_WITH,
  OP_STRING_CONTAINS,
  OP_STRING_NOT_CONTAINS,
  OP_IS_NULL,
  OP_IS_NOT_NULL,
  OP_IN,
  OP_NOT_IN,
  OP_ALL
};

enum ValueType {
  BOOL,
  INT8,
  UINT8,
  INT16,
  UINT16,
  INT32,
  UINT32,
  INT64,
  UINT64,
  FLOAT,
  DOUBLE,
  STRING
};

template<typename T>
struct ValueTypeMap { };

template<>
struct ValueTypeMap<bool> {
  static const ValueType type = ValueType::BOOL;
};

template<>
struct ValueTypeMap<int32_t> {
  static const ValueType type = ValueType::INT32;
};

template<>
struct ValueTypeMap<uint32_t> {
  static const ValueType type = ValueType::UINT32;
};

template<>
struct ValueTypeMap<int64_t> {
  static const ValueType type = ValueType::INT64;
};

template<>
struct ValueTypeMap<uint64_t> {
  static const ValueType type = ValueType::UINT64;
};

template<>
struct ValueTypeMap<float> {
  static const ValueType type = ValueType::FLOAT;
};

template<>
struct ValueTypeMap<double> {
  static const ValueType type = ValueType::DOUBLE;
};

template<>
struct ValueTypeMap<std::string> {
  static const ValueType type = ValueType::STRING;
};

struct ColumnReference {
  /*
   * e.g., AirportStatus.Weather.Temperature
   * It may point to any node in the schema, including intermediate nodes.
   * Some API may restrict ColumnReferences to the terminal AST nodes or 
   * to intermediate nodes only.
   */
  ColumnReference() = default;
  ColumnReference(const std::string& dotted_path, ValueType type) 
    : dotted_path(dotted_path)
    , type(type)
  {
  
  }

  std::string dotted_path;
  ValueType type;
};

const char* strnstr(const char *str1, const char *str2, int len1, int len2);

/*
 * Typical ColumnReference points to some terminal node of AST, e.g.
 * AirportStatus.Weather.Temperature >= 15
 * Notable exception is IS_NULL/IS_NOT_NULL operators: they may point also to 
 * intermediate schema nodes, e.g. AirportStatus.Weather IS_NOT_NULL
 */
struct Predicate {
  ColumnReference column_ref;
  Operator op;
  
  enum Kind {
    UNARY,
    BINARY_CONST,
    BINARY_CONST_STRING,
    BINARY_CONST_VECTOR,
    BINARY_COLUMN
  };
  Kind kind;

  ValueType value_type;
};

struct UnaryPredicate : public Predicate {
  // Operations like NOT, IS_NULL
  UnaryPredicate() {
    kind = Predicate::UNARY;
    value_type = BOOL; // Let's set something
  }
};

template <typename T>
struct BinaryConstPredicate : public Predicate {
  // Operations where right side is a constant
  BinaryConstPredicate(const T& arg) : value(arg) {
    kind = Predicate::BINARY_CONST;
    value_type = ValueTypeMap<T>::type;
  }
  
  template <typename U>
  bool operator()(const U& column_value) const {
    switch (op) {
      case Operator::OP_EQUAL_TO:
        return column_value == value;
      case Operator::OP_NOT_EQUAL_TO:
        return column_value != value;
      case Operator::OP_GT:
        return column_value > value;
      case Operator::OP_GTE:
        return column_value >= value;
      case Operator::OP_LT:
        return column_value < value;
      case Operator::OP_LTE:
        return column_value <= value;
      default:
        throw std::runtime_error("This can't happen: BinaryConstPredicate");
    }
    throw std::runtime_error("This can't happen: BinaryConstPredicate");
  }
  
  T value;
};

struct BinaryConstStringPredicate : public Predicate  {
  // Operations where right side is a constant
  BinaryConstStringPredicate(const std::string& arg) : value(arg) {
    kind = Predicate::BINARY_CONST_STRING;
    value_type = STRING;
  }
  
  bool operator()(const char* column_value, int32_t len) const {
    switch (op) {
      case Operator::OP_STRING_STARTS_WITH:
        return strncmp(column_value, value.c_str(), value.size()) == 0;
      case Operator::OP_STRING_NOT_STARTS_WITH:
        return strncmp(column_value, value.c_str(), value.size()) != 0;
      case Operator::OP_STRING_ENDS_WITH:
        if (value.size() > len) {
          return false;
        }
        return strncmp(column_value + len - value.size(),
            value.c_str(),
            value.size()) == 0;
      case Operator::OP_STRING_NOT_ENDS_WITH:
        if (value.size() > len) {
          return true;
        }
        return strncmp(column_value + len - value.size(),
            value.c_str(),
            value.size()) != 0;
      case Operator::OP_STRING_CONTAINS: {
        return strnstr(column_value, value.c_str(), len, value.size()) != nullptr;
      }
      case Operator::OP_STRING_NOT_CONTAINS: {
        return strnstr(column_value, value.c_str(), len, value.size()) == nullptr;
      }
      default:
        throw std::runtime_error("This can't happen: BinaryConstPredicate ");
    }
    throw std::runtime_error("This can't happen: BinaryConstPredicate");
  }
  
  std::string value;
};

template <typename T>
struct BinaryConstVectorPredicate : public Predicate  {
  // Operations where right side is a constant
  BinaryConstVectorPredicate(std::vector<T>&& arg_vec) : values_vec(arg_vec) {
    kind = Predicate::BINARY_CONST_VECTOR;
    value_type = ValueTypeMap<T>::type;
  }
  
  bool operator()(const T& column_value) const {
    switch (op) {
      case Operator::OP_IN:
        return std::find(values_vec.begin(),
            values_vec.end(), column_value) != values_vec.end();
      case Operator::OP_NOT_IN:
        return std::find(values_vec.begin(),
            values_vec.end(), column_value) == values_vec.end();
      default:
        throw std::runtime_error("This can't happen: BinaryConstVectorPredicate");
    }
    throw std::runtime_error("This can't happen: BinaryConstVectorPredicate");
  }
  
  std::vector<T> values_vec;
};

struct BinaryColumnPredicate : public Predicate  {
  // Operations where right side is a column
  BinaryColumnPredicate(const ColumnReference& column_arg) : column(column_arg) {
    kind = Predicate::BINARY_COLUMN;
    value_type = column_arg.type;
  }
  ColumnReference column;
};

bool is_valid_ip_address(const char *ip_address);

#endif // COMMON_COMMON_H
