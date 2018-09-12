#include "lmdb_dictionary.h"
#include <lmdb/lmdb.h>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>

namespace LMDB {

template <class Key, class Value>
LMDBDictionary<Key, Value>::const_iterator::const_iterator(
    const LMDBDictionary* lmdb,
    MDB_cursor* cursor,
    const Key& key,
    Value&& value)
  : cursor_(std::make_shared<LMDBCursor>(cursor))
  , value_(std::move(std::make_pair(key, std::move(value)))) 
  , lmdb_(lmdb) {

}

template <class Key, class Value>
LMDBDictionary<Key, Value>::const_iterator::const_iterator(
  LMDBDictionary<Key, Value>::const_iterator&& other)
  : cursor_(std::move(other.cursor_))
  , value_(std::move(other.value_)) {

}

template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator::reference 
LMDBDictionary<Key, Value>::const_iterator::operator*() const {
  return value_;
}

template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator::pointer 
LMDBDictionary<Key, Value>::const_iterator::operator->() const {
  return &value_;
}

template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator& 
LMDBDictionary<Key, Value>::const_iterator::operator++() {
  MDB_val mdb_value;
  MDB_val mdb_key;
  int rc = mdb_cursor_get(cursor_->get(), &mdb_key, &mdb_value, MDB_NEXT);
  if (rc == MDB_NOTFOUND) {
    // Make it equal to end().
    cursor_.reset();
    return *this;
  }
  lmdb_->throw_if_error_dict(rc, "function: iterator++");

  value_.first.from_byte_array(mdb_key.mv_size, reinterpret_cast<const char*>(mdb_key.mv_data));
  value_.second.from_byte_array(mdb_value.mv_size, reinterpret_cast<const char*>(mdb_value.mv_data));

  // Transaction will be closed once iterator is deleted.
  return *this;
}


template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator 
LMDBDictionary<Key, Value>::const_iterator::operator++(int) {
  LMDBDictionary<Key, Value>::const_iterator me(*this);
  ++(*this);
  return std::move(me);
}

template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator& 
LMDBDictionary<Key, Value>::const_iterator::operator--() {
  MDB_val mdb_value;
  MDB_val mdb_key;
  int rc = mdb_cursor_get(cursor_->get(), &mdb_key, &mdb_value, MDB_PREV);
  lmdb_->throw_if_error_dict(rc, "function: iterator--");

  value_.first.from_byte_array(mdb_key.mv_size, reinterpret_cast<const char*>(mdb_key.mv_data));
  value_.second.from_byte_array(mdb_value.mv_size, reinterpret_cast<const char*>(mdb_value.mv_data));

  // Transaction will be closed once iterator is deleted.
  return *this;
}

template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator 
LMDBDictionary<Key, Value>::const_iterator::operator--(int) {
  LMDBDictionary<Key, Value>::const_iterator me(*this);
  ++(*this);
  return me;
}

template <class Key, class Value>
bool LMDBDictionary<Key, Value>::const_iterator::operator==(
    const LMDBDictionary<Key, Value>::const_iterator& other) const {
  if (this->cursor_ && !other.cursor_)
    return false;
  if (!this->cursor_ && other.cursor_)
    return false;
  return (*this)->first == other->first;
}

template <class Key, class Value>
bool LMDBDictionary<Key, Value>::const_iterator::operator!=(
    const LMDBDictionary<Key, Value>::const_iterator& other) const {
  return !(*this == other);
}

template <class Key, class Value>
LMDBDictionary<Key, Value>::LMDBDictionary(
  const std::string& file_path, 
  size_t map_size,
  uint32_t flags)
    : file_path_(file_path)
    , flags_(flags) {
  // Create Directory if it doesn't exist yet.
  boost::filesystem::path dir(file_path);
  if (!boost::filesystem::is_directory(dir)) {
    boost::filesystem::create_directory(dir);
  }

  MDB_env *env_ptr;
  throw_if_error_dict(mdb_env_create(&env_ptr), "function: creating environment");

  throw_if_error_dict(mdb_env_set_mapsize(env_ptr, map_size), "function: setting map size");
  
  // Create the environment for writing.
  throw_if_error_dict(mdb_env_open(env_ptr, file_path.c_str(), flags, 0664), "function: openning environment");

  // Env_ owns env_ptr now.
  env_ = std::make_shared<LMDBEnv>(env_ptr);
}

template <class Key, class Value>
LMDBDictionary<Key, Value>::LMDBDictionary(
  const std::string& file_path,
  std::shared_ptr<LMDBEnv>& env, 
  uint32_t flags) 
  : env_(env)
  , file_path_(file_path)
  , flags_(flags) {
}

template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator LMDBDictionary<Key, Value>::begin(
    std::shared_ptr<LMDBTxn<Key, Value>> txn) {
  MDB_cursor *cursor;
  throw_if_error_dict(mdb_cursor_open(txn->get(), txn->get_dbi(), &cursor), "function: begin opening cursor");

  // Get the value.
  MDB_val mdb_key, mdb_value;
  int rc = mdb_cursor_get(cursor, &mdb_key, &mdb_value, MDB_FIRST);
  if (rc == MDB_NOTFOUND) {
    return end(txn);
  }
  throw_if_error_dict(rc, "function: begin");
  
  Value value;
  value.from_byte_array(mdb_value.mv_size, reinterpret_cast<const char*>(mdb_value.mv_data));
  
  Key found_key;
  found_key.from_byte_array(mdb_key.mv_size, reinterpret_cast<const char*>(mdb_key.mv_data));

  return const_iterator(this, cursor, found_key, std::move(value));   
}

template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator LMDBDictionary<Key, Value>::end(
    std::shared_ptr<LMDBTxn<Key, Value>> txn) const {
  // Return iteartor with a null cursor.
  return LMDBDictionary<Key, Value>::const_iterator();
}

template <class Key, class Value>
void LMDBDictionary<Key, Value>::insert(
    const Key& key, const Value& value, std::shared_ptr<LMDBTxn<Key, Value>> txn) {

  // Convert key and value to MDB.
  MDB_val mdb_key, mdb_value;
  mdb_key.mv_size = key.byte_length();
  mdb_value.mv_size = value.byte_length();
  std::unique_ptr<char[]> key_buffer(new char[mdb_key.mv_size]);
  std::unique_ptr<char[]> value_buffer(new char[mdb_value.mv_size]);
  key.to_byte_array(key_buffer.get());
  value.to_byte_array(value_buffer.get());
  mdb_key.mv_data = key_buffer.get();
  mdb_value.mv_data = value_buffer.get();

  throw_if_error_dict(mdb_put(txn->get(), txn->get_dbi(), &mdb_key, &mdb_value, MDB_NOOVERWRITE), "function: insert");
}

template <class Key, class Value>
void LMDBDictionary<Key, Value>::insert(
    const std::vector<std::pair<Key, Value>>& values,
    std::shared_ptr<LMDBTxn<Key, Value>> txn) {
    std::string(typeid(Key).name()), 
    std::string(typeid(Value).name());
 
  // Convert values to MDB values.
  MDB_val mdb_key, mdb_value;
  uint32_t last_key_size = 0;
  uint32_t last_value_size = 0;
  std::unique_ptr<char[]> key_buffer;
  std::unique_ptr<char[]> value_buffer;

  for (uint32_t i = 0; i < values.size(); ++i) {
    mdb_key.mv_size = values[i].first.byte_length();
    mdb_value.mv_size = values[i].second.byte_length();
    if (last_key_size < mdb_key.mv_size) {
      key_buffer.reset(new char[mdb_key.mv_size]);
      last_key_size = mdb_key.mv_size;
    }
    if (last_value_size < mdb_value.mv_size) {
      value_buffer.reset(new char[mdb_value.mv_size]);
      last_value_size = mdb_value.mv_size;
    }
    values[i].first.to_byte_array(key_buffer.get());
    values[i].second.to_byte_array(value_buffer.get());
    mdb_key.mv_data = key_buffer.get();
    mdb_value.mv_data = value_buffer.get();
    throw_if_error_dict(mdb_put(
      txn->get(), txn->get_dbi(), &mdb_key, &mdb_value, MDB_NOOVERWRITE), "function: bulk insert");
  }
}

template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator 
LMDBDictionary<Key, Value>::find(const Key& key, std::shared_ptr<LMDBTxn<Key, Value>> txn) {
    std::string(typeid(Key).name()), 
    std::string(typeid(Value).name());

  MDB_cursor *cursor;
  throw_if_error_dict(mdb_cursor_open(txn->get(), txn->get_dbi(), &cursor), "function: find");
  
  // Convert key to mdb_key.
  MDB_val mdb_key;
  mdb_key.mv_size = key.byte_length();
  std::unique_ptr<char[]> key_buffer(new char[mdb_key.mv_size]);
  key.to_byte_array(key_buffer.get());
  mdb_key.mv_data = key_buffer.get();
  
  // Find in the DB.
  MDB_val mdb_value;
  int rc = mdb_cursor_get(cursor, &mdb_key, &mdb_value, MDB_SET);
  if (rc == MDB_NOTFOUND) {
    return end(txn);
  }
  throw_if_error_dict(rc, "function: find");
  
  Value value;
  value.from_byte_array(mdb_value.mv_size, reinterpret_cast<const char*>(mdb_value.mv_data));

  // Transaction will be closed once iterator is deleted.
  return const_iterator(this, cursor, key, std::move(value));
}

template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator 
LMDBDictionary<Key, Value>::lower_bound(const Key& key, std::shared_ptr<LMDBTxn<Key, Value>> txn) {
    std::string(typeid(Key).name()), 
    std::string(typeid(Value).name());
 
  MDB_cursor *cursor;
  throw_if_error_dict(mdb_cursor_open(txn->get(), txn->get_dbi(), &cursor), "function: lower_bound, openning cursor");

  // Convert key to MDB_val.
  MDB_val mdb_key;
  mdb_key.mv_size = key.byte_length();
  std::unique_ptr<char[]> key_buffer(new char[mdb_key.mv_size]);
  key.to_byte_array(key_buffer.get());
  mdb_key.mv_data = key_buffer.get();

  // Get the value.
  MDB_val mdb_value;
  int rc = mdb_cursor_get(cursor, &mdb_key, &mdb_value, MDB_SET_RANGE);
  if (rc == MDB_NOTFOUND) {
    return end(txn);
  }
  throw_if_error_dict(rc, "function: lower_bound");
  
  Value value;
  value.from_byte_array(mdb_value.mv_size, reinterpret_cast<const char*>(mdb_value.mv_data));
  
  Key found_key;
  found_key.from_byte_array(mdb_key.mv_size, reinterpret_cast<const char*>(mdb_key.mv_data));

  return const_iterator(this, cursor, found_key, std::move(value));   
}

template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator 
LMDBDictionary<Key, Value>::upper_bound(const Key& key, std::shared_ptr<LMDBTxn<Key, Value>> txn) {
  // Get the lower bound and move it up until a value unequal to key is found.
  const_iterator iter = lower_bound(key, txn);
  while (iter->first == key) {
    ++iter;
  }
  return iter;
}

template <class Key, class Value>
typename LMDBDictionary<Key, Value>::const_iterator 
LMDBDictionary<Key, Value>::reverse_upper_bound(const Key& key, std::shared_ptr<LMDBTxn<Key, Value>> txn) {
  // Get the lower bound and move it down, until a value unequal to key is found.
  const_iterator iter = lower_bound(key, txn);
  while (iter->first == key) {
    --iter;
  }
  return iter;
}

template <class Key, class Value>
LMDBDictionary<Key, Value>::~LMDBDictionary() {

}

template <class Key, class Value>
std::shared_ptr<LMDBTxn<Key, Value>> LMDBDictionary<Key, Value>::new_txn() {
    std::string(typeid(Key).name()), 
    std::string(typeid(Value).name());
  return std::make_shared<LMDBTxn<Key, Value>>(this, env_, flags_);
}

// Throws a proper exception if rc contains an LMDB error.
template <class Key, class Value>
void LMDBDictionary<Key, Value>::throw_if_error_dict(int rc, std::string additional_info) const {
  throw_if_error(rc, "[Database location: " + file_path_ + "] " + additional_info);
}

} // end of namespace LMDB.
