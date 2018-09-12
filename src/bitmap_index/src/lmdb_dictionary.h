#ifndef DICTIONARY_STORAGE_H__
#define DICTIONARY_STORAGE_H__

#include <iterator>
#include <memory>
#include <vector>
#include "lmdb_wrappers.h"

namespace LMDB {

// LMDB based dictionary storage.
// Can be used to map any value to any other value. The requirement is both 
// Key and Value classes must have byte_length() and "to_byte_array" functions for serialization
// and a 'from_byte_array' function for deserialization.
// Key also needs to have an equality operator, inequality is measured on the serialized bytes.
template <class Key, class Value>
class LMDBDictionary {
public:

  /**
   * Const Iterator over LMDB. No normal const_iterator is provided, because writing is not thread safe.
   */
  class const_iterator {
  public:
    /** @brief Constructor.
     *  @param[in] lmdb LMDB instance to which we belong. Used for better log messsages.
     *  @param[in] txn Transaction on which the cursor was created. This way a transaction is clsoed once all the iterators created inside the txn are deleted.
     *  @param[in] cursor LMDB cursor. Iterator takes over ownership.
     *  @param[in] value First result(LMDB returns it on the cursor creation).
     */
    explicit const_iterator(
      const LMDBDictionary* lmdb,
      MDB_cursor* cursor,
      const Key& key,
      Value&& value);

    const_iterator(const_iterator&&);

    // To be used by LMDBDictionary to create an end const_iterator with null pointers.
    const_iterator() = default;

    // Even though it's not normal, we're unable to copy an iterator.
    const_iterator(const const_iterator&) = default;
    const_iterator& operator=(const const_iterator&) = default;

    typedef std::forward_iterator_tag const_iterator_category;
    typedef const std::pair<Key, Value> value_type;
    typedef const std::pair<Key, Value>* pointer;
    typedef const std::pair<Key, Value>& reference;
    typedef std::ptrdiff_t difference_type;

    reference operator*() const;
    pointer operator->() const;
    const_iterator& operator++();
    const_iterator operator++(int);
    const_iterator& operator--();
    const_iterator operator--(int);

    // WARNING: Equality operators are mainly for comparison with end().
    // If 2 iterators were created the exact same way, they are not equal now,
    // as LMDB does not provide an equality operator for it's cursors.
    bool operator==(const const_iterator& other) const;
    bool operator!=(const const_iterator& other) const;

  private:

    // LMDB cursor. NOTE: We NEVER delete the cursor, we mdb_cursor_close it, which 
    // is done on destruction of the iterator.
    std::shared_ptr<LMDBCursor> cursor_;

    // Transaction on which the cursor is created.
    // Once iterator is not necessary any more, it's transaction will get deleted.
    std::shared_ptr<LMDBTxn<Key, Value>> txn_;

    // We must store the current key and value, 
    // because mdb_cursor_get returns them.
    std::pair<Key, Value> value_;

    // LMDB instance to which we belong. Used for better log messsages.
    const LMDBDictionary* lmdb_;
  };

  // Make sure not to open the same environment multiple times.
  LMDBDictionary(
    const std::string& file_path, 
    size_t map_size, 
    uint32_t flags = MDB_WRITEMAP);

  // Constructs the dictionary using the given environment.
  LMDBDictionary(
    const std::string& file_path,
    std::shared_ptr<LMDBEnv>& env,
    uint32_t flags = MDB_WRITEMAP);

  LMDBDictionary(const LMDBDictionary& other) = default;

  ~LMDBDictionary();

  // Creates a new transaction on the environment.
  std::shared_ptr<LMDBTxn<Key, Value>> new_txn();

  /** Unable to make this const, because creation of new iterator may make change to env_.
   * returns a const_iterator to the first/smallest element there is.
   */
  const_iterator begin(std::shared_ptr<LMDBTxn<Key, Value>> txn);

  // returns a null const_iterator.
  const_iterator end(std::shared_ptr<LMDBTxn<Key, Value>> txn) const;

  // Inserts given <key, value> pair.
  void insert(const Key& key, const Value& value, std::shared_ptr<LMDBTxn<Key, Value>> txn);

  // @brief Bulk insertion.
  void insert(const std::vector<std::pair<Key, Value>>& values, std::shared_ptr<LMDBTxn<Key, Value>> txn);

  // @brief Returns value for the given key.
  // @returns - true, if the value was found, false otherwise.
  const_iterator find(const Key& key, std::shared_ptr<LMDBTxn<Key, Value>> txn);

  // @brief Finds all the keys, in case not found, returns a null pointer. 
  void bulk_find(const std::vector<Key>& keys, std::vector<std::shared_ptr<Value>>& values_out, 
      std::shared_ptr<LMDBTxn<Key, Value>> txn);

  // Look for the first value strictly less than the key.
  // @returns - false, if all the keys are strictly smaller than the given one, otherwise returns true.
  const_iterator reverse_upper_bound(const Key& key, std::shared_ptr<LMDBTxn<Key, Value>> txn);

  // Look for the first value >= than the key.
  // @returns - falsei, if all the keys are strictly smaller than the given one, otherwise returns true.
  const_iterator lower_bound(const Key& key, std::shared_ptr<LMDBTxn<Key, Value>> txn);

  // Look for the first value strictly greater than the key.
  // If all the values are strictly less than given value, returns false.
  const_iterator upper_bound(const Key& key, std::shared_ptr<LMDBTxn<Key, Value>> txn);

  /* \brief Throws a proper exception if rc contains an LMDB error.
   *        Made this function a member, so we can print as informative error message as possible.
   * \param[in] rc The LMDB error code.
   * \param[in] additional_info Some additional information to be added to the error message.
   */
  void throw_if_error_dict(int rc, std::string additional_info = "") const;

private:

  // LMDB environment.
  std::shared_ptr<LMDBEnv> env_;

  // File path is stored mostly for debugging.
  std::string file_path_;

  // Flags which were used when opening the database. The same flags will be used
  // while creating transactions.
  uint32_t flags_;

};

} // end of namespace LMDB.

// Include template function implementations.
#include "lmdb_dictionary.hpp"

#endif // DICTIONARY_STORAGE_H__
