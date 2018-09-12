#ifndef _LMDB_WRAPPERS_H__
#define _LMDB_WRAPPERS_H__

#include <lmdb/lmdb.h>
#include <memory>

namespace LMDB {

// Throws a proper exception if rc contains an LMDB error.
void throw_if_error(int rc, std::string additional_info = "");

// A simple wrapper over MDB_env, to make sure it's closed on destruction.
class LMDBEnv {
public:
    LMDBEnv(MDB_env* env);
    ~LMDBEnv();
    MDB_env* get();

private:
    MDB_env* env_;

};

template <class Key, class Value>
class LMDBDictionary;

// Wrapper over the LMDB transaction. Automatically commited on destruction.
template <class Key, class Value>
class LMDBTxn {
public:
    /** Constructs the wrapper over lmdb transaction.
     *  \param[in] env The LMDB environment, to be shared between all the transactions.
     *  \param[in] flags LMDB flags, value of MDB_RDONLY must be used to read only txn.
     */
  LMDBTxn(LMDBDictionary<Key, Value>* lmdb, std::shared_ptr<LMDBEnv>& env, int flags = 0)
    : lmdb_(lmdb)
    , env_(env)
  {
    lmdb_->throw_if_error_dict(
      mdb_txn_begin(env->get(), NULL, flags, &txn_), 
      "function: creating transaction.");
    lmdb_->throw_if_error_dict(

    //NOTE(martun): flags must be set to 0 in next line, on mdb_dbi_open call.
    mdb_dbi_open(txn_, NULL, 0, &dbi_), 
      "function: creating transaction-> openning database");
  }

  ~LMDBTxn() {
    if (txn_) {
      commit();
    }
  }
    
  MDB_txn* get() {
    return txn_;
  }

  MDB_dbi& get_dbi() {
    return dbi_;
  }

  // Commits the transaction.
  void commit() {
    lmdb_->throw_if_error_dict(mdb_txn_commit(txn_), "function: committing transaction.");
    mdb_dbi_close(env_->get(), dbi_);
    txn_ = nullptr;
  }
  
  // Aborts the transaction, s.t. all the write operations are canceled.
  void abort() {
    mdb_txn_abort(txn_);
    mdb_dbi_close(env_->get(), dbi_);
    txn_ = nullptr;
  }

private:

  // Database in which the transaction is opened.
  LMDBDictionary<Key, Value>* lmdb_;

  MDB_txn *txn_;
  MDB_dbi dbi_;

  // Pointer to a shared environment used by multiple transactions.
  std::shared_ptr<LMDBEnv> env_;
};

class LMDBCursor {
public:
    LMDBCursor(MDB_cursor* cursor_);
    MDB_cursor* get();

    // Closes cursor on destruction.
    ~LMDBCursor();

private:
    // LMDB cursor. NOTE: We NEVER delete the cursor, we mdb_cursor_close it, which 
    // is done on destruction of the iterator.
    MDB_cursor* cursor_;

};

} // end of namespace LMDB.

#endif  // _LMDB_WRAPPERS_H__
