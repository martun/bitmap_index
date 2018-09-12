#include "batch_bitmap_index.h"
#include "toolkit.h"

#include <functional>

BatchBitmapIndex::BatchBitmapIndex(
    std::shared_ptr<BatchInfo> batch_info, 
    const std::string& bitmaps_full_path,
    const std::string& lmdb_attribute_mapping_folder_path,
    const std::string& lmdb_bitmap_aux_data_folder_path,
    const std::string& lmdb_bitmap_offsets_folder_path,
    const std::string& lmdb_bitmap_storage_offsets_folder_path) 
  : batch_info_(batch_info) 
  , bitmap_offsets_lmdb_(std::make_shared<LMDBDictionary<BitmapLMDBID, OffsetRange>>(lmdb_bitmap_offsets_folder_path, 0, MDB_RDONLY))
  , storage_offsets_lmdb_(std::make_shared<LMDBDictionary<BitmapStorageLMDBID, OffsetRange>>(lmdb_bitmap_storage_offsets_folder_path, 0, MDB_RDONLY))
  , aux_data_lmdb_(std::make_shared<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>>(lmdb_bitmap_aux_data_folder_path, 0, MDB_RDONLY))
  , lmdb_attribute_mapping_folder_path_(lmdb_attribute_mapping_folder_path)
{
  // Open file to read bitmaps from it.
  mode_t mode = S_IRUSR | S_IRGRP | S_IROTH;
  this->fd_ = open(bitmaps_full_path.c_str(), O_RDONLY, mode);

  if (fd_ == -1) {
    std::stringstream ss;
    ss << "Failed to open file " << bitmaps_full_path << " for reading.\n";     
    throw std::runtime_error(ss.str()); 
  }

  // Create LMDB table for attribute value mapping.
  MDB_env *env_ptr;
  LMDB::throw_if_error(
    mdb_env_create(&env_ptr), 
    "function: batch bitmap, env create.");
  LMDB::throw_if_error(
    mdb_env_set_mapsize(env_ptr, 0),
    "function: batch bitmap, setting map size.");

  // Create Directory if it doesn't exist yet.
  boost::filesystem::path lmdb_dir(lmdb_attribute_mapping_folder_path);
  if (!boost::filesystem::is_directory(lmdb_dir)) {
    boost::filesystem::create_directory(lmdb_dir);
  }

  // Create the environment for writing.
  LMDB::throw_if_error(
      mdb_env_open(env_ptr, lmdb_attribute_mapping_folder_path.c_str(), MDB_RDONLY, 0664), 
      "function: batch bitmap, opening environment.");

  // Env_attr owns env_ptr now.
  env_attr_ = std::make_shared<LMDBEnv>(env_ptr);
}

// A wrapper over BitmapIndex<T> which satisfies IDocumentIndex interface.
template <class T>
class BitmapIndexImpl : public expreval::IDocumentIndex {
public:
  BitmapIndexImpl(std::shared_ptr<BitmapIndex<T>> index)
    : index_(index) 
  {
  }

  folly::Future<expreval::IndexResult> find_candidate_documents(
      std::shared_ptr<Predicate> predicate) override {
  
    std::function<folly::Future<std::shared_ptr<BitMap>>()> query_index;

    if (predicate->kind == Predicate::BINARY_CONST) {
      query_index = [predicate, this]() -> folly::Future<std::shared_ptr<BitMap>> {
      
        auto bin_predicate = std::static_pointer_cast<BinaryConstPredicate<T>>(predicate);
        switch(bin_predicate->op) {
          case Operator::OP_EQUAL_TO:
            return index_->lookup(bin_predicate->value);
          case Operator::OP_NOT_EQUAL_TO:
            return index_->not_equals(bin_predicate->value);
          case Operator::OP_GT:
            return index_->greater(bin_predicate->value, BitmapIndexBase::IntervalFlags::OPEN);
          case Operator::OP_GTE:
            return index_->greater(bin_predicate->value, 
                                   BitmapIndexBase::IntervalFlags::INCLUDE_LEFT);
          case Operator::OP_LT:
            return index_->lesser(bin_predicate->value, 
                                  BitmapIndexBase::IntervalFlags::OPEN);
          case Operator::OP_LTE:
            return index_->lesser(bin_predicate->value, 
                                  BitmapIndexBase::IntervalFlags::INCLUDE_RIGHT);
          default:
            return folly::makeFuture<std::shared_ptr<BitMap>>(std::shared_ptr<BitMap>(nullptr));
        }
      };
    } else if (predicate->kind == Predicate::UNARY &&
        (predicate->op == OP_IS_NULL || predicate->op == OP_IS_NOT_NULL)) { 
      query_index = [predicate, this]() -> folly::Future<std::shared_ptr<BitMap>> {
      
        switch(predicate->op) {
          case Operator::OP_IS_NOT_NULL:
            return index_->get_not_null();
          case Operator::OP_IS_NULL:
            // TODO(martun): support is_null after having the list of all document ids. 
            // In case we assume the range is [0..n], we can support it now,
            // yet it's better to have some "fresh_bitmap" per row_group,
            // or on the batch level.
            return folly::makeFuture<std::shared_ptr<BitMap>>(std::shared_ptr<BitMap>(nullptr));
          default:
            return folly::makeFuture<std::shared_ptr<BitMap>>(std::shared_ptr<BitMap>(nullptr));
        }
      };

    } else {
      // Return nullptr, result to a full scan.
      query_index = []() -> folly::Future<std::shared_ptr<BitMap>> {
        return folly::makeFuture<std::shared_ptr<BitMap>>(std::shared_ptr<BitMap>(nullptr));
      };
    }

    return query_index().then([](std::shared_ptr<BitMap> values){
      if (values) {
        return expreval::IndexResult(expreval::IA_EXACT, values);
      } else {
        return expreval::IndexResult(expreval::IA_NONE, nullptr);
      }
    });
  }

private:
  std::shared_ptr<BitmapIndex<T>> index_;

};

bool BatchBitmapIndex::load_storage_offset_range(
    uint32_t rg_id, 
    std::shared_ptr<ColumnReference> column_ref,
    OffsetRange& offsets_out) const {
  auto txn = storage_offsets_lmdb_->new_txn();
  BitmapStorageLMDBID storage_id(rg_id, column_ref->dotted_path);
  auto iter = storage_offsets_lmdb_->find(storage_id, txn);
  // Check if there's an index.
  if (iter == storage_offsets_lmdb_->end(txn) || (iter->first != storage_id))
    return false;
  offsets_out = iter->second;
  return true;
}

// Loads aux data from lmdb. 
std::shared_ptr<BitmapIndexAuxData> get_aux_data(
    std::shared_ptr<RowGroupInfo>& rg_info,
    std::shared_ptr<ColumnReference>& column_ref,
    std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>>& aux_data_lmdb) {
  // Load aux_data from LMDB.
  auto txn = aux_data_lmdb->new_txn();
  BitmapStorageLMDBID storage_id(rg_info->id, column_ref->dotted_path);
  auto iter = aux_data_lmdb->find(storage_id, txn);
  if (iter == aux_data_lmdb->end(txn)) {
    return nullptr;
  }

  return std::make_shared<BitmapIndexAuxData>(iter->second);
}

// Returns index for the given row group.
Future<std::shared_ptr<expreval::IDocumentIndex>> 
BatchBitmapIndex::get_bitmap_index(
    uint32_t rg_id, std::shared_ptr<ColumnReference> column_ref) {
  auto rg_info = std::make_shared<RowGroupInfo>(batch_info_->rg_info[rg_id]);

  // Offsets of the current bitmap index in the batch bitmap file.
  OffsetRange offsets;
  if (!load_storage_offset_range(rg_id, column_ref, offsets)) {
    // Fail silently, most probably the index was not created for this column.
    return makeFuture(std::shared_ptr<expreval::IDocumentIndex>(nullptr));
  }

  auto aux_data = get_aux_data(rg_info, column_ref, aux_data_lmdb_);
  if (!aux_data) {
    // Fail silently, most probably the index was not created for this column.
    return makeFuture(std::shared_ptr<expreval::IDocumentIndex>(nullptr));
  }
  auto storage_future = BitMapStorage::load(
    rg_info, column_ref, fd_, offsets, aux_data->bitmap_counts, bitmap_offsets_lmdb_, INT_MAX, true);

#define CASE(TYPE, VALUE_TYPE) \
  case ValueType::TYPE: \
    { \
    auto index = BitmapIndex<VALUE_TYPE>::load( \
      rg_info, \
      column_ref, \
      std::move(storage_future), \
      std::make_shared<LMDBDictionary<AttributeValue<VALUE_TYPE>, LMDBValue<uint32_t>>>( \
          lmdb_attribute_mapping_folder_path_, env_attr_, MDB_RDONLY), \
      aux_data_lmdb_); \
    return index.then([=](std::shared_ptr<BitmapIndex<VALUE_TYPE>> index) { \
      return std::shared_ptr<expreval::IDocumentIndex>(new BitmapIndexImpl<VALUE_TYPE>(index)); \
    }); \
    }

  // Need to check somewhere, if an index was created for given column_ref, and what's it's type.
  switch(column_ref->type) {
      CASE(BOOL, bool)
      CASE(UINT8, uint8_t)
      CASE(INT8, int8_t)
      CASE(UINT16, uint16_t)
      CASE(INT16, int16_t)
      CASE(UINT32, uint32_t)
      CASE(INT32, int32_t)
      CASE(UINT64, uint64_t)
      CASE(INT64, int64_t)
      CASE(FLOAT, float)
      CASE(DOUBLE, double)
      CASE(STRING, std::string)
    default:
      throw "Unsupported column type for bitmap document indexing.";
  }
}

