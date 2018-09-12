#ifndef BATCH_BITMAP_INDEX_BUILDER_H__
#define BATCH_BITMAP_INDEX_BUILDER_H__

#include "bitmap_index.h"
#include "bitmap_index_base.h"
#include "common.h"
#include <lmdb/lmdb.h>
#include "src/lmdb_dictionary.h"
#include "src/lmdb_values.h"
#include "index_utils.h"
#include "bitmap_index.h"
#include "bitmap_index_base.h"
#include "src/lmdb_wrappers.h"

#include <limits>
#include <memory>
#include <vector>

#include <folly/futures/Future.h>
#include <folly/executors/Async.h>

using namespace folly::futures;
using folly::makeFuture;
using folly::Promise;
using namespace LMDB;

// Contains bitmap indexes for each row_group inside a given batch.
class BatchBitmapIndexBuilder {
public:

  typedef folly::Future<std::unique_ptr<FilerJobResult>> AsyncRetType;


  // Initializes all the required LMDB environments. Make sure not to open the same DB from another place.
  BatchBitmapIndexBuilder(
    std::shared_ptr<BatchInfo> batch_info, 
    const std::string& bitmaps_full_path,
    const std::string& lmdb_attribute_mapping_folder_path,
    const std::string& lmdb_bitmap_aux_data_folder_path,
    const std::string& lmdb_bitmap_offsets_folder_path,
    const std::string& lmdb_bitmap_storage_offsets_folder_path);
  
  // Closes file descriptor.
  ~BatchBitmapIndexBuilder();

  // Adds another index for some row_group and column.
  template<class T>
  void add_index(
    std::shared_ptr<RowGroupInfo>& rg_info,
    std::shared_ptr<ColumnReference>& column_ref,
    const std::vector<std::pair<DocumentID, T>>& values,
    BitmapIndexEncodingType enc_type = BitmapIndexEncodingType::INTERVAL, 
    BitmapIndexType index_type = BITSLICED);

  // Must be called after a few calls to 'AddIndex' are made.
  // Waits for all the futures in 'bmp_futures' to finish,
  // Then saves all the created indexes, removes their futures.
  void save_all();

  // Builds a bitmap index with given values.
  template<class T>
  std::shared_ptr<BitmapIndex<T>> build_index(
      std::shared_ptr<RowGroupInfo>& rg_info,
      std::shared_ptr<ColumnReference>& column_ref,
      const std::vector<std::pair<DocumentID, T>>& values,
      BitmapIndexEncodingType enc_type,
      BitmapIndexType index_type = BITSLICED);

  template<class T>
  std::shared_ptr<BitmapIndexBase> build_index_base(
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    const std::vector<std::pair<DocumentID, T>>& values,
    BitmapIndexEncodingType enc_type,
    BitmapIndexType index_type = BITSLICED);

private:
  // Computes cardinality. Later can be replaced by hyperloglog.
  // Or in case of using some binning, the number of bins goes here.
  template<class T>
  uint32_t estimate_cardinality(const std::vector<std::pair<DocumentID, T>>& values) const;   

  // Returns basis to be used by the index.
  // Currently returns [sqrt(cardinality), cardinality / sqrt(cardinality)], later
  // for bitsliced index may return a vector of 2s.
  void get_basis(uint64_t cardinality, BitmapIndexType index_type, std::vector<uint32_t>& basis_out) const;

  // Returns number of bitmaps per atribute, based on the encoding type.
  void calculate_bitmap_counts(
    const std::vector<uint32_t>& basis,
    BitmapIndexEncodingType enc_type,
    std::vector<uint32_t>& bitmap_counts_out) const;

  /** @brief Based on type and cardinality values configures the index and i
   *         fills up the aux_data and basis.
   *  
   *  \param[out] aux_data_out All auxiliary data related to the index.
   *  \param[out] basis_out The basis to be used for attribute value decomposition.
   */
  template<class T>
  typename std::enable_if<std::is_integral<T>::value>::type configure(
    const std::vector<std::pair<DocumentID, T>>& values,
    BitmapIndexEncodingType enc_type,
    BitmapIndexType index_type,
    std::shared_ptr<BitmapIndexAuxData> aux_data_out, 
    std::vector<uint32_t>& basis_out) const;

  template<class T>
  typename std::enable_if<!std::is_integral<T>::value>::type configure(
    const std::vector<std::pair<DocumentID, T>>& values,
    BitmapIndexEncodingType enc_type,
    BitmapIndexType index_type,
    std::shared_ptr<BitmapIndexAuxData> aux_data_out, 
    std::vector<uint32_t>& basis_out) const;


private:

  // LMDB environment for atribute value mapping. Used by all the row groups and all the column predicates. We are unable to create the LMDBDictionary, because we don't know it's type yet.
  std::shared_ptr<LMDBEnv> env_attr_;

  // Information on current batch.
  std::shared_ptr<BatchInfo> batch_info_;

  // All the futures of created BitMap indexes.
  std::vector<Future<std::shared_ptr<BitmapIndexBase>>> bmp_futures_;

  // Path to the file we want to save to.
  std::string full_path_;

   // For each bitmap id will return a pair of offsets in the file.
   std::shared_ptr<LMDBDictionary<BitmapLMDBID, OffsetRange>> bitmap_offsets_lmdb_;

  // For each bitmap storage will return a pair of offsets in the file.
  std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, OffsetRange>> storage_offsets_lmdb_;

  // For each bitmap storage will return the auxiliary data stored with it.
  std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>> aux_data_lmdb_;

  // File descriptor to write to.
  int fd_;

  // Until which offset we have filled the file.
  uint32_t current_offset_;

  std::string lmdb_attribute_mapping_folder_path_;

};

template<class T>
typename std::enable_if<!std::is_integral<T>::value>::type 
BatchBitmapIndexBuilder::configure(
  const std::vector<std::pair<DocumentID, T>>& values,
  BitmapIndexEncodingType enc_type,
  BitmapIndexType index_type,
  std::shared_ptr<BitmapIndexAuxData> aux_data_out, 
  std::vector<uint32_t>& basis_out) const {

  aux_data_out->enc_type = enc_type;

  // Compute cardinality. Later can be replaced by hyperloglog.
  // Or in case of using some binning, the number of bins goes here.
  aux_data_out->cardinality = estimate_cardinality(values);

  aux_data_out->use_value_mapping = true;
  aux_data_out->min_mapped_value = 0;
  aux_data_out->max_mapped_value = aux_data_out->cardinality;
  get_basis((uint64_t)aux_data_out->cardinality, index_type, basis_out);
}

template<class T>
typename std::enable_if<std::is_integral<T>::value>::type 
BatchBitmapIndexBuilder::configure(
  const std::vector<std::pair<DocumentID, T>>& values,
  BitmapIndexEncodingType enc_type,
  BitmapIndexType index_type,
  std::shared_ptr<BitmapIndexAuxData> aux_data_out, 
  std::vector<uint32_t>& basis_out) const {
  aux_data_out->enc_type = enc_type;

  // Compute cardinality. Later can be replaced by hyperloglog.
  // Or in case of using some binning, the number of bins goes here.
  aux_data_out->cardinality = estimate_cardinality(values);

  if (aux_data_out->cardinality > values.size() / 10) {
    aux_data_out->use_value_mapping = false;
  } else {
    aux_data_out->use_value_mapping = true;
  }
  if (aux_data_out->use_value_mapping) {
    aux_data_out->min_mapped_value = 0;
    aux_data_out->max_mapped_value = aux_data_out->cardinality;
    get_basis((uint64_t)aux_data_out->cardinality, index_type, basis_out);
  } else {
    aux_data_out->min_mapped_value = (int64_t)std::numeric_limits<T>::max();
    aux_data_out->max_mapped_value = (int64_t)std::numeric_limits<T>::min();
    for (auto value : values) {
      if (value.second > aux_data_out->max_mapped_value) {
        aux_data_out->max_mapped_value = value.second;
      }
      if (value.second < aux_data_out->min_mapped_value) {
        aux_data_out->min_mapped_value = value.second;
      }
    }
    get_basis(aux_data_out->max_mapped_value - aux_data_out->min_mapped_value + 1, 
        index_type, basis_out);
  }
}

template<class T>
std::shared_ptr<BitmapIndex<T>> BatchBitmapIndexBuilder::build_index(
  std::shared_ptr<RowGroupInfo>& rg_info,
  std::shared_ptr<ColumnReference>& column_ref,
  const std::vector<std::pair<DocumentID, T>>& values,
  BitmapIndexEncodingType enc_type,
  BitmapIndexType index_type) {

  auto aux_data = std::make_shared<BitmapIndexAuxData>();
  std::vector<uint32_t> basis;
  
  configure(values, enc_type, index_type, aux_data, basis);
 
  // Used to decompose integer values.
  aux_data->vd.reset(new ValueDecomposer(basis));

  calculate_bitmap_counts(basis, aux_data->enc_type, aux_data->bitmap_counts);



  // Create a storage for the new index.
  std::shared_ptr<BitMapStorage> storage = BitMapStorage::create(
      rg_info, column_ref, fd_, aux_data->bitmap_counts, bitmap_offsets_lmdb_);

  // Create an LMDB instance for the current type. 
  auto attr_values_lmdb = std::make_shared<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>>(
      lmdb_attribute_mapping_folder_path_, env_attr_);
  return BitmapIndex<T>::create(
    rg_info, column_ref, std::move(storage), attr_values_lmdb, aux_data_lmdb_, aux_data, values);
}

template<class T>
std::shared_ptr<BitmapIndexBase> BatchBitmapIndexBuilder::build_index_base(
  std::shared_ptr<RowGroupInfo> rg_info,
  std::shared_ptr<ColumnReference> column_ref,
  const std::vector<std::pair<DocumentID, T>>& values,
  BitmapIndexEncodingType enc_type, 
  BitmapIndexType index_type) {
  return std::static_pointer_cast<BitmapIndexBase>(build_index(
    rg_info, column_ref, values, enc_type, index_type));
}

template<class T>
void BatchBitmapIndexBuilder::add_index(
  std::shared_ptr<RowGroupInfo>& rg_info,
  std::shared_ptr<ColumnReference>& column_ref,
  const std::vector<std::pair<DocumentID, T>>& values,
  BitmapIndexEncodingType enc_type, 
  BitmapIndexType index_type) {

  bmp_futures_.push_back(folly::async([=]() {
    return this->build_index_base(rg_info, column_ref, values, enc_type, index_type);
  }));
}

template<class T>
uint32_t BatchBitmapIndexBuilder::estimate_cardinality(
    const std::vector<std::pair<DocumentID, T>>& values) const {
  std::unordered_set<T> unique_values;
  for (size_t i = 0; i < values.size(); ++i) {
    unique_values.insert(values[i].second);
  }
  return unique_values.size();
}

#endif // BATCH_BITMAP_INDEX_BUILDER_H__

