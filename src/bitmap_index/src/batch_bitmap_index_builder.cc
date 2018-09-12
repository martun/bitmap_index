#include "batch_bitmap_index_builder.h"

#include <unordered_set>
#include <sstream>
#include <glog/logging.h>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include <limits>
#include <iostream>

using namespace LMDB;


BatchBitmapIndexBuilder::BatchBitmapIndexBuilder(
    std::shared_ptr<BatchInfo> batch_info,
    const std::string& bitmaps_full_path,
    const std::string& lmdb_attribute_mapping_folder_path,
    const std::string& lmdb_bitmap_aux_data_folder_path,
    const std::string& lmdb_bitmap_offsets_folder_path, 
    const std::string& lmdb_bitmap_storage_offsets_folder_path) 
  : batch_info_(batch_info)
  , lmdb_attribute_mapping_folder_path_(lmdb_attribute_mapping_folder_path)
{
  //uint32_t rg_count = batch_info->rg_info.size();
  //uint32_t document_count = 0;
  //for (auto rg_info : batch_info->rg_info) {
  //  document_count += rg_info.num_docs;
  //}

  //uint32_t kBitmapOffsetsMapSize = 
  //    rg_count * 
  //    sqrt(document_count / rg_count) * 
  //    (sizeof(BitmapLMDBID) + sizeof(OffsetRange));
  //uint32_t kStorageOffsetsMapSize = 
  //    rg_count * (sizeof(BitmapStorageLMDBID) + sizeof(OffsetRange));
  //uint32_t kAuxdataMapSize = 
  //    rg_count * (sizeof(BitmapStorageLMDBID) + sizeof(BitmapIndexAuxData));

  //// NOTE(martun): for attribute values the size is dependent on type. For now 
  //// we can assume that we do not support strings, and larges values is uint64_t.
  //uint32_t kAttributeMapSize = 
  //    document_count * 
  //    (sizeof(BitmapLMDBID) + sizeof(uint64_t) + sizeof(uint32_t));

  // TODO(martun): We can come up with a better estimates of LMDB dictionary sizes, but
  // looks like it's not important. We can also gradually grow the size over time. 
  // Setting them to super huge numbers, some forums suggest that's the right choice.
  // Here I found a recommendation to just set it to 1TB: http://lmdb.readthedocs.io/en/release/
  size_t kBitmapOffsetsMapSize = 10 * (size_t)1073741824; // 10 GB.
  size_t kStorageOffsetsMapSize = 10 * (size_t)1073741824; // 10 GB.
  size_t kAuxdataMapSize = 10 * (size_t)1073741824; // 10 GB.
  size_t kAttributeMapSize = 10 * (size_t)1073741824; // 10 GB.

  bitmap_offsets_lmdb_ = std::make_shared<LMDBDictionary<BitmapLMDBID, OffsetRange>>(
      lmdb_bitmap_offsets_folder_path, kBitmapOffsetsMapSize, MDB_WRITEMAP);
  storage_offsets_lmdb_ = std::make_shared<LMDBDictionary<BitmapStorageLMDBID, OffsetRange>>(
      lmdb_bitmap_storage_offsets_folder_path, kStorageOffsetsMapSize, MDB_WRITEMAP);
  aux_data_lmdb_ = std::make_shared<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>>(
      lmdb_bitmap_aux_data_folder_path, kAuxdataMapSize, MDB_WRITEMAP);

  // Open file to save bitmap files to it once all the indexes are built.
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  this->fd_ = open(bitmaps_full_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);

  if (fd_ == -1) {
    std::stringstream ss;
    ss << "Failed to open file " << bitmaps_full_path << " for writing.\n";
    throw std::runtime_error(ss.str());
  }

  current_offset_ = 0;

  // Create LMDB table for attribute value mapping.
  MDB_env *env_ptr;
  LMDB::throw_if_error(
    mdb_env_create(&env_ptr), 
    "function: building batch bitmap index, creating lmdb env for attribute values.");
  LMDB::throw_if_error(mdb_env_set_mapsize(env_ptr, kAttributeMapSize),
    "function: building batch bitmap index, setting map size for attribute values.");
 
  // Create Directory if it doesn't exist yet.
  boost::filesystem::path lmdb_dir(lmdb_attribute_mapping_folder_path);
  if (!boost::filesystem::is_directory(lmdb_dir)) {
    boost::filesystem::create_directory(lmdb_dir);
  }
   
  // Create the environment for writing.
  LMDB::throw_if_error(
    mdb_env_open(env_ptr, lmdb_attribute_mapping_folder_path.c_str(), MDB_WRITEMAP, 0664), 
    "function: building batch bitmap index, opening env.");

  // Env_attr owns env_ptr now.
  env_attr_ = std::make_shared<LMDBEnv>(env_ptr);
}

BatchBitmapIndexBuilder::~BatchBitmapIndexBuilder() {
  close(fd_);
}

void BatchBitmapIndexBuilder::get_basis(
    uint64_t cardinality, BitmapIndexType index_type, std::vector<uint32_t>& basis_out) const {
  if (cardinality < 4) {
    // If cardinality is 1-3, the first element of basis is 1 and does not make sense. 
    basis_out.push_back(cardinality);
  } else {
    if (index_type == BITMAP) {
      basis_out.push_back((uint32_t)sqrt(cardinality));
      basis_out.push_back((uint32_t)(cardinality / basis_out[0]));
      if (cardinality % basis_out[0]) {
        basis_out[1]++;
      }
    } else if (index_type == BITSLICED) { 
      while (cardinality) {
        basis_out.push_back(2);
        cardinality /= 2;
      }
    } else {
      throw "Unsupported index type.";
    }
  }
}

void BatchBitmapIndexBuilder::calculate_bitmap_counts(
    const std::vector<uint32_t>& basis,
    BitmapIndexEncodingType enc_type,
    std::vector<uint32_t>& bitmap_counts_out) const {
  bitmap_counts_out.resize(basis.size());
  for (size_t i = 0; i < basis.size(); ++i) {
    if (enc_type == BitmapIndexEncodingType::EQUALITY) {
      bitmap_counts_out[i] = basis[i];
    }
    else if (enc_type == BitmapIndexEncodingType::INTERVAL) {
      bitmap_counts_out[i] = (basis[i] + 1) / 2;
    }
    else if (enc_type == BitmapIndexEncodingType::RANGE) {
      // For range encoding we don't store the last value,
      // because all the values are <= basis[i] - 1.
      bitmap_counts_out[i] = basis[i] - 1;
    }
  }
}

void BatchBitmapIndexBuilder::save_all() {
  uint32_t total_bytes_written = collect(bmp_futures_).then(
    [=](std::vector<std::shared_ptr<BitmapIndexBase>>& indexes) {
      std::vector<AsyncRetType> save_futures;
      uint32_t offset = current_offset_;
      std::vector<uint32_t> expected_sizes;
      // For each bitmap storage, map it's row_group and column reference to 
      // a pair of offsets in the file.
      std::vector<std::pair<BitmapStorageLMDBID, OffsetRange>> lmdb_entries;

      for (auto index : indexes) {
        BitMapStorage& storage = index->storage();
        // Store in LMDB the offsets and sizes of each bitmap storage.
        
        save_futures.push_back(storage.save(offset));
        uint32_t expected_size = storage.get_total_byte_size();      
        lmdb_entries.push_back(std::make_pair(
          BitmapStorageLMDBID(storage.get_rg_info()->id, storage.get_column_ref()->dotted_path),
          OffsetRange(offset, offset + expected_size)
        ));

        offset += expected_size;
        expected_sizes.push_back(expected_size);
      }
      return collect(save_futures).then([=](std::vector<std::unique_ptr<FilerJobResult>>& results) {
        uint32_t total_bytes_written = 0;
        for (uint32_t i = 0; i < expected_sizes.size(); ++i) {
          if (expected_sizes[i] != results[i]->getIOSize()) {
            std::stringstream error;
            error << "Expected to write " << expected_sizes[i] 
              << " bytes while saving the bitmap storage, but wrote " 
              << results[i]->getIOSize() << " bytes instead.";
            throw std::runtime_error(error.str());
          }
          if (results[i]->getIOResult() != 0) {
            throw std::runtime_error("Failed to save bitmap storage to file.");
          }
          total_bytes_written += expected_sizes[i];
        }
        auto txn = storage_offsets_lmdb_->new_txn();
        // Remember offsets in LMDB.
        storage_offsets_lmdb_->insert(lmdb_entries, txn);
        return total_bytes_written;
      }).get();
    }).get();
  current_offset_ += total_bytes_written;
  bmp_futures_.clear();
}

