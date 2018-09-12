#ifndef BATCH_BITMAP_INDEX_H__
#define BATCH_BITMAP_INDEX_H__

#include "common.h"
#include "bitmap.h"
#include "bitmap_index.h"

#include <memory>
#include <vector>
#include <unordered_map>
#include "index_interfaces.h"
#include <folly/futures/Future.h>

using namespace folly::futures;
using folly::makeFuture;

// Contains bitmap indexes for each row_group inside a given batch.
class BatchBitmapIndex {
public:
  BatchBitmapIndex(
    std::shared_ptr<BatchInfo> batch_info,
    const std::string& bitmaps_full_path,
    const std::string& lmdb_attribute_mapping_folder_path,
    const std::string& lmdb_bitmap_aux_data_folder_path,
    const std::string& lmdb_bitmap_offsets_folder_path,
    const std::string& lmdb_bitmap_storage_offsets_folder_path); 

	// Returns future of index for the given row group. Loads it completely from the disk on future completion.
	Future<std::shared_ptr<expreval::IDocumentIndex>> get_bitmap_index(
			uint32_t rg_id, std::shared_ptr<ColumnReference> column_ref);

private:

  bool load_storage_offset_range(
    uint32_t rg_id, 
    std::shared_ptr<ColumnReference> column_ref, 
    OffsetRange& offsets_out) const;

private:

  // LMDB environment for atribute value mapping. Used by all the row groups and all the column predicates. We are unable to create the LMDBDictionary, because we don't know it's type yet.
  std::shared_ptr<LMDBEnv> env_attr_;

  // Information on current batch.
  std::shared_ptr<BatchInfo> batch_info_;

  // Path to the file containing all the bitmaps.
  std::string full_path_;

  // For each bitmap id will return a pair of offsets in the file.
  std::shared_ptr<LMDBDictionary<BitmapLMDBID, OffsetRange>> bitmap_offsets_lmdb_;

  // For each bitmap storage will return a pair of offsets in the file.
  std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, OffsetRange>> storage_offsets_lmdb_;

  // For each bitmap storage will return the auxiliary data stored with it.
  std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>> aux_data_lmdb_;

  // File descriptor to read from.
  int fd_;

  std::string lmdb_attribute_mapping_folder_path_;

};

#endif // BATCH_BITMAP_INDEX_H__

