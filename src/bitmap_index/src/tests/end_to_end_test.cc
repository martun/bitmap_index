#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <iostream>
#include <stdint.h>
#include <string>
#include "batch_bitmap_index_builder.h"
#include "batch_bitmap_index.h"
#include "bitmap_index.h"
#include "bitmap_index_test.h"
#include "common.h"
#include "index_interfaces.h"

typedef BitMap::FUTURE_BMP FUTURE_BMP;
typedef BitMap::FUTURE_CONST_BMP FUTURE_CONST_BMP;

TEST_F(BitmapIndexUnitTest, batch_parallel_creation_test) {
	   
  auto batch_info = std::make_shared<BatchInfo>();
  batch_info->id = 1;

  for (uint32_t rg = 0; rg < 9; ++rg) {
    RowGroupInfo rg_info;
    rg_info.id = rg;
    batch_info->rg_info.push_back(rg_info);
  }
 
  BatchBitmapIndexBuilder builder(
      batch_info,
      "test_dir/bitmaps_full_path",
      "test_dir/lmdb_attribute_mapping_folder_path",
      "test_dir/lmdb_bitmap_aux_data_folder_path",
      "test_dir/lmdb_bitmap_offsets_folder_path",
      "test_dir/lmdb_bitmap_storage_offsets_folder_path");

  auto column_ref = std::make_shared<ColumnReference>();
  column_ref->dotted_path = "some.test.dotted.path";
  column_ref->type = ValueType::UINT32;
  
  for (uint32_t i = 0; i < batch_info->rg_info.size(); ++i) {
    std::vector<std::pair<DocumentID, uint32_t>> values;
    for (uint32_t j = 0; j < 16000; ++j) {
      // Let the value of document j be "10 * j + i".
      values.push_back(std::make_pair(j, 10 * j + i));
    }
    auto rg_info = std::make_shared<RowGroupInfo>(batch_info->rg_info[i]);
    builder.add_index(
        rg_info, 
        column_ref, 
        values, 
        BitmapIndexEncodingType::INTERVAL);
  }
  
  // Wait for all the indexes to be stored in parallel.
  builder.save_all();

  BatchBitmapIndex index(
      batch_info,
      "test_dir/bitmaps_full_path",
      "test_dir/lmdb_attribute_mapping_folder_path",
      "test_dir/lmdb_bitmap_aux_data_folder_path",
      "test_dir/lmdb_bitmap_offsets_folder_path",
      "test_dir/lmdb_bitmap_storage_offsets_folder_path");

  auto rg_index = index.get_bitmap_index(0, column_ref).get();


  auto predicate = std::make_shared<BinaryConstPredicate<uint32_t>>(15999 * 10 );
  predicate->column_ref = *column_ref;
  predicate->op = OP_GTE;

  expreval::IndexResult result = rg_index->find_candidate_documents(predicate).get();
  std::vector<uint32_t> result_GT_160000 = result.values()->values_as_vector();
  ASSERT_THAT(result_GT_160000, ::testing::ElementsAre(15999));
}

