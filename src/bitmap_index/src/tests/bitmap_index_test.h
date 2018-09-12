#ifndef _BITMAP_INDEX_UNIT_TEST_H__
#define _BITMAP_INDEX_UNIT_TEST_H__

#include "bitmap_index.h"
#include "batch_bitmap_index_builder.h"

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

class BitmapIndexUnitTest: public testing::Test {
public:
  BitmapIndexUnitTest(); 

  void SetUp();

  void TearDown();  

protected:
  boost::filesystem::path test_dir;

};

template<class T>
std::shared_ptr<BitmapIndex<T>> create_index(
    const std::vector<std::pair<DocumentID, T>>& values, 
    BitmapIndexEncodingType enc_type, 
    BitmapIndexType index_type = BITMAP) {
  auto batch_info = std::make_shared<BatchInfo>();
  batch_info->id = 1;
  
  auto rg_info = std::make_shared<RowGroupInfo>();
  rg_info->id = 2;
  rg_info->num_docs = values.size();
  batch_info->rg_info.push_back(*rg_info);

  BatchBitmapIndexBuilder builder(
      batch_info,
      "test_dir/bitmaps_full_path",
      "test_dir/lmdb_attribute_mapping_folder_path",
      "test_dir/lmdb_bitmap_aux_data_folder_path",
      "test_dir/lmdb_bitmap_offsets_folder_path",
      "test_dir/lmdb_bitmap_storage_offsets_folder_path");

  auto column_ref = std::make_shared<ColumnReference>();
  column_ref->dotted_path = "some.test.dotted.path";
  return builder.build_index(rg_info, column_ref, values, enc_type, index_type); 
}

#endif // _BITMAP_INDEX_UNIT_TEST_H__
