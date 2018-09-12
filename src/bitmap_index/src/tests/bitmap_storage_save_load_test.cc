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

TEST_F(BitmapIndexUnitTest, bitmap_storage_save_load_test) {
  auto rg_info = std::make_shared<RowGroupInfo>();
  rg_info->id = 1;
  
  auto column_ref = std::make_shared<ColumnReference>();
  column_ref->dotted_path = "some.test.dotted.path";
  column_ref->type = ValueType::UINT32;
  auto bitmap_offsets_lmdb = std::make_shared<LMDBDictionary<BitmapLMDBID, OffsetRange>>(
      "test_dir/lmdb_bitmap_offsets_folder_path", 1000000000, MDB_WRITEMAP);
  
  std::vector<uint32_t>  bitmap_counts = {2, 2, 2};

  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  int fd = open("test_dir/bitmaps_full_path", O_WRONLY | O_CREAT | O_TRUNC, mode);

  std::shared_ptr<BitMapStorage> created_storage = BitMapStorage::create(
    rg_info,
    column_ref,
    fd,
    bitmap_counts,
    bitmap_offsets_lmdb);
  created_storage->add_to_bitmap(0, 0, 28);
  created_storage->add_to_bitmap(0, 0, 158);
  created_storage->add_to_all_values_bitmap(15);
  created_storage->add_to_all_values_bitmap(27);
  created_storage->add_to_all_values_bitmap(37);

  // Save the created bitmap storage starting from offset 0.
  std::unique_ptr<FilerJobResult> save_result = created_storage->save(0).get();
  
  // Close and re-open the file.
  close(fd);
  
  mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
  int read_fd = open("test_dir/bitmaps_full_path", O_RDONLY, read_mode);

  OffsetRange offsets(0, save_result->getIOSize());

  // Load the storage back.
  auto loaded_storage = BitMapStorage::load(
      rg_info, column_ref, read_fd, offsets, bitmap_counts, bitmap_offsets_lmdb, INT_MAX, true).get();
  auto all_values_bmp = loaded_storage->load_all_values_bitmap().get();
  auto bitmap_0_0 = loaded_storage->load_bitmap(0, 0).get();

  ASSERT_THAT(all_values_bmp->values_as_vector(), ::testing::ElementsAre(15, 27, 37));
  ASSERT_THAT(bitmap_0_0->values_as_vector(), ::testing::ElementsAre(28, 158));
  close(read_fd);
}

