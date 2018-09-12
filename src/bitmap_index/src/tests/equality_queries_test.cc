#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <iostream>
#include <stdint.h>
#include <string>

#include "bitmap_index.h"
#include "bitmap_index_test.h"

typedef BitMap::FUTURE_BMP FUTURE_BMP;
typedef BitMap::FUTURE_CONST_BMP FUTURE_CONST_BMP;

template<class T>
void run_equality_test(
    boost::filesystem::path& test_dir,
    const std::vector<std::pair<uint32_t, T>>& values, 
    const T lookup_value,
    const std::vector<uint32_t>& expected_values) {
  BitmapIndexType index_types[] = {BITMAP, BITSLICED};
  BitmapIndexEncodingType encoding_types[] = {
      BitmapIndexEncodingType::INTERVAL, 
      BitmapIndexEncodingType::RANGE, 
      BitmapIndexEncodingType::EQUALITY};
  for (auto encoding_type : encoding_types) {
    for (auto index_type : index_types) {
      boost::filesystem::create_directory(test_dir);
 
	    auto index = create_index(values, encoding_type, index_type);

	    std::shared_ptr<BitMap> bmp_result = index->lookup(lookup_value).get();
	    std::vector<uint32_t> result_row_ids = bmp_result->values_as_vector();
	    
	    EXPECT_EQ(result_row_ids, expected_values);

	    boost::filesystem::remove_all(test_dir);
    }
  }
}

TEST_F(BitmapIndexUnitTest, strings_lookup_test) {
	// A vector of (Value, row_id) pairs.
  std::vector<std::pair<uint32_t, std::string>> values =
        { { 15, "armenia" }, { 16, "india" }, { 17, "japan" },
          { 19, "india" }, { 25, "korea" }, { 30, "USA" } };
  std::vector<uint32_t> expected_result_india = {16, 19};

  run_equality_test<std::string>(test_dir, values, "india", expected_result_india);

  std::vector<uint32_t> expected_result_armenia = {15};

  run_equality_test<std::string>(test_dir, values, "armenia", expected_result_armenia);
}

TEST_F(BitmapIndexUnitTest, interger_lookup_test) {
	// A vector of (Value, row_id) pairs.
  std::vector<std::pair<uint32_t, uint32_t>> values =
        { { 15, 4 }, { 16, 5 }, { 17, 4 },
          { 19, 5 }, { 25, 7 }, { 30, 4 }, {40, 8} };
  std::vector<uint32_t> expected_result_1 = {15, 17, 30};
  run_equality_test<uint32_t>(test_dir, values, (uint32_t)4, expected_result_1);

  std::vector<uint32_t> expected_result_2 = {40};
  run_equality_test<uint32_t>(test_dir, values, 8, expected_result_2);
}

TEST_F(BitmapIndexUnitTest, interger_lookup_test_2) {
	// A vector of (Value, row_id) pairs.
  std::vector<std::pair<uint32_t, uint32_t>> values =
    { { 15, 400000000 }, { 16, 500000000 }, { 17, 400000000 },
		  { 19, 500000000 }, { 25, 700000000 }, { 30, 400000000 }, {40, 800000000} };

  std::vector<uint32_t> expected_result_1 = {15, 17, 30};
  run_equality_test<uint32_t>(test_dir, values, (uint32_t)400000000, expected_result_1);

  std::vector<uint32_t> expected_result_2 = {40};
  run_equality_test<uint32_t>(test_dir, values, (uint32_t)800000000, expected_result_2);
}

