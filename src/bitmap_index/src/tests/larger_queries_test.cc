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
void run_greater_test(
    boost::filesystem::path& test_dir,
    const std::vector<std::pair<uint32_t, T>>& values, 
    const T range_start,
    BitmapIndexBase::IntervalFlags flags,
    const std::vector<uint32_t>& expected_values, 
    bool skip_bitmap_index = false) {
  BitmapIndexType index_types[] = {BITMAP, BITSLICED};
  BitmapIndexEncodingType encoding_types[] = {
      BitmapIndexEncodingType::INTERVAL, BitmapIndexEncodingType::RANGE};
  for (auto encoding_type : encoding_types) {
    for (auto index_type : index_types) {
      // For some tests with large numbers we want to test only bitslice indexes,
      // as bitmap indexes are too slow.
      if (skip_bitmap_index && index_type == BITMAP)
        continue; 
      boost::filesystem::create_directory(test_dir);
 
	    auto index = create_index(values, encoding_type, index_type);

	    std::shared_ptr<BitMap> bmp_result = index->greater(range_start, flags).get();
	    std::vector<uint32_t> result_row_ids = bmp_result->values_as_vector();
	    
	    EXPECT_EQ(result_row_ids, expected_values);

	    boost::filesystem::remove_all(test_dir);
    }
  }
}

TEST_F(BitmapIndexUnitTest, integer_value_larger_query_test) {
	// A vector of (row_id, value) pairs.
	std::vector<std::pair<uint32_t, uint32_t>> values =
		{ { 15, 4 }, { 16, 5 }, { 17, 4 },
		  { 19, 5 }, { 25, 7 }, { 30, 4 }, {40, 8} };
  std::vector<uint32_t> expected_result = {25, 40};

  run_greater_test<uint32_t>(
      test_dir, values, (uint32_t)7, BitmapIndexBase::IntervalFlags::CLOSED, expected_result);
  std::vector<uint32_t> expected_result2 = {15, 16, 17, 19, 25, 30, 40};
  run_greater_test<uint32_t>(
      test_dir, values, (uint32_t)4, BitmapIndexBase::IntervalFlags::CLOSED, expected_result2);
}

TEST_F(BitmapIndexUnitTest, large_integer_value_larger_query_test) {
	// A vector of (row_id, value) pairs.
	std::vector<std::pair<uint32_t, uint32_t>> values =
		{ { 15, 0 }, { 16, 50 }, { 17, 10 },
		  { 19, 50 }, { 25, 30 }, { 30, 40 }, 
      { 40, 20 } 
    };
  std::vector<uint32_t> expected_result = {16, 19, 25, 30};
  run_greater_test<uint32_t>(
      test_dir, values, (uint32_t)30, BitmapIndexBase::IntervalFlags::CLOSED, expected_result);
}

TEST_F(BitmapIndexUnitTest, integer_value_large_larger_query_over_small_data_test) {
	// A vector of (row_id, value) pairs.
	std::vector<std::pair<uint32_t, uint64_t>> values =
		{ { 15, 4 }, { 16, 5 }, { 17, 4 },
		  { 19, 5 }, { 25, 7 }, { 30, 4 }, {40, 8} };
  std::vector<uint32_t> expected_result = {};
  run_greater_test<uint64_t>(
      test_dir, values, (uint64_t)1010, 
      BitmapIndexBase::IntervalFlags::CLOSED, expected_result);
}

TEST_F(BitmapIndexUnitTest, larger_integer_value_larger_query_test) {
	// A vector of (Value, row_id) pairs.
	std::vector<std::pair<uint32_t, uint32_t>> values;
	for (int i = 0; i < 10000; ++i) {
		values.push_back(std::make_pair(i*i, i % 10));
	}
  
  std::vector<uint32_t> expected_result;
  for (int i = 0; i < 10000; ++i) {
    if (i % 10 >= 5) {
  		expected_result.push_back(i*i);
    }
	}
  run_greater_test<uint32_t>(
      test_dir, values, (uint32_t)5, 
      BitmapIndexBase::IntervalFlags::CLOSED, expected_result);
}

TEST_F(BitmapIndexUnitTest, integer_value_larger_query_corner_case_test) {
	// A vector of (Value, row_id) pairs.
	std::vector<std::pair<uint32_t, uint32_t>> values =
		{ { 15, 4 }, { 16, 5 }, { 17, 4 }, 
		  { 19, 5 }, { 25, 7 }, { 30, 4 }, { 40, 8 } };
  std::vector<uint32_t> expected_result = {15, 16, 17, 19, 25, 30, 40};
  run_greater_test<uint32_t>(
      test_dir, values, (uint64_t)3, 
      BitmapIndexBase::IntervalFlags::CLOSED, expected_result);
}

TEST_F(BitmapIndexUnitTest, low_cardinality_larger_test) {
	// A vector of (Value, row_id) pairs.
	std::vector<std::pair<uint32_t, uint32_t>> values =
		{ { 15, 4 }, { 16, 5 } };
  std::vector<uint32_t> expected_result = {15, 16};
  run_greater_test<uint32_t>(
      test_dir, values, (uint64_t)4, 
      BitmapIndexBase::IntervalFlags::CLOSED, expected_result);
}

TEST_F(BitmapIndexUnitTest, low_cardinality_larger_test_2) {
	// A vector of (Value, row_id) pairs.
	std::vector<std::pair<uint32_t, uint32_t>> values =
		  { { 15, 4 }, { 16, 5 } };
  std::vector<uint32_t> expected_result = {15, 16};
  run_greater_test<uint32_t>(
      test_dir, values, (uint64_t)2, 
      BitmapIndexBase::IntervalFlags::CLOSED, expected_result);
}

TEST_F(BitmapIndexUnitTest, low_cardinality_test_3) {
	// A vector of (Value, row_id) pairs.
	std::vector<std::pair<uint32_t, uint32_t>> values =
		{ { 15, 4 }, { 16, 4 } };
  std::vector<uint32_t> expected_result = {15, 16};
  run_greater_test<uint32_t>(
      test_dir, values, (uint32_t)4, 
      BitmapIndexBase::IntervalFlags::CLOSED, expected_result);
}

TEST_F(BitmapIndexUnitTest, large_integers_test) {
	// A vector of (Value, row_id) pairs.
	std::vector<std::pair<uint32_t, uint64_t>> values =
		{ { 15, 40000000000000ull }, { 17, 50000000000000ull }, { 18, 80000000000000ull }, 
      { 19, 80000000000000ull }, { 25, 100000000000000ull }, { 7, 10000000000000ull }};
  std::vector<uint32_t> expected_result = {18, 19, 25};
  run_greater_test<uint64_t>(
      test_dir, values, 80000000000000ull, 
      BitmapIndexBase::IntervalFlags::CLOSED, expected_result, true);
}

TEST_F(BitmapIndexUnitTest, negative_large_integers_test) {
	// A vector of (Value, row_id) pairs.
	std::vector<std::pair<uint32_t, int64_t>> values =
		{ { 15, -40000000000000ll }, { 17, -50000000000000ll }, { 18, -80000000000000ll }, 
      { 19, -80000000000000ll }, { 25, -100000000000000ll }, { 7, -10000000000000ll }};
  std::vector<uint32_t> expected_result = {7, 15, 17};
  run_greater_test<int64_t>(
      test_dir, values, -50000000000000ll,
      BitmapIndexBase::IntervalFlags::CLOSED, expected_result, true);
}

TEST_F(BitmapIndexUnitTest, larger_negative_integer_value_range_query_test) {
	// A vector of (Value, row_id) pairs.
	std::vector<std::pair<uint32_t, int32_t>> values;
	for (int i = 0; i < 10000; ++i) {
		values.push_back(std::make_pair(i*i, -(i % 10)));
	}
  
  std::vector<uint32_t> expected_result;
  for (int i = 0; i < 10000; i++) {
    if (-(i % 10) >= -7) {
  		expected_result.push_back(i*i);
    }
	}
  run_greater_test<int32_t>(
      test_dir, values, (int32_t)-7, 
      BitmapIndexBase::IntervalFlags::CLOSED, expected_result);
}

TEST_F(BitmapIndexUnitTest, negative_integer_value_range_query_test) {
	// A vector of (row_id, value) pairs.
	std::vector<std::pair<uint32_t, int32_t>> values =
		{ { 15, -4 }, { 16, -5 }, { 17, -4 },
		  { 19, -5 }, { 25, -7 }, { 30, -4 }, {40, -3} };
  std::vector<uint32_t> expected_result = {15, 17, 30, 40};

  run_greater_test<int32_t>(
      test_dir, values, (int32_t)-4, BitmapIndexBase::IntervalFlags::CLOSED, expected_result);
  std::vector<uint32_t> expected_result2 = {15, 16, 17, 19, 30, 40};
  run_greater_test<int32_t>(
      test_dir, values, (int32_t)-7, BitmapIndexBase::IntervalFlags::INCLUDE_RIGHT, expected_result2);
}

TEST_F(BitmapIndexUnitTest, positive_and_negative_integer_value_range_query_test) {
	// A vector of (row_id, value) pairs.
	std::vector<std::pair<uint32_t, int32_t>> values =
		{ { 15, -4 }, { 16, -5 }, { 17, -4 },
		  { 19, -5 }, { 25, -7 }, { 30, -4 }, {40, -8}, 
      { 18, 4 }, { 20, 5 }, { 22, 4 },
		  { 27, 5 }, { 35, 7 }, { 37, 4 }, {44, 8}};
  std::vector<uint32_t> expected_result = {15, 16, 17, 18, 19, 20, 22, 27, 30, 35, 37, 44};

  run_greater_test<int32_t>(
      test_dir, values, (int32_t)-6, BitmapIndexBase::IntervalFlags::CLOSED, expected_result);
}

TEST_F(BitmapIndexUnitTest, large_integers_test_2) {
	// A vector of (Value, row_id) pairs.
	std::vector<std::pair<uint32_t, int64_t>> values =
		{ { 15, 902379575ll }, { 17, 902379569ll }, { 18, 902379571ll }, 
      { 19, 902379514ll }, { 25, 902379585ll }, { 7, 902379574ll }};
  std::vector<uint32_t> expected_result = {7, 15, 18, 25};
  run_greater_test<int64_t>(
      test_dir, values, 902379569ll, 
      BitmapIndexBase::IntervalFlags::OPEN, expected_result, true);
}
