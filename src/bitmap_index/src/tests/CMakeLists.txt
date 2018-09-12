add_executable(equality_queries_test
  test_main.cc
  bitmap_index_test.cc
  equality_queries_test.cc)

target_link_libraries(equality_queries_test 
  bit_map_index
  gtest)

add_test(
		NAME equality_queries_test
		COMMAND equality_queries_test
)


add_executable(bitmap_save_load_test
	test_main.cc
  bitmap_index_test.cc
	bitmap_save_load_test.cc)

target_link_libraries(bitmap_save_load_test
	bit_map_index 
	gtest)


add_test(
		NAME bitmap_save_load_test
		COMMAND bitmap_save_load_test
)


add_executable(range_queries_test
	test_main.cc
  bitmap_index_test.cc
	range_queries_test.cc)

target_link_libraries(range_queries_test 
	bit_map_index 
	gtest)

add_test(
		NAME range_queries_test
		COMMAND range_queries_test
)


add_executable(lesser_queries_test
	test_main.cc
  bitmap_index_test.cc
	lesser_queries_test.cc)

target_link_libraries(lesser_queries_test 
	bit_map_index 
	gtest)

add_test(
		NAME lesser_queries_test
		COMMAND lesser_queries_test
)


add_executable(larger_queries_test
	test_main.cc
  bitmap_index_test.cc
	larger_queries_test.cc)

target_link_libraries(larger_queries_test 
	bit_map_index 
	gtest)

add_test(
		NAME larger_queries_test
		COMMAND larger_queries_test
)


add_executable(full_save_load_test
	test_main.cc
  bitmap_index_test.cc
	full_save_load_test.cc)

target_link_libraries(full_save_load_test 
	bit_map_index 
	gtest)

add_test(
		NAME full_save_load_test
		COMMAND full_save_load_test
)

add_executable(end_to_end_test
  test_main.cc
  bitmap_index_test.cc
  end_to_end_test.cc)

target_link_libraries(end_to_end_test 
	bit_map_index 
	gtest)

add_test(
		NAME end_to_end_test
		COMMAND end_to_end_test
)


add_executable(bitmap_storage_save_load_test
	test_main.cc
  bitmap_index_test.cc
	bitmap_storage_save_load_test.cc)

target_link_libraries(bitmap_storage_save_load_test
	bit_map_index 
	gtest)

add_test(
		NAME bitmap_storage_save_load_test
		COMMAND bitmap_storage_save_load_test
)


add_library(testing-lib test_main.cc)

