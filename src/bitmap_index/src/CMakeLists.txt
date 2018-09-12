set(CMAKE_VERBOSE_MAKEFILE on)

add_library(bit_map_index
  value_decomposer.cc
  bitmap_storage.cc
  lmdb_values.cc
  lmdb_wrappers.cc
  bitmap_index_base.cc
  batch_bitmap_index.cc
  batch_bitmap_index_builder.cc
	)

target_link_libraries(bit_map_index
  ioexec
  lmdb
  glog
  common
  boost_context
  boost_system
  boost_filesystem
  folly
  -lpthread -lm)

add_executable(bitmap_index_benchmark 
	perf_benchmark.cc
	value_decomposer.cc
	bitmap_storage.cc)

target_link_libraries(bitmap_index_benchmark 
	bit_map_index)

add_subdirectory(tests)
