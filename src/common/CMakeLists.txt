include_directories(.)

add_library(common 
  src/bitmap.cc
  src/common.cc
        src/toolkit.cc
)

target_link_libraries(common
  roaring
  ioexec
  boost_filesystem
  lmdb)

