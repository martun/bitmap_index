#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "bitmap_index_test.h"

BitmapIndexUnitTest::BitmapIndexUnitTest() 
  : test_dir("test_dir")
{
}

void BitmapIndexUnitTest::SetUp() {
  // Create the test folder.
  if (!boost::filesystem::is_directory(test_dir)) {
    boost::filesystem::create_directory(test_dir);
  }
}

void BitmapIndexUnitTest::TearDown() {
  // Delete created test folder.
  if (boost::filesystem::is_directory(test_dir)) {
    boost::filesystem::remove_all(test_dir);
  }
}

