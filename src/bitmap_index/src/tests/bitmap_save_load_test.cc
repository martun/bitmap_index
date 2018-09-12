#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <iostream>
#include <memory>
#include <stdint.h>
#include <string>

#include "bitmap.h"

TEST(SmallBitmapSaveLoadTest, small_bitmap_save_load_test) {
	auto bmp = std::make_unique<BitMap>();
	bmp->add(10);
	bmp->add(12);
	bmp->add(17);
	bmp->add(19);
  int expected_size = bmp->get_save_byte_size();
  std::unique_ptr<char[]> buffer(new char[expected_size]);
  bmp->write(buffer.get());

  auto loaded = BitMap::load(buffer.get());
	ASSERT_THAT(bmp->values_as_vector(), 
		::testing::ElementsAre(10, 12, 17, 19));
}

TEST(LargeBitmapSaveLoadTest, large_bitmap_save_load_test) {
	auto bmp = std::make_unique<BitMap>();
	for (int i = 0; i < 1000000; ++i) {
		bmp->add(i * 9);
	}
  int expected_size = bmp->get_save_byte_size();
  std::unique_ptr<char[]> buffer(new char[expected_size]);
  bmp->write(buffer.get());

  auto loaded = BitMap::load(buffer.get());
	ASSERT_TRUE(loaded->contains(99918));
}
