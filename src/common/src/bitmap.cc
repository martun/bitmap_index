#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <sstream>
#include <thread>
#include <algorithm>
#include <iostream>

#include "bitmap.h"
#include <DiskIOThreadPool.h>
#include "IOExecutor.h"
#include "FilerJob.h"
#include "EventFD.h"

typedef folly::Future<std::unique_ptr<FilerJobResult>> AsyncRetType;
typedef BitMap::FUTURE_BMP FUTURE_BMP;
typedef BitMap::FUTURE_CONST_BMP FUTURE_CONST_BMP;


BitMap::BitMap()
  : modified(false)
{ }

// Adds a given value to the bitmap.
void BitMap::add(uint32_t value) {
  modified = true;
  roaring.add(value);
}

// Batch adds values to the current bitmap.
void BitMap::add(const std::vector<uint32_t>& values) {
  add(&values[0], values.size());
}

void BitMap::add(const uint32_t* values, uint32_t size) {
  modified = true;
  roaring.addMany(size, values);
}



// Returns if given value is in the bitmap.
bool BitMap::contains(uint32_t x) const {
  return roaring.contains(x);
}

/**
* Moves the content of the provided bitmap, and
* discards the current content.
*/
BitMap& BitMap::operator=(BitMap &&r) {
  modified = true;
  roaring = std::move(r.roaring);
  return *this;
}

/**
* Compute the intersection between the current bitmap and the provided
* bitmap,
* writing the result in the current bitmap. The provided bitmap is not
* modified.
*/
BitMap& BitMap::operator&=(const BitMap &r) {
  modified = true;
  roaring &= r.roaring;
  return *this;
}

/**
* Compute the difference between the current bitmap and the provided
* bitmap, writing the result in the current bitmap. The provided bitmap is not modified.
*/
BitMap& BitMap::operator-=(const BitMap &r) {
  modified = true;
  roaring -= r.roaring;
  return *this;
}

/**
* Compute the union between the current bitmap and the provided bitmap,
* writing the result in the current bitmap. The provided bitmap is not
* modified.
*
* See also the fastunion function to aggregate many bitmaps more quickly.
*/
BitMap& BitMap::operator|=(const BitMap &r) {
  modified = true;
  roaring |= r.roaring;
  return *this;
}

/**
* Compute the symmetric union between the current bitmap and the provided
* bitmap, writing the result in the current bitmap.
* The provided bitmap is not modified.
*/
BitMap& BitMap::operator^=(const BitMap &r) {
  modified = true;
  roaring ^= r.roaring;
  return *this;
}




BitMap::const_iterator BitMap::begin() const {
  Roaring::const_iterator roar_begin = roaring.begin();
  return BitMap::const_iterator(roar_begin);
}

/**
* A bogus iterator that can be used together with begin()
* for constructions such as for(auto i = b.begin();
* i!=b.end(); ++i) {}
*/
BitMap::const_iterator& BitMap::end() const {
  static Roaring::const_iterator end_iter_roar = roaring.end();
  static BitMap::const_iterator end_iter(end_iter_roar);
  return end_iter;
}

// Iterators code. Wrapping the roaring iterators.
BitMap::const_iterator::const_iterator(Roaring::const_iterator& roar_iter)
  : roar_iter(new Roaring::const_iterator(roar_iter))
{

}

BitMap::const_iterator::const_iterator(const const_iterator &o) {
  roar_iter.reset(new Roaring::const_iterator(*o.roar_iter));
}

uint32_t BitMap::const_iterator::operator*() const {
  return *(*roar_iter);
}

bool BitMap::const_iterator::operator<(const const_iterator &o) const {
  return (*roar_iter) < (*o.roar_iter);
}

bool BitMap::const_iterator::operator<=(const const_iterator &o) const {
  return (*roar_iter) <= (*o.roar_iter);
}

bool BitMap::const_iterator::operator>(const const_iterator &o) const {
  return (*roar_iter) > (*o.roar_iter);
}

bool BitMap::const_iterator::operator>=(const const_iterator &o) const {
  return (*roar_iter) >= (*o.roar_iter);
}

BitMap::const_iterator& BitMap::const_iterator::operator++() {
  ++(*roar_iter);
  return *this;
}

BitMap::const_iterator BitMap::const_iterator::operator++(int) {
  BitMap::const_iterator cpy(*this);
  ++(*roar_iter);
  return cpy;
}

bool BitMap::const_iterator::operator==(const const_iterator &o) {
  return (*roar_iter) == (*o.roar_iter);
}

bool BitMap::const_iterator::operator!=(const const_iterator &o) {
  return (*roar_iter) != (*o.roar_iter);
}

std::vector<uint32_t> BitMap::values_as_vector() const {
  std::vector<uint32_t> res;
  for (auto iter = this->begin(); iter != this->end(); ++iter) {
    res.push_back(*iter);
  }
  return res;
}



uint32_t BitMap::write(char* buffer) const {
    // For now there's nothing else to save with the bitmap.
    return roaring.write(buffer);
}

std::shared_ptr<BitMap> BitMap::load(const char* buffer) {
   auto bmp = std::make_shared<BitMap>();
   bmp->roaring = Roaring::read(buffer);
   return bmp;
}

FUTURE_BMP BitMap::load_bitmap_async(
        int fd, uint32_t start_offset, uint32_t end_offset) {
  auto& disk_pool = DiskIOThreadPool::getInstance();

  uint32_t expected_size = end_offset - start_offset;
  // Read the rest of file. 
  auto fut = disk_pool.submitReadTask(fd, start_offset, expected_size);
  return fut.then([=](std::unique_ptr<FilerJobResult> result) {
      if (result->getIOResult() != 0 || 
            result->getIOSize() != expected_size) {
        throw std::runtime_error("Problem reading bitmap file.");
      }
      return BitMap::load(result->getIOBuffer());
  });
}

uint32_t BitMap::get_save_byte_size() const {
  return roaring.getSizeInBytes();
}





void BitMap::run_optimize() {
  roaring_bitmap_run_optimize(&roaring.roaring);
  modified = true;
}
