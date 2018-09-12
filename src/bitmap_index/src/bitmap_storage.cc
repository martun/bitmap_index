#include "bitmap_storage.h"
#include "IOExecutor.h"
#include "FilerJob.h"
#include "EventFD.h"
#include "toolkit.h"

#include <folly/futures/Future.h> 
#include <numeric>

using folly::Future;

namespace {

uint32_t round_to_disk_block_size(uint32_t byte_count) {
  const uint32_t block_size = 4096;
  uint32_t block_count = byte_count / block_size;
  if (byte_count % block_size != 0)
    ++block_count;
  return block_count * block_size;
}

}

/// To be called while creating a bitmap index. 
std::shared_ptr<BitMapStorage> BitMapStorage::create(
    std::shared_ptr<RowGroupInfo>& rg_info,
    std::shared_ptr<ColumnReference>& column_ref,
    int fd,
    const std::vector<uint32_t>& bitmap_counts,
    std::shared_ptr<LMDBDictionary<BitmapLMDBID, OffsetRange>>& bitmap_offsets_lmdb) {
  // Keep all the bitmaps in memory when creating the index.
  auto storage = std::make_shared<BitMapStorage>(
    rg_info, column_ref, fd, bitmap_counts, bitmap_offsets_lmdb, true, INT_MAX);
  return std::move(storage);
}

/// To be called while using the bitmap index.
Future<std::shared_ptr<BitMapStorage>> BitMapStorage::load(
    std::shared_ptr<RowGroupInfo>& rg_info,
    std::shared_ptr<ColumnReference>& column_ref,
    int fd,
    const OffsetRange& offsets,
    const std::vector<uint32_t>& bitmap_counts,
    std::shared_ptr<LMDBDictionary<BitmapLMDBID, OffsetRange>>& bitmap_offsets_lmdb,
    uint32_t bitmap_cache_size,
    bool load_all_bitmaps) {
  auto storage = std::make_shared<BitMapStorage>(
    rg_info, column_ref, fd, bitmap_counts, bitmap_offsets_lmdb, false, bitmap_cache_size);

  if (!load_all_bitmaps) {
    return storage;
  }
  
  uint32_t expected_size = offsets.end_offset - offsets.start_offset;

  auto& disk_pool = DiskIOThreadPool::getInstance();

  // Read the file portion, where the index data is saved.
  return disk_pool.submitReadTask(
      fd, 
      offsets.start_offset, 
      expected_size).then(
        [=](std::unique_ptr<FilerJobResult>& result) {
          if (result->getIOResult() != 0 || result->getIOSize() != expected_size) {
            throw "Problem reading bitmap storage: %s" + std::string(strerror(-result->getIOResult()));
          }
          // Load offsets of bitmaps from LMDB. 
          // TBD(martun): consider doing this in parallel. It's pretty fast so doing it sync for now.
          storage->load_offsets(); 

          // Storage needs to know it's own start and end offsets, because bitmap offsets 
          // are not relative to storage start position.
          storage->file_offsets = offsets;

          // Load all the bitmaps from the buffer.
          // TBD(martun) : Consider loading bitmaps from buffer in parallel. 
          // It's pretty fast so doing it sync for now.
          for (size_t i = 0; i < bitmap_counts.size(); ++i) {
            for (size_t j = 0; j < bitmap_counts[i]; ++j) {
              uint32_t buffer_offset = storage->bitmap_offsets[i][j].start_offset - offsets.start_offset;
              storage->bitmaps[i][j] = BitMap::load(result->getIOBuffer() + buffer_offset);
            }
          }
          storage->all_values_bitmap = BitMap::load(
            result->getIOBuffer() + storage->all_values_bitmap_offsets.start_offset - offsets.start_offset);
          return storage;
        });
}

void BitMapStorage::load_offsets() {
  // Load bitmap offsets from LMDB.
  // NOTE(martun): Here we're using the fact than keys for all the bitmaps for a single 
  // row_group and column predicate are continuous. Make sure to always have this feature.
  auto txn = bitmap_offsets_lmdb->new_txn();
  uint32_t bitmap_number = 0;
  auto lmdb_iter = bitmap_offsets_lmdb->find(BitmapLMDBID(
    rg_info->id, column_ref->dotted_path, bitmap_number), txn);
  
  if (lmdb_iter->first != BitmapLMDBID(rg_info->id, column_ref->dotted_path, bitmap_number)) {
    throw std::runtime_error(
        "Problem loading all values bitmap offsets from LMDB, iterator points to offsets of another row group..");
  }

  // Read all values bitmap offsets.
  all_values_bitmap_offsets = lmdb_iter->second;
  ++lmdb_iter;
  ++bitmap_number;

  bitmap_offsets.resize(bitmap_counts.size());
  for (size_t i = 0; i < bitmap_counts.size(); ++i) {
    bitmap_offsets[i].resize(bitmap_counts[i]);
    for (size_t j = 0; j < bitmap_counts[i]; ++j) {
      if (lmdb_iter->first != BitmapLMDBID(rg_info->id, column_ref->dotted_path, bitmap_number)) {
        throw std::runtime_error(
            "Problem loading bitmap offsets from LMDB, iterator points to offsets of another row group..");
      }
      bitmap_offsets[i][j] = lmdb_iter->second;
      ++lmdb_iter;
      ++bitmap_number;
    }
  }
}

BitMapStorage::BitMapStorage(
    std::shared_ptr<RowGroupInfo>& rg_info,
    std::shared_ptr<ColumnReference>& column_ref,
    int fd,
    const std::vector<uint32_t>& bitmap_counts, 
    std::shared_ptr<LMDBDictionary<BitmapLMDBID, OffsetRange>>& bitmap_offsets_lmdb,
    bool create,
    uint32_t bitmap_cache_size)
  : rg_info(rg_info)
  , column_ref(column_ref)
  , bitmap_offsets_lmdb(bitmap_offsets_lmdb)
  , bitmap_counts(bitmap_counts)
  , bitmap_cache_size(bitmap_cache_size)
  , fd(fd)
{
  // Create empty bitmaps. If create==true, will create empty bitmaps.
  bitmaps.resize(bitmap_counts.size());
  for (size_t i = 0; i < bitmap_counts.size(); ++i) {
    bitmaps[i].resize(bitmap_counts[i]);
    if (create) {
      for (size_t j = 0; j < bitmap_counts[i]; ++j) {
        bitmaps[i][j].reset(new BitMap());
      }
    }
  }

  if (create) {
    all_values_bitmap.reset(new BitMap());
    frequencies.resize(bitmap_counts.size());
    for (size_t i = 0; i < bitmap_counts.size(); ++i) {
      frequencies[i].resize(bitmap_counts[i]);
    }
  } else {
    // TODO(martun): change this to load frequencies of bitmap access from somewhere.
    frequencies.resize(bitmap_counts.size());
    for (size_t i = 0; i < bitmap_counts.size(); ++i) {
      frequencies[i].resize(bitmap_counts[i]);
    }
  }
  
  recompute_frequency_threshold();
}

BitMapStorage::~BitMapStorage() { 
  // TODO(martun): save frequencies of bitmap access.
}

void BitMapStorage::reset_usage_frequencies() {
  for (size_t i = 0; i < bitmap_counts.size(); ++i) {
    for (size_t j = 0; j < frequencies[i].size(); ++j) {
      frequencies[i][j] = 0;
    }
  }
}

void BitMapStorage::increase_frequency(int component, int i) {
  // If some bitmap crosses the previous threshold, we need to update it.
  if (frequencies[component][i] == frequency_threshold) {
    recompute_frequency_threshold();
  }
  ++frequencies[component][i];
}

FUTURE_CONST_BMP BitMapStorage::load_const_bitmap(
    int component, int i, bool always_store) {
  increase_frequency(component, i);

  if (bitmaps[component][i]) {
    FUTURE_CONST_BMP res = folly::makeFuture(
      std::const_pointer_cast<const BitMap>(bitmaps[component][i]));
    if (!always_store) {
      check_unload(component, i);
    }
    return res;
  }
  // If bitmap is not there, load it from file async.
  FUTURE_BMP res = std::move(BitMap::load_bitmap_async(
    fd, 
    bitmap_offsets[component][i].start_offset, 
    bitmap_offsets[component][i].end_offset));
  return res.then([&](std::shared_ptr<BitMap>& bmp) {
    // Store the bitmap, and return a future.
    bitmaps[component][i] = bmp;
    if (!always_store) {
      check_unload(component, i); 
    }
    return std::const_pointer_cast<const BitMap>(bmp);
  });
}

void BitMapStorage::check_unload(int component, int i) {
  // Unload the value, if the bitmap usage is below the threshold.
  if (frequencies[component][i] >= frequency_threshold) {
    bitmaps[component][i] = nullptr;
  }
}


FUTURE_BMP BitMapStorage::load_bitmap(int component, int i, bool always_store) {
  increase_frequency(component, i);

  if (bitmaps[component][i]) {
    FUTURE_BMP res = folly::makeFuture(std::make_shared<BitMap>(
      *bitmaps[component][i]));
    if (!always_store) {
      check_unload(component, i);
    }
    return res;
  }
  // If bitmap is not there, load it from file async.
  FUTURE_BMP res = std::move(BitMap::load_bitmap_async(
    fd, 
    bitmap_offsets[component][i].start_offset,
    bitmap_offsets[component][i].end_offset));
  return res.then([&](std::shared_ptr<BitMap>& bmp) {
    // Store the bitmap, and return a future.
    bitmaps[component][i] = bmp;
    if (!always_store) {
      check_unload(component, i);
    }
    return std::make_shared<BitMap>(*bmp);
  });
}

FUTURE_BMP BitMapStorage::load_all_values_bitmap() {
  if (all_values_bitmap) {
    return folly::makeFuture(all_values_bitmap);
  }
  // Always cache all_values_bitmap, it's used too often.
  FUTURE_BMP res = std::move(BitMap::load_bitmap_async(
    fd, 
    all_values_bitmap_offsets.start_offset,
    all_values_bitmap_offsets.end_offset));
  return res.then([&](std::shared_ptr<BitMap>& bmp) {
    // Store the bitmap, and return a future.
    all_values_bitmap = bmp;
    return std::shared_ptr<BitMap>(new BitMap(*bmp));
  });
}

FUTURE_CONST_BMP BitMapStorage::load_all_values_bitmap_const() {
  if (all_values_bitmap) {
    return folly::makeFuture(std::const_pointer_cast<const BitMap>(all_values_bitmap));
  }
  // Always cache all_values_bitmap, it's used too often.
  FUTURE_BMP res = std::move(BitMap::load_bitmap_async(
    fd, 
    all_values_bitmap_offsets.start_offset,
    all_values_bitmap_offsets.end_offset));
  return res.then([&](std::shared_ptr<BitMap>& bmp) {
    // Store the bitmap, and return a future.
    all_values_bitmap = bmp;
    return std::const_pointer_cast<const BitMap>(bmp);
  });
}

void BitMapStorage::add_to_all_values_bitmap(uint32_t value) {
  load_all_values_bitmap().wait();
  all_values_bitmap->add(value);
}

void BitMapStorage::add_to_all_values_bitmap(const std::vector<uint32_t>& values) {
  load_all_values_bitmap().wait();
  all_values_bitmap->add(values);
}

void BitMapStorage::add_to_bitmap(uint32_t component, uint32_t i, uint32_t value) {
  auto ptr = load_const_bitmap(component, i, true).get();

  bitmaps[component][i]->add(value);

  check_unload(component, i);
}

void BitMapStorage::add_to_bitmap(
    uint32_t component, uint32_t i, const std::vector<uint32_t>& values) {
  auto ptr = load_const_bitmap(component, i, true).get();

  bitmaps[component][i]->add(values);

  bitmaps[component][i]->run_optimize();
  check_unload(component, i);
}

void BitMapStorage::recompute_frequency_threshold() {
  std::vector<uint32_t> freqs;
  for (size_t i = 0; i < bitmap_counts.size(); ++i) {
    for (size_t j = 0; j < bitmap_counts[i]; ++j) {
      freqs.push_back(frequencies[i][j]);
    }
  }
  if (bitmap_cache_size >= freqs.size()) {
    // All the bitmaps are cached.
    frequency_threshold = UINT32_MAX;
    return;
  }
  std::nth_element(freqs.begin(), freqs.begin() + bitmap_cache_size, freqs.end());
  frequency_threshold = freqs[bitmap_cache_size];
  // TODO(martun): unload bitmaps which are below threshold, currently they are lazily unloaded on the first use.
}

void BitMapStorage::set_bitmap_counts(
    const std::vector<uint32_t>& bitmap_counts) {
  this->bitmap_counts = bitmap_counts;
}

uint32_t BitMapStorage::get_total_byte_size() {
  std::vector<FUTURE_CONST_BMP> futures;
  futures.push_back(std::move(load_all_values_bitmap_const()));
  for (uint32_t i = 0; i < bitmap_counts.size(); ++i) {
    for (uint32_t j = 0; j < bitmap_counts[i]; ++j) {
      // Hopefully all the bitmaps are cached, and there's
      // no actual loading happening here.
      futures.push_back(std::move(load_const_bitmap(i, j)));
    }
  }
  return collect(futures).then([=](std::vector<std::shared_ptr<const BitMap>>& bitmaps) {
    uint32_t total_size = 0;
    for (uint32_t i = 0; i < bitmaps.size(); ++i) {
      total_size += bitmaps[i]->get_save_byte_size();
    }
    return round_to_disk_block_size(total_size);
  }).get();
}

AsyncRetType BitMapStorage::save(uint32_t offset) {
  // Allocate buffer at alligned location in the memory.
  GaneshaDB::ManagedBuffer buffer = GaneshaDB::allocateBuffer(
      get_total_byte_size());
  std::vector<FUTURE_CONST_BMP> futures;
  futures.push_back(std::move(load_all_values_bitmap_const()));
  for (uint32_t i = 0; i < bitmap_counts.size(); ++i) {
    for (uint32_t j = 0; j < bitmap_counts[i]; ++j) {
      // Hopefully all the bitmaps are cached, and there's
      // no actual loading happening here.
      futures.push_back(std::move(load_const_bitmap(i, j)));
    }
  }
  return collect(futures).then([=](std::vector<std::shared_ptr<const BitMap>>& bitmaps) {
    std::vector<uint32_t> expected_sizes;
    for (uint32_t i = 0; i < bitmaps.size(); ++i) {
      expected_sizes.push_back(bitmaps[i]->get_save_byte_size());
    }
    uint32_t total_size = 0;
    total_size = round_to_disk_block_size(std::accumulate(
      expected_sizes.begin(), expected_sizes.end(), 0));

    // Allocate buffer at alligned location in the memory.
    GaneshaDB::ManagedBuffer buffer = GaneshaDB::allocateBuffer(total_size);
    uint32_t buffer_offset = 0;
    // For each bitmap, map it's bitmap number to a pair of offset and size in the file.
    std::vector<std::pair<BitmapLMDBID, OffsetRange>> lmdb_entries;
    BitmapLMDBID next_bmp_id;
    for (uint32_t i = 0; i < bitmaps.size(); ++i) {
      if (bitmaps[i]->write(buffer.get() + buffer_offset) != expected_sizes[i]) {
        throw std::runtime_error("Bitmap did not use the expected number "
          "of bytes to save to a buffer.");
      }
      lmdb_entries.push_back(std::make_pair(
        BitmapLMDBID(rg_info->id, column_ref->dotted_path, i),
        OffsetRange(offset + buffer_offset, offset + buffer_offset + expected_sizes[i])
        ));
      buffer_offset += expected_sizes[i];
    }
    auto txn = bitmap_offsets_lmdb->new_txn();
    bitmap_offsets_lmdb->insert(lmdb_entries, txn);

    // Now write the buffer to the file async.
    auto& disk_pool = DiskIOThreadPool::getInstance();
    return disk_pool.submitWriteTask(
      fd, offset, total_size, buffer.get()).get();
  });
}

// Getter for rg_info.
std::shared_ptr<RowGroupInfo>& BitMapStorage::get_rg_info() {
  return rg_info;
}

// Getter for column_ref. 
std::shared_ptr<ColumnReference>& BitMapStorage::get_column_ref() {
  return column_ref;
}

