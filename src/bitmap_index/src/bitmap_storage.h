#ifndef BITMAP_STORAGE_H__
#define BITMAP_STORAGE_H__

#include "bitmap.h"
#include "lmdb_values.h"
#include "common.h"

#include <folly/futures/Future.h>
#include <DiskIOThreadPool.h>
#include "bitmap.h"
#include "lmdb_dictionary.h"
#include "lmdb_values.h"

#include <string>
#include <vector>
#include <memory>
#include <climits>

using folly::Future;
using namespace LMDB;

typedef folly::Future<std::unique_ptr<FilerJobResult>> AsyncRetType;
typedef BitMap::FUTURE_BMP FUTURE_BMP;
typedef BitMap::FUTURE_CONST_BMP FUTURE_CONST_BMP;

// Class stores and retrieves all the bitmaps required for the bitmap index of 
// row group level for a single row group for a single column predicate. 
class BitMapStorage {
public:

  /** @brief To be called while creating a bitmap storage. Creates a storage with empty bitmaps.
   *
   *  @param[in] fd File descriptor of the file to which the created bitmaps must get saved.
   *  @param[in] bitmap_counts Number of bitmaps per attribute.
   *  @param[in] bitmap_offsets_lmdb LMDB database for bitmap offsets.
   */
  static std::shared_ptr<BitMapStorage> create(
      std::shared_ptr<RowGroupInfo>& rg_info,
      std::shared_ptr<ColumnReference>& column_ref,
      int fd,
      const std::vector<uint32_t>& bitmap_counts, 
      std::shared_ptr<LMDBDictionary<BitmapLMDBID, OffsetRange>>& bitmap_offsets_lmdb);

  /** @brief To be called while using the bitmap index.  
   *  @param[in] fd File descriptor in which the bitmaps are saved. 
   *  @param[in] offsets Start and end offsets of current bitmap storage in the file fd.
   *  @param[in] load_all_bitmaps If set to true, ignores bitmap_cache_size and loads i
   *    all the bitmaps at once.
   */
  static Future<std::shared_ptr<BitMapStorage>> load(
    std::shared_ptr<RowGroupInfo>& rg_info,
    std::shared_ptr<ColumnReference>& column_ref,
    int fd,
    const OffsetRange& offsets,
    const std::vector<uint32_t>& bitmap_counts, 
    std::shared_ptr<LMDBDictionary<BitmapLMDBID, OffsetRange>>& bitmap_offsets_lmdb,
    uint32_t bitmap_cache_size = 1,
    bool load_all_bitmaps = false);

  /** @brief Constructor, creates a bunch of bitmaps needed for the interval index, or loads them from files on demand.
   *  @param[in] bitmap_counts Number of bitmaps for each attribute value.
   *  @param[in] bitmap_cache_size Number of bitmaps we want to keep in memory for fast access.
   *  @param[in] create If set to true, creates empty bitmaps, otherwise loads them from files.
   */
  BitMapStorage(
    std::shared_ptr<RowGroupInfo>& rg_info,
    std::shared_ptr<ColumnReference>& column_ref,
    int fd,
    const std::vector<uint32_t>& bitmap_counts, 
    std::shared_ptr<LMDBDictionary<BitmapLMDBID, OffsetRange>>& bitmap_offsets_lmdb,
    bool create = false,
    uint32_t bitmap_cache_size = 1);

  BitMapStorage(const BitMapStorage&) = delete;  
  BitMapStorage& operator=(const BitMapStorage&) = delete;  
  
  ~BitMapStorage();

  /// @brief Returns i-th bitmap of 'component-th' component. Returns it as const, because we don't want the external caller to modify our bitmap store.
  FUTURE_CONST_BMP load_const_bitmap(int component, int i, bool always_store = false);

  /** @brief Returns a deep copy of the required bitmap. If you're not going to modify the bitmap, use "load_const_bitmap" instead.
   *  @param[in] component Shows the number of the component when doing atribute value decomposition.
   *  @param[in] component Shows the number of bitmap in the component.
   *  @param[in] always_store If set to true, will not unload the bitmap, even if it's under the usage threshold.
   */
  FUTURE_BMP load_bitmap(int component, int i, bool always_store = false);

  /// @brief Returns a deep copy of the all values bitmap. If you're not going to modify the bitmap, 
  //  use "load_all_values_bitmap_const" instead.
  FUTURE_BMP load_all_values_bitmap();
  
  /// @brief Loads and returns all_values_bitmap.
  FUTURE_CONST_BMP load_all_values_bitmap_const();

  /// @brief Adds given value into the all_values_bitmap.
  void add_to_all_values_bitmap(uint32_t value);
  void add_to_all_values_bitmap(const std::vector<uint32_t>& values);

  /// @brief Adds a value to the given bitmap.
  void add_to_bitmap(uint32_t component, uint32_t i, uint32_t value);
  void add_to_bitmap(uint32_t component, uint32_t i, const std::vector<uint32_t>& values);

  /// @brief Recomputes the value of 'frequency_threshold' based on the frequencies used.
  void recompute_frequency_threshold();

  /// @brief Increases usage frequency of given bitmap.
  void increase_frequency(int component, int i);

  /// @brief Resets bitmap usage statistics. Usefull to call after the insertions are done.
  void reset_usage_frequencies();

  void set_bitmap_counts(const std::vector<uint32_t>& bitmap_counts);

  // Returns the total number of bytes required to save all the bitmaps.
  // Rounds up the value to 4096, to make sure write was efficient. 
  // The function is NOT const, because the contents of the cache may get changed.
  uint32_t get_total_byte_size();

  /** Saves all the bitmaps in file 'fd' starting from offset 'offset'.
   *  Save is NOT CONST, because it starts writing bitmap offsets in LMDB.
   */ 
  AsyncRetType save(uint32_t offset);
  
  /** Loads all the bitmaps from the file 'fd' starting from offset 'offset'.
   */ 
  AsyncRetType load(uint32_t offset);

  /** @brief Loads file offsets for all the bitmaps from LMDB.
   *    Fills up values of bitmap_offsets and all_values_bitmap_offsets.
   */
  void load_offsets();

  // Getter for rg_info.
  std::shared_ptr<RowGroupInfo>& get_rg_info();

  // Getter for column_ref. 
  std::shared_ptr<ColumnReference>& get_column_ref();

private:

  // Unloads the bitmap if it's under the usage threshold.
  void check_unload(int component, int i);

  // Cache for frequently used bitmaps. 
  // Some bitmaps may not be present, and must be loaded from a file.
  std::vector<std::vector<std::shared_ptr<BitMap>>> bitmaps;

private:
  // Information on the row_group.
  std::shared_ptr<RowGroupInfo> rg_info;

  // Column which is indexed by current bitmap index.
  std::shared_ptr<ColumnReference> column_ref;

  // frequencies[i][j] is the number of accesses to bitmaps[i][j].
  std::vector<std::vector<uint32_t>> frequencies;
  
  // Shows the threshold after which a bitmap is cached. Updated while the index is used.
  uint32_t frequency_threshold;

  // A bitmap containing all the document ids which are not null. 
  // Papers assume that all the row_ids form a range, which 
  // results to returning unexistance row_ids in some queries.
  // Now some query results will be "&"ed with this bitmap to ensure that does not happen. 
  std::shared_ptr<BitMap> all_values_bitmap;
  
  // For each bitmap id will return a pair of offsets in the file.
  std::shared_ptr<LMDBDictionary<BitmapLMDBID, OffsetRange>> bitmap_offsets_lmdb;
  
  // File offsets for all the bitmaps.
  std::vector<std::vector<OffsetRange>> bitmap_offsets;

  // File offset of all_values_bitmap.
  OffsetRange all_values_bitmap_offsets;

  // Number of bitmaps for each attribute value.
  std::vector<uint32_t> bitmap_counts; 
    
  // Number of bitmaps we want to keep in memory for fast access.
  uint32_t bitmap_cache_size;

  // File descriptor to read/write from.
  int fd;
  
  // Start and end offsets of current bitmap storage in the file fd.
  OffsetRange file_offsets;

};

#endif // BITMAP_STORAGE_H__
