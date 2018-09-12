#ifndef BITMAP_INDEX_BASE_H__
#define BITMAP_INDEX_BASE_H__

#include "common.h"
#include "index_utils.h"
#include "src/bitmap_storage.h"

// Base class for all bitmap indexes of all types.
// Useful for creating arrays of indexes.
class BitmapIndexBase {
public:	
  BitmapIndexBase(
    std::shared_ptr<RowGroupInfo>& rg_info,
    std::shared_ptr<ColumnReference>& column_ref,
    std::shared_ptr<BitMapStorage>&& storage);

	// range flags passed during query 
  enum IntervalFlags {
    OPEN = 0x00, // Both endpoints of the query are excluded.
    INCLUDE_LEFT = 0x01, // Left enpoint of the query is included.
    INCLUDE_RIGHT = 0x02, // Right enpoint of the query is included.
    CLOSED = 0x03 // Both endpoints of the query are included.
  };

  /**
   * return number of total inserts, query stats, disk size, etc
   * will have to maintain that data here.
   */
  std::string get_stats() const;

	// Returns a reference to the storage.
	BitMapStorage& storage() const;

protected:

  // Information on the row_group.
  std::shared_ptr<RowGroupInfo> rg_info_;

  // Column which is indexed by current bitmap index.
  std::shared_ptr<ColumnReference> column_ref_;

	// Storage of bitmaps, has functions for retrieving bitmaps from files or cache.
  std::shared_ptr<BitMapStorage> storage_;

};

#endif // BITMAP_INDEX_BASE_H__
