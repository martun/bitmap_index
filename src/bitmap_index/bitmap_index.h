#ifndef BITMAP_INDEX_H__
#define BITMAP_INDEX_H__

#include <vector>
#include <map>
#include <exception>
#include <memory>
#include <algorithm>
#include <stdint.h>
#include <math.h>
#include <limits.h>

#include <type_traits>

#include "common.h"
#include "bitmap.h"
#include "bitmap_index_base.h"
#include "src/bitmap_storage.h"
#include "src/value_decomposer.h"
#include "src/lmdb_dictionary.h"
#include "src/lmdb_values.h"

#include <folly/futures/Future.h>

using namespace folly::futures;
using folly::makeFuture;
using namespace LMDB;

typedef BitMap::FUTURE_BMP FUTURE_BMP;
typedef BitMap::FUTURE_CONST_BMP FUTURE_CONST_BMP;

/**
*
* Assume not thread-safe for now.
*
*/
template <class T>
class BitmapIndex : public BitmapIndexBase
{
public:
	typedef typename LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>::const_iterator LMDBIterator;
	typedef typename std::vector<std::shared_ptr<const BitMap>> SharedConstBitmapVector;
	typedef typename std::vector<std::shared_ptr<BitMap>> SharedBitmapVector;

	/**
	 * @brief Loads given row group bitmap index for using.
	 * @param[in] storage Bitmap storage where the bitmap files are stored.
	 * @param[in] dict LMDB dictionary to store attribute value mapping.
	 * @param[in] aux_data All the auxiliary data which bitmap index needs to persist.
	 */
	static std::shared_ptr<BitmapIndex<T>> load(
      std::shared_ptr<RowGroupInfo> rg_info,
      std::shared_ptr<ColumnReference> column_ref,
		  std::shared_ptr<BitMapStorage>&& storage,
		  std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
      std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>> aux_data_lmdb);

  /**
	 * @brief Loads given row group bitmap index for using.
	 * @param[in] storage Bitmap storage where the bitmap files are stored.
	 * @param[in] dict LMDB dictionary to store attribute value mapping.
	 * @param[in] aux_data All the auxiliary data which bitmap index needs to persist.
	 */
	static Future<std::shared_ptr<BitmapIndex<T>>> load(
      std::shared_ptr<RowGroupInfo> rg_info,
      std::shared_ptr<ColumnReference> column_ref,
		  Future<std::shared_ptr<BitMapStorage>>&& storage,
		  std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
      std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>> aux_data_lmdb);

	/**
	 * @brief: Constructs index from a list of values.
	 * @param[in] storage Bitmap storage where the bitmap files are stored.
	 * @param values Vector of pairs <value, row_id>, which must contain all the possible values 
	 * 	  (total number of distinct values must be equal to 'cardinality').
	 */
	static std::shared_ptr<BitmapIndex<T>> create(
      std::shared_ptr<RowGroupInfo>& rg_info,
      std::shared_ptr<ColumnReference>& column_ref,
		  std::shared_ptr<BitMapStorage>&& storage,
		  std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>>& attr_values_lmdb,
      std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>>& aux_data_lmdb,
      std::shared_ptr<BitmapIndexAuxData>& aux_data,
    	const std::vector<std::pair<DocumentID, T>>& values); 
	
  /** @brief Constructs the index. Later the index must be either loaded or created from values.
	 * 				To be called ONLY by load/create functions. 
   * @param[in] storage Bitmap storage where the bitmap files are stored.
	 * @param[in] attr_values_lmdb LMDB dictionary to store attribute value mapping.
	 * @param[in] aux_data All the auxiliary data which bitmap index needs to persist.
	 */	
	BitmapIndex(
      std::shared_ptr<RowGroupInfo> rg_info,
      std::shared_ptr<ColumnReference> column_ref,
		  std::shared_ptr<BitMapStorage>&& storage,
		  std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
      std::shared_ptr<BitmapIndexAuxData> aux_data);

  // Loads auxiliary data from lmdb.
  static std::shared_ptr<BitmapIndexAuxData> get_aux_data(
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>> aux_data_lmdb);

	/// Destructor, saves related data if changed.
	~BitmapIndex();

	/**
	 * @brief Inserts given value into the bitmap index. Throws exception in case the current value increases cardinality.
	 */
	void insert(const DocumentID& row_id, const T& value);

	/**
	 * @brief Batch inserts values into the bitmap index. Throws exception in case these new values increase cardinality.
	 * TODO(martun): make inserts assync.
	 */
	void insert(const std::vector<std::pair<DocumentID, T>>& value);

	/**
	 * @brief Looks up the row_ids the values of which are in the range.
	 * @returns A bitmap containing row_ids for which the corresponding value is in range [min, max].
	 */
	FUTURE_BMP range_search(const T& min, const T& max, 
		const IntervalFlags interval_flags);
	
  /**
	 * @brief Looks up the row_ids the values of which are less than given value.
	 * @returns A bitmap containing row_ids for which the corresponding value is in range [min, max].
	 */
	FUTURE_BMP lesser(const T& max,	const IntervalFlags interval_flags);
  
  /**
	 * @brief Looks up the row_ids the values of which are less than given value.
	 * @returns A bitmap containing row_ids for which the corresponding value is in range [min, max].
	 */
  FUTURE_BMP greater(const T& min,	const IntervalFlags interval_flags);

	/**
	 * @brief : returns list of row_ids that have given value.
	 */
	FUTURE_BMP lookup(const T& value) const;
	
  /**
	 * @brief : returns list of row_ids that don't have given value.
	 */
	FUTURE_BMP not_equals(const T& value) const;

  /// @brief Returns the value in the all_values_bitmap, which contains all non-null row ids.
  FUTURE_BMP get_not_null() const;

	/// @brief Resets bitmap usage statistics. Usefull to call after the insertions are done.
	void reset_usage_frequencies();

private:

  // Inserts already mapped value into the index.
  // Note: Even though sometime, when no value map is stored in the LMDB,
  // the mapped value can be int64, we store only int32 in lmdb for space reasons.
  void insert_mapped_value(const DocumentID& row_id, uint64_t mapped_value);

  // Inserts a list of mapped values into the index.
  // Note: Even though sometime, when no value map is stored in the LMDB,
  // the mapped value can be int64, we store only int32 in lmdb for space reasons.
  void insert_mapped_values(std::vector<std::pair<DocumentID, uint64_t>> mapped_values);

	/**
	 * @brief : return list of row_ids that have given value.
	 */
	FUTURE_BMP lookup_mapped_value(int64_t value) const;

	FUTURE_BMP get_equality_bitmap(uint32_t i, uint32_t attribute_i) const;

	FUTURE_BMP get_equality_bitmap_range_encoding(
		uint32_t i, uint32_t attribute_i) const;

	FUTURE_BMP get_equality_bitmap_interval_encoding(
		uint32_t i, uint32_t attribute_i) const;

	// @brief Returns bitmap for values strictly less than provided.
	FUTURE_BMP get_lesser_bitmap(int i, uint32_t attribute_i) const;

	/// @brief Performs a range search over the bitmaps for values with integer representation in range [v1,v2], v1 < v2.
	FUTURE_BMP range_search_internal(int64_t v1, int64_t v2);

	/// @brief Performs query <=upper_bound.
	FUTURE_BMP less_or_equal_query(int64_t upper_bound);

	/// @brief Returns the total number of bitmaps used.
  uint32_t get_total_bitmaps_number() const;

  /** @brief Inserts given values into LMDB attribute value mapping database. Returns the mapping back.
   *  @param[in] values Values in the index.
   *  @param[out] values_mapping_out Output mapping.
   */
  void insert_values_mapping_into_lmdb(
      const std::vector<std::pair<DocumentID, T>>& values, 
      std::unordered_map<T, uint32_t>* values_mapping_out);

private:

	// Used to convert the current string values into integers,
	// or to convert integer values into values from range [0, cardinality).
	std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb_;
	
  // All the auxiliary data stored along with this index.
  // While creating the index, changes to this variables will be saved to LMDB.
  std::shared_ptr<BitmapIndexAuxData> aux_data_;

  // If set, then insertion has happened, I.E. related data must be
	// stored on destruction.
	bool modified_;

};

#include "src/bitmap_index.hpp" // Implementation of the class BitMapIndex.

#endif // BITMAP_INDEX_H__
