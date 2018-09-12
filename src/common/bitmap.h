#ifndef _BITMAP_H__
#define _BITMAP_H__

#include <vector>
#include <memory>
#include <mutex>
#include <stdint.h>


#include <FilerJob.h>
#include "EventFD.h"

#include <roaring/roaring.hh>
#include <folly/futures/Future.h>

using GaneshaDB::FilerJobResult;
using GaneshaDB::IOExecutor;

/// Class to store a single bitmap in compressed form. Basically a wrapper over the class 'BitMap'.
class BitMap
{
public:

  typedef folly::Future<std::shared_ptr<BitMap>> FUTURE_BMP;
  typedef folly::Future<std::shared_ptr<const BitMap>> FUTURE_CONST_BMP;

  // Used to return asynchronous calls to save a bitmap.
  typedef folly::Future<std::unique_ptr<FilerJobResult>> AsyncRetType;

  // A wrapper over roaring iterator.
  class const_iterator {
  public:
    const_iterator() = default;
    
    const_iterator(const const_iterator &o);

    uint32_t operator*() const;

    bool operator<(const const_iterator &o) const;

    bool operator<=(const const_iterator &o) const;

    bool operator>(const const_iterator &o) const;

    bool operator>=(const const_iterator &o) const;

    const_iterator& operator++();

    const_iterator operator++(int);

    bool operator==(const const_iterator &o);

    bool operator!=(const const_iterator &o);

    const_iterator &operator=(const const_iterator &o) = default;
    const_iterator &operator=(const_iterator &&o) = default;

    ~const_iterator() = default;
    
  private:
    // Let only Bitmap class create an instance of it's own iterator.
    friend class BitMap;
    const_iterator(Roaring::const_iterator& roar_iter);

    // The roaring iterator which we wrap here.
    std::unique_ptr<Roaring::const_iterator> roar_iter;
  };

  /// @brief Factory method to loads a bitmap from the file asyncronously.
  static FUTURE_BMP load_bitmap_async(int fd, uint32_t start_offset, uint32_t end_offset);

  /// @brief A Factory method to load bitmap from a byte array. Opposite of function 'write'.
  static std::shared_ptr<BitMap> load(const char* buffer);

  /// @brief Constructor for an empty bitmap, not tied to a file.
  ///   This can be returned as a result of a query.
  BitMap();

  BitMap(const BitMap& other) = default;

  // Adds a given value to the bitmap.
  void add(uint32_t value);

  // Batch adds values to the current bitmap.
  void add(const std::vector<uint32_t>& values);
  void add(const uint32_t* values, uint32_t size);

  /*
   * Batch adds range of values [begin, end).
   */
  void add_range(uint32_t begin, uint32_t end);

  // Returns if given value is in the bitmap.
  bool contains(uint32_t x) const;

  /**
  * Moves the content of the provided bitmap, and
  * discards the current content.
  */
  BitMap &operator=(BitMap &&r);

  /**
  * Compute the intersection between the current bitmap and the provided
  * bitmap,
  * writing the result in the current bitmap. The provided bitmap is not
  * modified.
  */
  BitMap &operator&=(const BitMap &r);

  /**
  * Compute the difference between the current bitmap and the provided
  * bitmap, writing the result in the current bitmap. The provided bitmap is not modified.
  */
  BitMap &operator-=(const BitMap &r);

  /**
  * Compute the union between the current bitmap and the provided bitmap,
  * writing the result in the current bitmap. The provided bitmap is not
  * modified.
  *
  * See also the fastunion function to aggregate many bitmaps more quickly.
  */
  BitMap &operator|=(const BitMap &r);

  /**
  * Compute the symmetric union between the current bitmap and the provided
  * bitmap, writing the result in the current bitmap. 
  * The provided bitmap is not modified.
  */
  BitMap &operator^=(const BitMap &r);

  /**
  * Get the cardinality of the bitmap (number of elements).
  */
  uint64_t cardinality() const;

  /**
  * Returns true if the bitmap is empty (cardinality is zero).
  */
  bool isEmpty() const;

  uint32_t maximum() const;
  uint32_t minimum() const;

  /// @brief Flips given range in the current bitmap.
  void flip(int start, int end);

  /**
  * Returns an iterator that can be used to access the position of the
  * set bits. The running time complexity of a full scan is proportional to
  * the
  * number
  * of set bits: be aware that if you have long strings of 1s, this can be
  * very inefficient.
  *
  * It can be much faster to use the toArray method if you want to
  * retrieve the set bits.
  */
  const_iterator begin() const;

  /**
  * A bogus iterator that can be used together with begin()
  * for constructions such as for(auto i = b.begin();
  * i!=b.end(); ++i) {}
  */
  const_iterator& end() const;

  /// @brief Returns the values in current bitmap. Mainly used for debugging.
  std::vector<uint32_t> values_as_vector() const;

  /// @brief Prints the values in the bitmap, for debugging. 
  void print() const;

  /// @brief Returns number of bytes required to save the bitmap file.
  uint32_t get_save_byte_size() const;

  /** @brief Writer bitmap to the given output buffer. 
   *    The buffer must be pre-allocated with 'get_save_byte_size()' bytes.
   *  @param[out] buffer A buffer to write into.
   *  @returns Number of bytes written.
   */
  uint32_t write(char* buffer) const;

  /*
   * ${dst} must have at least ${cardinality()} capacity
   */
  void to_array(uint32_t* dst) const;

  void clear();

  void run_optimize();

private:
  // The actual bitmap.
  Roaring roaring;

  // Shows if the bitmap was modified after getting loaded from file.
  bool modified;

};

#endif  // _BITMAP_H__

