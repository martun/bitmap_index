#ifndef STATS_H
#define STATS_H

#include <cassert>  // assert
#include <cmath>    // log10 sqrt
#include <cstdint>  // uint64
#include <iostream> // ostream overload
#include <limits>   // numeric_limits
#include <memory>
#include <string.h>    // memcpy
#include <type_traits> // is_arithmetic

#include "LangUtils.h"

namespace GaneshaDB {
namespace stats {

template <typename T> class StatsCounter;

template <typename T>
std::ostream &operator<<(std::ostream &os, const StatsCounter<T> &t);

/**
 * Usage
 * StatsCounter<int64_t> a;
 * for (int i = 0; i < 10; i++)
 * {
 *   a = i;
 * }
 * std::cout << a; // will print min, max, avg, stddev
 */
template <class T> class StatsCounter {
  static_assert(std::is_arithmetic<T>::value, "arithmetic type required");

public:
  T min_{std::numeric_limits<T>::max()};
  T max_{std::numeric_limits<T>::min()};

  // E[X] in statistical terms
  float mean_{0};

  // sum of square of differences from current mean
  float meanSquared_{0};

  uint32_t numSamples_{0};

  void updateStats(const T &data) {
    if (min_ > data)
      min_ = data;
    if (max_ < data)
      max_ = data;

    numSamples_++;
    const float delta = (data - mean_);
    mean_ += delta / numSamples_;
    // Welford's algo for variance
    // MeanSquare = MeanSquare + (x - Avg(n)) (x - Avg(n-1))
    meanSquared_ += delta * (data - mean_);

    assert(numSamples_ > 0); // overflow error
  }

  explicit StatsCounter() = default;

  StatsCounter(const StatsCounter<T> &other) {
    memcpy(this, &other, sizeof(other));
  }

  StatsCounter<T> &operator=(const StatsCounter<T> &other) {
    memcpy(this, &other, sizeof(other));
    return *this;
  }

  void reset() {
    static StatsCounter<T> a;
    *this = std::move(a);
  }

  // called when you add waitTime + serviceTime
  StatsCounter<T> &add(const StatsCounter<T> &other) {
    assert(numSamples_ == other.numSamples_);
    min_ += other.min_;
    max_ += other.max_;
    mean_ += other.mean_;
    meanSquared_ += other.meanSquared_;
    return *this;
  }

  // called when you add averages for multiple threads
  StatsCounter<T> &operator+=(const StatsCounter<T> &other) {
    min_ = (min_ < other.min_) ? min_ : other.min_;
    max_ = (max_ > other.max_) ? max_ : other.max_;
    // compute weighted average
    mean_ = ((mean_ * numSamples_) + (other.mean_ * other.numSamples_)) /
            (numSamples_ + other.numSamples_);
    // compute weighted mean-squared
    meanSquared_ = ((meanSquared_ * numSamples_) +
                    (other.meanSquared_ * other.numSamples_)) /
                   (numSamples_ + other.numSamples_);
    // update numSamples after computing avg
    numSamples_ += other.numSamples_;
    return *this;
  }

  StatsCounter<T> operator+(const StatsCounter<T> &other) {
    StatsCounter<T> total(*this);
    total += other;
    return total;
  }

  void operator=(const T &other) { updateStats(other); }

  float mean() const { return mean_; }

  float variance() const {
    // Dividing by N-1 leads to an unbiased estimate of variance from the
    // sample,
    // whereas dividing by N on average underestimates variance (because it
    // doesn't
    // take into account the variance between the sample mean and the true mean)
    // http://stackoverflow.com/questions/1174984/how-to-efficiently-calculate-a-running-standard-deviation
    return meanSquared_ / (numSamples_ - 1);
  }

  float stdDeviation() const { return std::sqrt(variance()); }

  // coefficient of variation is the normalized value
  float coefficientOfVariation() const { return stdDeviation() / mean_; }

  friend std::ostream &operator<<<T>(std::ostream &os,
                                     const StatsCounter<T> &t);
};

template <class T>
std::ostream &operator<<(std::ostream &os, const StatsCounter<T> &t) {
  if (dce_unlikely(t.numSamples_ == 0)) {
    os << "{\"min\":0,\"avg\":0,\"stddev\":0,\"max\":0,\"numSamples\":0}";
  } else {
    // json format
    os << "{\"min\":" << t.min_ << ",\"avg\":" << t.mean_
       << ",\"stddev\":" << t.stdDeviation() << ",\"max\":" << t.max_
       << ",\"numSamples\":" << t.numSamples_ << "}";
  }
  return os;
}

// ================

/**
 * Log scale histogram
 * bucket 0 contains 0-10
 * bucket 1 contains 10-100
 * and so on..
 *
 * Usage
 * Histogram<int64_t> a;
 * for (int i = 0; i < 10; i++)
 * {
 *   a = i;
 * }
 * std::cout << a; // will print histogram of all values
 */
template <class T> class Histogram {
  static_assert(std::is_integral<T>::value, "integer type required");
  // to support floating point, figure out how to change MaxDigitsInType

public:
  // allocate as many buckets as number of digits in type
  static constexpr size_t MaxDigitsInType =
      (std::numeric_limits<T>::digits10 + std::numeric_limits<T>::is_signed +
       2);

  uint32_t buckets_[MaxDigitsInType];
  uint32_t numSamples_{0};

  void updateStats(const T &data) {
    uint16_t buck = static_cast<uint16_t>(std::floor(std::log10(data)));
    assert(buck < MaxDigitsInType);
    assert(buck < MaxDigitsInType);
    buckets_[buck]++;
    numSamples_++;
  }

  explicit Histogram() { reset(); }

  Histogram(const Histogram<T> &other) { memcpy(this, &other, sizeof(other)); }

  Histogram<T> &operator=(const Histogram<T> &other) {
    memcpy(this, &other, sizeof(other));
    return *this;
  }

  Histogram<T> &operator+=(const Histogram<T> &other) {
    for (size_t i = 0; i < MaxDigitsInType; i++) {
      buckets_[i] += other.buckets_[i];
    }
    numSamples_ += other.numSamples_;
    return *this;
  }

  Histogram<T> operator+(const Histogram<T> &other) {
    Histogram<T> total(*this);
    total += other;
    return total;
  }

  void operator=(const T &other) { updateStats(other); }

  void reset() {
    bzero(buckets_, sizeof(buckets_[0]) * MaxDigitsInType);
    numSamples_ = 0;
  }

  template <typename U>
  friend std::ostream &operator<<(std::ostream &os, const Histogram<U> &t) {
    // json format
    os << "{\"numSamples\":" << t.numSamples_ << ",\"histogram\":[";
    for (uint16_t i = 0; i < MaxDigitsInType - 1; i++) {
      os << t.buckets_[i] << ",";
    }
    os << t.buckets_[MaxDigitsInType - 1] << "]}";
    return os;
  }
};

// ================

template <typename T> class MinValue;

template <typename T>
std::ostream &operator<<(std::ostream &os, const MinValue<T> &t);

/**
 * Usage
 * MinValue<int64_t> a;
 * for (int i = 0; i < 10; i++)
 * {
 *   a = i;
 * }
 * std::cout << a; // will print min of all assignments
 */
template <class T> class MinValue {
public:
  static_assert(std::is_arithmetic<T>::value, "arithmetic type required");

  T min_{std::numeric_limits<T>::max()};

  explicit MinValue() = default;

  explicit MinValue(const T& val) {
    min_ = val;
  }

  void operator=(T &&val) {
    if (val < min_)
      min_ = val;
  }
  void operator=(const T &val) {
    if (val < min_)
      min_ = val;
  }

  T get() const { return min_; }

  friend std::ostream &operator<<<T>(std::ostream &os, const MinValue<T> &t);
};

template <class T>
std::ostream &operator<<(std::ostream &os, const MinValue<T> &t) {
  os << t.min_;
  return os;
}

// ================

template <typename T> class MaxValue;

template <typename T>
std::ostream &operator<<(std::ostream &os, const MaxValue<T> &t);

/**
 * Usage
 * MaxValue<int64_t> a;
 * for (int i = 0; i < 10; i++)
 * {
 *   a = i;
 * }
 * std::cout << a; // will print max of all assignments
 */
template <class T> class MaxValue {
public:
  static_assert(std::is_arithmetic<T>::value, "arithmetic type required");

  T max_{std::numeric_limits<T>::min()};

  explicit MaxValue() = default;

  explicit MaxValue(const T& val) {
    max_ = val;
  }

  void operator=(T &&val) {
    if (val > max_)
      max_ = val;
  }
  void operator=(const T &val) {
    if (val > max_)
      max_ = val;
  }

  T get() const { return max_; }

  friend std::ostream &operator<<<T>(std::ostream &os, const MaxValue<T> &t);
};

template <class T>
std::ostream &operator<<(std::ostream &os, const MaxValue<T> &t) {
  os << t.max_;
  return os;
}

// ================
}
}

#endif
