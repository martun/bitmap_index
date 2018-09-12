#ifndef TIMER_H
#define TIMER_H

#include <chrono>

namespace GaneshaDB {
namespace stats {

/**
 * Usage { Timer t1; {...}; t1.elapsedSeconds(); }
 */
class Timer {
private:
  typedef std::chrono::high_resolution_clock Clock;
  Clock::time_point begin_;

public:
  explicit Timer(bool startNow = false) {
    if (startNow)
      start();
  }

  void start() { begin_ = Clock::now(); }
  void clear() { start(); }

  int64_t differenceMicroseconds(const Timer& older) const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               begin_ - older.begin_).count();
  }

  int64_t differenceMilliseconds(const Timer& older) const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               begin_ - older.begin_).count();
  }

  int64_t differenceNanoseconds(const Timer& older) const {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               begin_ - older.begin_).count();
  }

  int64_t elapsedSeconds() const {
    return std::chrono::duration_cast<std::chrono::seconds>(Clock::now() -
                                                            begin_).count();
  }

  int64_t elapsedMilliseconds() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               Clock::now() - begin_).count();
  }

  int64_t elapsedMicroseconds() const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               Clock::now() - begin_).count();
  }

  int64_t elapsedNanoseconds() const {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                                begin_).count();
  }
};

}
} // namespace

#endif
