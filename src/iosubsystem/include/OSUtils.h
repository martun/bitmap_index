#ifndef OSUTILS_H
#define OSUTILS_H

#include <cassert>
#include <cstdint>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h> // getcpu
#include <string>

#ifndef gettid
#include <sys/types.h>
#include <syscall.h>
#include <unistd.h>
#define gettid() (syscall(SYS_gettid))
#endif

typedef int16_t CoreId; // TODO make a full class later ?
#define CoreIdInvalid (-1)

namespace GaneshaDB {
namespace os {

static constexpr int32_t DirectIOSize = 512; // for posix_memalign

// https://blogs.oracle.com/jwadams/entry/macros_and_powers_of_two
inline bool IsDirectIOAligned(uint64_t number) {
  // check for 0 is special
  return (!number) || (!(number & (DirectIOSize - 1)));
}

inline size_t RoundToNext512(size_t numToRound) {
  constexpr uint32_t multiple = DirectIOSize;
  // another alternative (-((-number) & -(512)));
  return (numToRound + multiple - 1) & ~(multiple - 1);
}

static constexpr int32_t FD_INVALID = -1;

inline bool IsFdOpen(int fd) { return (fcntl(fd, F_GETFL) != -1); }

inline void BindThreadToCore(CoreId cpu_id) {
    cpu_set_t cs; 
    CPU_ZERO(&cs);
    CPU_SET(cpu_id, &cs);
    auto r = pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs);
    assert(r == 0); 
	(void)r; // silence a warning.
}

int32_t RaiseThreadPriority();

inline CoreId GetCpuCore() {
  thread_local CoreId thisCore_ = sched_getcpu();
  return (thisCore_ >= 0) ? thisCore_ : 0;
}

}
} // namespace

#endif
