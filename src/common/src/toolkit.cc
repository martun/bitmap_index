#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif
#include <unistd.h>
#include <syscall.h>
#include <time.h>

#include <cstdarg>
#include <chrono>
#include <array>
#include <iostream>
#include <vector>
#include <mutex>
#include <algorithm>

#include "toolkit.h"

namespace tkimpl {
    std::string sprintf(const char* format, ...) {
      int capacity = 32;
      while (true) {
        char buffer[capacity];
        va_list args;
        va_start(args, format);
        int length = vsnprintf(buffer, capacity, format, args);
        va_end(args);
        if (length < capacity) {
          return std::string(buffer, 0, length);
        } else {
          capacity = length + 1;
        }
      }
    }




inline uint64_t now() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  uint64_t res = 0;
  res += ts.tv_sec * 1000ull*1000*1000;
  res += ts.tv_nsec;
  return res;
}

}


