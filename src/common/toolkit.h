#pragma once


#include <string>
#include <exception>
#include <atomic>


// private namespace
namespace tkimpl {

  std::string sprintf(const char* format, ...) __attribute__((format(printf, 1, 2)));

  template<typename T>
  inline auto adapt(T&& arg) -> decltype(std::forward<T>(arg)) {
    return std::forward<T>(arg);
  }
  inline const char* adapt(const std::string& str) {
    return str.c_str();
  }
  inline const char* adapt(std::string& str) {
    return str.c_str();
  }
  inline const char* adapt(std::string&& str) {
    return str.c_str();
  }

  enum LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR
  };

  void log(LogLevel level, const std::string& msg);


}

template<typename... T>
    inline std::string FORMAT(const char *msg, T &&... args) {
        return tkimpl::sprintf(msg, tkimpl::adapt(std::forward<T>(args))...);
    }
inline std::string FORMAT(const char *msg) {
    return std::string(msg);
}
inline std::string FORMAT(const std::string &msg) {
    return msg;
}


#define GDBE_LIKELY(x)    __builtin_expect(!!(x), 1)
#define GDBE_UNLIKELY(x)  __builtin_expect(!!(x), 0)

