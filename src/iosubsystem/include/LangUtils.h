#ifndef LANGUTILS_H
#define LANGUTILS_H

#include <memory> // unique_ptr
#include <vector>

namespace GaneshaDB {

// double negation (!!) guarantees expr quickly turns into an "bool"
// http://stackoverflow.com/questions/248693/double-negation-in-c-code/249305#249305
#ifndef dce_unlikely
#define dce_unlikely(x) (__builtin_expect(!!(x), 0))
#endif
#ifndef dce_likely
#define dce_likely(x) (__builtin_expect(!!(x), 1))
#endif

#define DISALLOW_COPY(CLASS)                                            \
  CLASS(const CLASS &) = delete;                                               \
  CLASS &operator=(const CLASS &) = delete;

#define DISALLOW_MOVE(CLASS)                                            \
  CLASS(CLASS &&) = delete;                                                    \
  CLASS &operator=(CLASS &&) = delete;

#define Stringize(name) #name

// Assign a char pr to this type and it will self-destruct 
// https://stackoverflow.com/questions/27440953/stdunique-ptr-for-c-functions-that-need-free
struct free_deleter {
	template <typename T>
	void operator()(T *p) const {
		std::free(const_cast<std::remove_const_t<T>*>(p));
	}
};

template <typename T>
using unique_C_ptr = std::unique_ptr<T,free_deleter>;

static_assert(sizeof(char *)==
              sizeof(unique_C_ptr<char>),""); // ensure no overhead

typedef unique_C_ptr<char const> CharUPtr;

/*
 * vector.erase(arbitrary index) is not efficient;
 * better to swap desired elem with last element and then delete last elem
 * http://stackoverflow.com/questions/34994311/stdvectorerase-vs-swap-and-pop
 * TODO : move this func to some place more appropriate
 */


template <typename T>
void vector_erase(std::vector<T>& avector,
    typename std::vector<T>::iterator iter)
{
  if (avector.size() > 1) {
      // swap with last element
      std::iter_swap(iter, avector.end() - 1);
      // delete last element
      avector.pop_back();
  } else {
      avector.clear();
  }
}

}

#endif
