#ifndef _INDEX_INTERFACES_H__
#define _INDEX_INTERFACES_H__

#include <memory>
#include "common.h"
#include "bitmap.h"

#include <folly/futures/Future.h>

using namespace folly::futures;

namespace expreval {

typedef std::map<BatchID, std::shared_ptr<BitMap>> BatchResultMap;

enum IndexAccuracy {
  IA_NONE,   // no index
  IA_COARSE, // index with false positives
  IA_EXACT   // index without false positives
};

class IndexResult {
public:
  IndexResult() :
    accuracy_(IA_NONE), values_(nullptr) {}

  IndexResult(IndexAccuracy accuracy, std::shared_ptr<BitMap> values) :
    accuracy_(accuracy), values_(values) {}

  inline IndexAccuracy accuracy() const { return accuracy_; }
  inline std::shared_ptr<const BitMap> values() const { return values_; }
  inline std::shared_ptr<BitMap> values() { return values_; }

private:
  IndexAccuracy accuracy_;
  std::shared_ptr<BitMap> values_;
};

// Base class for all document level indexes like bitmap index.
class IDocumentIndex {
 public:
   virtual ~IDocumentIndex() { }

   virtual folly::Future<IndexResult> find_candidate_documents(
     std::shared_ptr<Predicate> predicate) = 0;

};

} // namespace expreval

#endif // _INDEX_INTERFACES_H_
