#pragma once

#include <folly/executors/thread_factory/ThreadFactory.h>
#include <OSUtils.h>  
#include <thread>
#include <memory>

namespace GaneshaDB {

/**
 * This factory will bind each new thread to a specific core
 */
class BoundThreadFactory : public folly::ThreadFactory
{
  public:
    explicit BoundThreadFactory(CoreId core)
      : core_(core)
    {
    }

    std::thread newThread(folly::Func&& func) override {
      auto newFunc = [this, moveFunc = std::move(func)] () mutable {
		// first bind to core, then execute user function
        GaneshaDB::os::BindThreadToCore(core_);  
        moveFunc();
      };
      return std::thread(std::move(newFunc));
    }

  private:
    CoreId core_;
};

}
