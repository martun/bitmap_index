/*
 *  The version of this class in wangle does not compile with folly
 *  Hence brought it here and made requisite modifications
 *  Do a diff with wangle/concurrent to find difference
 */

/*
 *  Copyright (c) 2017, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#pragma once

#include <folly/io/async/EventBaseManager.h>
#include <folly/fibers/FiberManagerMap.h>
#include <folly/Function.h>
#include <folly/executors/IOExecutor.h>

namespace GaneshaDB {

/**
 * @class FiberIOExecutor
 * @brief An IOExecutor that executes funcs under mapped fiber context
 *
 * A FiberIOExecutor wraps an IOExecutor, but executes funcs on the FiberManager
 * mapped to the underlying IOExector's event base.
 */
class FiberIOExecutor : public folly::IOExecutor {

public:
	explicit FiberIOExecutor(const std::shared_ptr<folly::IOExecutor>& ioExecutor,
			const folly::fibers::FiberManager::Options& fiberOpt)	
			: ioExecutor_(ioExecutor) {
		// dummy call done to initialize fiberManager right here
		(void) folly::fibers::getFiberManager(*getEventBase(), fiberOpt);
	}

	virtual void add(folly::Func f) override {
		auto eventBase = ioExecutor_->getEventBase();
		folly::fibers::getFiberManager(*eventBase).add(std::move(f));
	}

	virtual folly::EventBase* getEventBase() override {
		return ioExecutor_->getEventBase();
	}

private:
	std::shared_ptr<folly::IOExecutor> ioExecutor_;
};

} // namespace wangle
