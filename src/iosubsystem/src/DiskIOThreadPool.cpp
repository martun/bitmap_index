#include <glog/logging.h>
#include <thread>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include <BoundThreadFactory.h>
#include "DiskIOThreadPool.h"

using GaneshaDB::IOExecutor;
using GaneshaDB::FiberIOExecutor;
using GaneshaDB::FutureExecutor;
using GaneshaDB::os::GetCpuCore;

// this handler is called everytime disk IO completes
class IOExecEventHandler : public folly::EventHandler {
public:

	IOExecEventHandler(IOExecutor* exec,
		folly::EventBase* eb, int eventFD)
		: EventHandler(eb, eventFD), eb_(eb), exec_(exec), eventFD_(eventFD)
	{
		LOG(INFO) << "registered event handler evb=" << (void*)eb_ << " fd=" << eventFD;
	}

	virtual void handlerReady(uint16_t events) noexcept override {
		// call the IOExecutor to reap completion IO events
		// this will trigger completion of waiting futures
		const int32_t numEvents = EventFD::readfd(eventFD_);
		if (numEvents > 0) {
			numDone_ += exec_->handleDiskCompletion(numEvents);
		}
	}

private:
	const folly::EventBase* eb_{nullptr};
	IOExecutor *exec_{nullptr};
	const int eventFD_{-1};
	int numDone_;
};

// =====================

DiskIOThreadPool::PerCoreInfo::PerCoreInfo(CoreId core_id) : event_fd_() {
	IOExecutor::Config config;
	io_exec_ptr_ = new IOExecutor(config);

	// only one thread in each executor which is used by all fibers
	auto iothread_exec = std::make_shared<folly::IOThreadPoolExecutor>(1, 
		std::make_shared<GaneshaDB::BoundThreadFactory>(core_id));

	folly::fibers::FiberManager::Options fiberOpt;
	fiberOpt.stackSize = 64 * 1024;

	// create a fiberManager which will return a future For every task submitted
	future_exec_ = std::make_shared<GaneshaDB::FutureExecutor<GaneshaDB::FiberIOExecutor>>(iothread_exec, fiberOpt);

	// add event handler for IOExecutor to Folly Eventbase
	event_handler_ = new IOExecEventHandler(
		io_exec_ptr_,
		future_exec_->getEventBase(), 
		event_fd_.getfd());

	auto worked = event_handler_->registerHandler(
		folly::EventHandler::READ | folly::EventHandler::PERSIST);

	// restart event loop iteration so registered handler is polled immediately
	static auto dummy = [] {};
	future_exec_->getEventBase()->runInEventBaseThread(dummy);

	if (not worked) {
		LOG(FATAL) << "failed event handler evb=" 
			<< (void*)future_exec_->getEventBase() 
			<< " fd=" << event_fd_.getfd();
	}
	LOG(INFO) << "started disk thread pool for core=" << core_id;
}

DiskIOThreadPool::PerCoreInfo::~PerCoreInfo() {
	delete event_handler_;
	delete io_exec_ptr_;
}

// =======================

DiskIOThreadPool& DiskIOThreadPool::getInstance() {
	static DiskIOThreadPool instance;
	return instance;
}

DiskIOThreadPool::DiskIOThreadPool() {
	const CoreId numcpu = std::thread::hardware_concurrency();
	LOG_IF(FATAL, numcpu <= 0) << "cannot obtain num cpus";
	for (CoreId i = 0; i < numcpu; i ++) {
		per_core_vec_.emplace_back(new PerCoreInfo(i));
	}
}

IOExecutor* DiskIOThreadPool::getIOExecutor() {
	const CoreId cpuid = GetCpuCore();
	return per_core_vec_[cpuid]->io_exec_ptr_;
}

int DiskIOThreadPool::getEventFD() {
	const CoreId cpuid = GetCpuCore();
	return per_core_vec_[cpuid]->event_fd_.getfd();
}

folly::Future<std::unique_ptr<GaneshaDB::FilerJobResult>> DiskIOThreadPool::submitWriteTask(
    const int fd, const off_t offset, const size_t size, const char *bufferp) {
    return getIOExecutor()->submitWriteTask(fd, getEventFD(), offset, size, bufferp);
}

folly::Future<std::unique_ptr<GaneshaDB::FilerJobResult>> DiskIOThreadPool::submitReadTask(
    const int fd, const off_t offset, const size_t size) {
    return getIOExecutor()->submitReadTask(fd, getEventFD(), offset, size);
}

std::shared_ptr<FutureExecutor<FiberIOExecutor>> DiskIOThreadPool::getFiberExecutor() {
	const CoreId cpuid = GetCpuCore();
	return per_core_vec_[cpuid]->future_exec_;
}

void DiskIOThreadPool::shutdown() {
	for (auto& per_core : per_core_vec_) {
		// calling destructor on each wangle IOExecutor
		// invokes their stopThread function
		per_core->future_exec_.reset();
	}
	per_core_vec_.clear();
}

DiskIOThreadPool::~DiskIOThreadPool() {
	shutdown();
}

