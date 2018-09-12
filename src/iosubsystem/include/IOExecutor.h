#ifndef IOEXECUTOR_H
#define IOEXECUTOR_H

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <libaio.h>
#include <string>
#include <vector>
#include <folly/futures/Future.h>

#include "Stats.h"
#include "Timer.h"
#include "OSUtils.h"

/*
namespace boost {
namespace program_options {
class options_description; // forward declare
}
}
*/

namespace GaneshaDB {

class FilerJob;
class FilerJobResult;
class IOExecutor;

using GaneshaDB::stats::Timer;
using GaneshaDB::os::FD_INVALID;

/*
 * FilerCtx:
 *	- FilerCtx is an async io context wrapper
 *	- Structure keeps track of parameters like
 *		ioQueueDepth_ : queue depth of io_context at async layer
 */
class FilerCtx {
public:
	io_context_t ioCtx_;

	int32_t ioQueueDepth_{0};

public:
	std::atomic<int32_t> numAvailable_{0};

	explicit FilerCtx();

	~FilerCtx();

	int32_t init(size_t queueDepth);

	std::string getState() const;

	bool isEmpty() const { return (numAvailable_ == 0); }

	bool isFull() const { return (numAvailable_ == ioQueueDepth_); }

	int32_t incrementNumAvailable(int32_t count = 1) {
		return numAvailable_.fetch_add(count);
	}
	int32_t decrementNumAvailable(int32_t count) {
		return numAvailable_.fetch_sub(count);
	}

	DISALLOW_COPY(FilerCtx);
	DISALLOW_MOVE(FilerCtx);
};

class IOExecutor {
public:
	enum CallType {
		EXTERNAL = 0,
		INLINE,
		COMPLETION
	};

	enum State : int8_t {
		NOT_STARTED,
		RUNNING,
		TERMINATED,
	};

	State state_ = NOT_STARTED;

	struct Config {
	public:

		int32_t queueDepth_ = 200;
		// 200 is good default for NVME SSDs
		// if we run on other disks, lets abstract this out

		int32_t minSubmitSize_ = 1;

		// maxRequestQueueSize need not be more than io contexts available
		int32_t maxRequestQueueSize_;

		void setDerivedParam();

		explicit Config(); // use defaults

		explicit Config(uint32_t queueDepth);

		void print() const;

		// add options needed by IOExecutor to parser config
		//int addOptions(boost::program_options::options_description &desc);
	};

	static Config defaultConfig_;
	Config config_;

	struct Statistics {
		// Most variables are incremented by a single thread
		// but it is *not* the same thread that updates all

		std::atomic<uint64_t> numQueued_{0};    // num sent to queue for io_submit()
		std::atomic<uint64_t> numSubmitted_{0}; // num submitted to kernel
		std::atomic<uint64_t> numCompleted_{0}; // num that kernel has finished
		std::atomic<uint64_t> numSynchronous_{0}; // num of pread/pwrite

		// updated by completionThread
		struct OpStats {
			GaneshaDB::stats::StatsCounter<int64_t> waitTime_;
			GaneshaDB::stats::StatsCounter<int64_t> serviceTime_;

			GaneshaDB::stats::Histogram<int64_t> waitHist_;
			GaneshaDB::stats::Histogram<int64_t> serviceHist_;

			std::atomic<uint32_t> numOps_{0};
			std::atomic<uint32_t> numFailed_{0};
			std::atomic<uint32_t> numBytes_{0};

			std::string getState() const;
		};

		// maintain per-op statistics
		OpStats read_;
		OpStats write_;

		GaneshaDB::stats::StatsCounter<int64_t> interArrivalNsec_;
		GaneshaDB::stats::Histogram<int64_t> interArrivalHist_;

		GaneshaDB::stats::MaxValue<uint32_t> maxRequestQueueSize_;
		GaneshaDB::stats::StatsCounter<uint32_t> minSubmitSize_;

		GaneshaDB::stats::StatsCounter<int64_t> numProcessedInLoop_;

		GaneshaDB::stats::StatsCounter<uint32_t> numCompletionEvents_;

		// flushes done by caller outside IOExecutor
		uint32_t numExternalFlushes_ = 0;
		// flushes done when batch size was met
		uint32_t numInlineFlushes_ = 0;
		// flushes done after kernel free some io_ctx
		uint32_t numCompletionFlushes_ = 0;

		uint32_t numTimesCtxEmpty_ = 0;
		uint32_t requestQueueFull_ = 0;

		void incrementOps(const FilerJob *job);

		void clear() { bzero(this, sizeof(*this)); }

		void print() const;

		std::string getState() const;
	} stats_;

	explicit IOExecutor(const Config &config = defaultConfig_);

	virtual ~IOExecutor();

	DISALLOW_COPY(IOExecutor);
	DISALLOW_MOVE(IOExecutor); // dont move executing obj

	/**
	 * @returns zero in case of success,
	 *          -EAGAIN in case queue is full
	 *          -ENXIO in case ioexecutor not initialized
	 *          -EINVAL in case job->op is invalid
	 *          other negative values reflecting kernel error
	 */
	folly::Future<std::unique_ptr<FilerJobResult>> submitTask(FilerJob *job);

	folly::Future<std::unique_ptr<FilerJobResult>> submitWriteTask(const int fd,
		const int eventfd, const off_t offset, const size_t size, const char *bufferp);

	folly::Future<std::unique_ptr<FilerJobResult>> submitReadTask(const int fd,
		const int eventfd, const off_t offset, const size_t size);



	/**
	 * @brief call this func to check disk IO completed
	 * @returns number of actual completed events
	 */
	int handleDiskCompletion(const int numExpectedEvents);

	void stop();

	std::string getState() const;

	/**
	 * @param calledFrom  whether called from external obj or interally
	 *       this gets recorded in stats
	 */
	int32_t ProcessRequestQueue(CallType calledFrom = CallType::EXTERNAL);

	size_t requestQueueSize() const {
		return requestQueueSize_;
	}

	// @param minSubmitSz change minSubmitSize dynamically based on
	// number of connections
	// @return whether setting was successful
	bool setMinSubmitSize(const int32_t minSubmitSz);

	int32_t minSubmitSize() const {
		return minSubmitSize_;
	}

private:

	void ProcessCompletions();
	int32_t ProcessCallbacks(io_event *events, const int32_t n_events);
	int32_t doPostProcessingOfJob(FilerJob *job);

	void updateInterArrivalStats(const Timer& current);

	Timer prevJobSubmitTime_;

	int32_t minSubmitSize_{1};

	// lockfree queue doesnt provide size() so we maintain it outside
	std::atomic<int32_t> requestQueueSize_{0};
	/**
	 * Maintain Requests added by submitTask
	 *
	 * Classes which are elements of boost lockfree queue
	 * cannot have any nontrivial c++ classes as data members
	 * therefore, this class is stored by ptr in queue
	 */
	boost::lockfree::queue<FilerJob *, boost::lockfree::fixed_sized<true>> requestQueue_;

	FilerCtx ctx_;
};

typedef std::shared_ptr<IOExecutor> IOExecutorSPtr;

} // namespace

#endif
