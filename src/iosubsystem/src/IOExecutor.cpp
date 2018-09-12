#include "IOExecutor.h"
#include "FilerJob.h"

#include <errno.h>
#include <sstream>     // open
#include <sys/epoll.h> // epoll_event

#include <sstream>       //
#include <sys/eventfd.h> // EFD_NONBLOCk
//#include <boost/program_options.hpp>
#include <glog/logging.h>

using namespace GaneshaDB;
using namespace GaneshaDB::stats;
using GaneshaDB::os::IsDirectIOAligned;

#define EPOLL_MAXEVENT 10 // arbitrary number

namespace GaneshaDB {

FilerCtx::FilerCtx() {}

int32_t FilerCtx::init(size_t queueDepth) {
	ioQueueDepth_ = queueDepth;

	int retcode = 0;

	do {
		bzero(&ioCtx_, sizeof(ioCtx_));

		retcode = io_queue_init(queueDepth, &ioCtx_);
		if (retcode != 0) {
			LOG(ERROR) << "Failed to init io queue errno=" << errno;
			break;
		}
		numAvailable_ = ioQueueDepth_;

	} while (0);

	return retcode;
}

FilerCtx::~FilerCtx() {

	int retcode = io_queue_release(ioCtx_);
	if (retcode < 0) {
		LOG(WARNING) << "Failed to release ioctx errno=" << retcode;
	}
	(void)retcode;
}

std::string FilerCtx::getState() const {
	std::ostringstream s;
	// json format
	s << " \"ctx\":{\"numAvail\":" << numAvailable_
		<< ",\"queueDepth\":" << ioQueueDepth_ << "}";
	return s.str();
}

// ============

IOExecutor::Config IOExecutor::defaultConfig_;

IOExecutor::Config::Config() { setDerivedParam(); }

IOExecutor::Config::Config(uint32_t queueDepth) : queueDepth_(queueDepth) {
	setDerivedParam();
}

void IOExecutor::Config::setDerivedParam() {
	maxRequestQueueSize_ = queueDepth_;
}

void IOExecutor::Config::print() const {
	LOG(INFO) << " \"queueDepth\":" << queueDepth_
		<< ",\"maxRequestQueueSize\":" << maxRequestQueueSize_
		<< ",\"minSubmitSize\":" << minSubmitSize_;
}

/*
namespace po = boost::program_options;

int IOExecutor::Config::addOptions(
    boost::program_options::options_description &desc) {

  po::options_description ioexecOptions("ioexec config");

  ioexecOptions.add_options()("ioexec.ctx_queue_depth",
                              po::value<uint32_t>(&queueDepth_),
                              "io depth of each context in IOExecutor");

  desc.add(ioexecOptions);

  return 0;
}
*/

// ================

void IOExecutor::Statistics::incrementOps(const FilerJob *job) {

	OpStats* statsPtr = nullptr;
	if (job->op_ == FileOp::Read) {
		statsPtr = &read_;
	} else if (job->op_ == FileOp::Write) {
		statsPtr = &write_;
	}

	if (statsPtr) {
		assert(job->userSize_);
		if (job->retcode_ != 0) {
			statsPtr->numFailed_ ++;
		} else {
			statsPtr->numOps_++;
			statsPtr->numBytes_ += job->userSize_;
			statsPtr->waitTime_ = job->waitTime();
			statsPtr->serviceTime_ = job->serviceTime();
			statsPtr->waitHist_ = job->waitTime();
			statsPtr->serviceHist_ = job->serviceTime();
		}
		numCompleted_++;
	} else {
		LOG(WARNING) << "stats saw bad op=" << job->op_;
	}
}

void IOExecutor::Statistics::print() const {

	LOG(INFO) << getState();

	if ((numQueued_ + numSynchronous_ != numCompleted_) ||
			(numQueued_ != numSubmitted_)) {
		LOG(ERROR) << "mismatch in IOExecutor stats "
			<< " numQueued=" << numQueued_
			<< ",numSynchronous=" << numSynchronous_
			<< ",numSubmitted=" << numSubmitted_
			<< ",numCompleted=" << numCompleted_;
	}
}

std::string IOExecutor::Statistics::getState() const {
	std::ostringstream s;

	// json format TODO use jansson
	s << "{\"stats\":{"
		<< "\"read\":" << read_.getState()
		<< ",\"write\":" << write_.getState()
		<< ",\"numCompleted\":" << numCompleted_
		<< ",\"numSynchronous\":" << numSynchronous_
		<< ",\"maxRequestQueueSize\":" << maxRequestQueueSize_
		<< ",\"minSubmitSize\":" << minSubmitSize_
		<< ",\"interArrival(nsec)\":" << interArrivalNsec_
		<< ",\"interArrivalHist\":" << interArrivalHist_
		<< ",\"numProcessedInLoop\":" << numProcessedInLoop_
		<< ",\"numCompletionEvents\":" << numCompletionEvents_
		<< ",\"numInlineFlushes\":" << numInlineFlushes_
		<< ",\"numExternalFlushes\":" << numExternalFlushes_
		<< ",\"numCompletionFlushes\":" << numCompletionFlushes_
		<< ",\"numTimesCtxEmpty\":" << numTimesCtxEmpty_
		<< ",\"requestQueueFull\":" << requestQueueFull_ << "}}";

	return s.str();
}

std::string IOExecutor::Statistics::OpStats::getState() const {
	std::ostringstream s;

	// json format
	s << " {\"numOps\":" << numOps_ << ",\"numBytes\":" << numBytes_
		<< ",\"waitTime\":" << waitTime_ << ",\"waitHist\":" << waitHist_
		<< ",\"serviceTime\":" << serviceTime_
		<< ",\"serviceHist\":" << serviceHist_ << "}";

	return s.str();
}

// ===================

IOExecutor::IOExecutor(const Config &config) : config_(config),
		requestQueue_(config_.queueDepth_) {

	config_.print();

	setMinSubmitSize(config_.minSubmitSize_);

	ctx_.init(config_.queueDepth_);

	state_ = State::RUNNING;

	LOG(INFO) << "IOExecutor ptr=" << (void *)this
		<< " ioctx size=" << ctx_.numAvailable_
		<< " minSubmitSize=" << minSubmitSize_;

	if (not requestQueue_.is_lock_free()) {
		LOG(WARNING) << "queue is not lockfree";
	}
}

IOExecutor::~IOExecutor() {
	if (state_ != State::TERMINATED) {
		stop();
	}
}

void IOExecutor::stop() {

	stats_.print();

	state_ = State::TERMINATED;
}

bool IOExecutor::setMinSubmitSize(const int32_t minSubmitSz)  {
	// bound check
	if ((minSubmitSz > 0) && (minSubmitSz <= config_.queueDepth_)) {
		minSubmitSize_ = minSubmitSz;
		stats_.minSubmitSize_ = minSubmitSz;
		return true;
	}
	return false;
}

void IOExecutor::updateInterArrivalStats(const Timer& current) {
	if (dce_likely(stats_.read_.numOps_ > 1) ||
			dce_likely(stats_.write_.numOps_ > 1)) {
		// calc diff after first job
		auto diff = current.differenceNanoseconds(prevJobSubmitTime_);
		stats_.interArrivalNsec_ = diff;
		stats_.interArrivalHist_ = diff;
	}
	prevJobSubmitTime_ = current;
}

int32_t IOExecutor::ProcessRequestQueue(CallType calledFrom) {
	int32_t numToSubmit = 0;

	iocb *post_iocb[ctx_.ioQueueDepth_];
	// post_iocb can be freed after io_submit()

	// as per linux source code, io_submit() makes
	// a copy of iocb [using copy_from_user() in fs/aio.c]
	// that is why we can allocate iocb array on stack
	// and free it after io_submit
	iocb cbVec[ctx_.ioQueueDepth_];

	while (ctx_.isEmpty()) {
		// lets see if we can get any free ctx
		stats_.numTimesCtxEmpty_ ++;
		handleDiskCompletion(-1);
	}

	while (not ctx_.isEmpty() && requestQueueSize_) {

		if (ctx_.decrementNumAvailable(1) <= 0) {
			// atomically grab a free io permit
			// it can already be zero due to race condition with other threads
			// which are also in this same while loop
			// in that case, reincrement it and continue waiting
			ctx_.incrementNumAvailable(1);
			continue;
		}

		// atomically grab a job
		const bool gotJob = requestQueue_.consume_one( [&] (FilerJob* job) {
				requestQueueSize_.fetch_sub(1);

				iocb* cb = nullptr;
				post_iocb[numToSubmit] = cb = &cbVec[numToSubmit];
				numToSubmit ++;

				job->prepareCallblock(cb);
				io_set_eventfd(cb, job->eventFD_);

				});

		// if could not grab job, give back io permit
		if (not gotJob) {
			ctx_.incrementNumAvailable(1);
			break;
		}
	}

	int32_t numSubmitted = 0;

	if (numToSubmit) {
		VLOG(1) << "to submit num io=" << numToSubmit;

		{
			int iosubmitRetcode = 0;
			do {
				iosubmitRetcode = io_submit(ctx_.ioCtx_, numToSubmit, &post_iocb[0]);
			} while ((iosubmitRetcode == -EINTR) || (iosubmitRetcode == -EAGAIN));

			/*
			 * if errcode < 0
			 *   if errcode = -EINTR/-EAGAIN
			 *     retry
			 *   else
			 *     abort
			 * else
			 *   if not all ios submitted
			 *     abort the ones which did not make it
			 *   else
			 *     done
			 */

			if (iosubmitRetcode >= 0) {
				// only increment as many as are submitted
				numSubmitted = iosubmitRetcode;

				stats_.numSubmitted_ += numSubmitted;
				stats_.numProcessedInLoop_ = numSubmitted;
				if (calledFrom == CallType::EXTERNAL) {
					stats_.numExternalFlushes_ ++;
				} else if (calledFrom == CallType::INLINE) {
					stats_.numInlineFlushes_ ++;
				} else if (calledFrom == CallType::COMPLETION) {
					stats_.numCompletionFlushes_ ++;
				}
			}

			{
				// if you come here, its because fd is invalid OR
				// offset or buffer is not 512 aligned
				std::ostringstream ostr;

				// abort all remaining iocb
				for (int32_t idx = numSubmitted; idx < numToSubmit; idx++) {

					iocb *cb = post_iocb[idx];
					if (not cb) {
						ostr << ":iocb at index=" << idx << " became null";
					} else {
						FilerJob *job = static_cast<FilerJob *>(cb->data);
						if (job) {
							bool isValidRet = job->isValid(ostr);
							(void)isValidRet; // ignore, ostr has the details
							job->retcode_ = iosubmitRetcode;
							doPostProcessingOfJob(job);
						} else {
							LOG(ERROR) << "found cb data to be null for cb=" << (void *)cb;
						}
					}
				}

				if (numToSubmit != numSubmitted) {
					LOG(ERROR) << "Failed io_submit total=" << numToSubmit << " could_submit=" << numSubmitted
						<< " got errno=" << iosubmitRetcode
						<< " with errors=" << ostr.str();
				}
				// put back unused io permits
				ctx_.incrementNumAvailable(numToSubmit - numSubmitted);
			}
		}
	}

	return (numToSubmit - numSubmitted);
}

folly::Future<std::unique_ptr<FilerJobResult>> IOExecutor::submitWriteTask(
		const int fd, const int eventfd, const off_t offset, const size_t size,
		const char *bufferp) {
	assert(bufferp != nullptr);
	auto jobp = new FilerJob(fd, FileOp::Write, eventfd);
	auto bufp = jobp->prepare(offset, size);
	std::memcpy(bufp, bufferp, size);
	return submitTask(jobp);
}

folly::Future<std::unique_ptr<FilerJobResult>> IOExecutor::submitReadTask(
		const int fd, const int eventfd, const off_t offset, const size_t size) {
	auto jobp = new FilerJob(fd, FileOp::Read, eventfd);
	auto bufp = jobp->prepare(offset, size);
	(void) bufp;
	return submitTask(jobp);
}

folly::Future<std::unique_ptr<FilerJobResult>>
IOExecutor::submitTask(FilerJob *job) {
	auto fut = job->promise_.getFuture();

	do {

		if (state_ != RUNNING) {
			// signal to caller that no more jobs
			LOG(ERROR) << "shutting down. rejecting job=" << (void *)job;
			job->setSubmitAndWaitTime();
			job->retcode_ = -ENXIO;
			job->reset();
			break;
		}

		if (job->op_ == FileOp::Write &&
				(not IsDirectIOAligned(job->userSize_) ||
				not IsDirectIOAligned(job->offset_))) {

			job->setSubmitAndWaitTime();
			updateInterArrivalStats(job->timer_);
			job->executor_ = this;

			int fd = (job->syncFD_ != GaneshaDB::os::FD_INVALID) ? job->syncFD_ : job->fd_;
			ssize_t ret = pwrite(fd, job->buffer_.get(), job->userSize_, job->offset_);
			int error = errno;
			stats_.numSynchronous_ ++;
			job->retcode_ = (ret == (ssize_t)job->userSize_) ? 0 : -error;
			doPostProcessingOfJob(job);
			break;
		}


		if (job->op_ == FileOp::Write || job->op_ == FileOp::Read) {

			int32_t prevNumAvailable = ctx_.numAvailable_;
			while (ctx_.numAvailable_ < minSubmitSize_) {
				ProcessRequestQueue(CallType::INLINE);
				handleDiskCompletion(-2);
				if (prevNumAvailable == ctx_.numAvailable_) {
					break; // dont loop forever
				}
				prevNumAvailable = ctx_.numAvailable_;
			}

			if (requestQueueSize_ >= config_.maxRequestQueueSize_) {
				job->setSubmitAndWaitTime();
				stats_.requestQueueFull_++;
				job->retcode_ = -EAGAIN;
				job->reset();
				break;
			}

			job->setSubmitTime();
			updateInterArrivalStats(job->timer_);
			job->executor_ = this;

			// increment requestQueueSize before push into requestQueue to avoid race condition
			stats_.maxRequestQueueSize_ = ++ requestQueueSize_;
			requestQueue_.push(job);
			stats_.numQueued_++;

			if (requestQueueSize_ >= minSubmitSize_) {
				ProcessRequestQueue(CallType::INLINE);
			}
		} else {
			LOG(ERROR) << "bad op=" << job->op_;
			job->setSubmitAndWaitTime();
			job->retcode_ = -EINVAL;
			job->reset();
		}
	} while (0);

	return fut;
}

/*
 * called when kernel notifies eventFD that submitted async IO
 * has completed
 */
int IOExecutor::handleDiskCompletion(const int numExpectedEvents) {

	timespec ts = {0, 1000 };
	int minEvents = 1;
	const int maxEvents = ctx_.ioQueueDepth_;
	bool isExternalCall = (numExpectedEvents > 0);

	if (isExternalCall) {
		minEvents = numExpectedEvents;
	}
	if (minEvents > maxEvents) {
		// minevents should be less than max else u get EINVAL
		minEvents = maxEvents;
	}

	io_event readyIOEvents[maxEvents];
	bzero(readyIOEvents, sizeof(io_event) * maxEvents);

	int numEventsGot = 0;

	do {
		numEventsGot = io_getevents(
				ctx_.ioCtx_, minEvents, maxEvents,
				&readyIOEvents[0], &ts);
	} while ((numEventsGot == -EINTR) || (numEventsGot == -EAGAIN));

	if (numEventsGot > 0) {
		stats_.numCompletionEvents_ = numEventsGot;
		// process the bottom half on all completed jobs in the io context
		int ret = ProcessCallbacks(readyIOEvents, numEventsGot);
		assert (ret == 0); // TODO: handle errors
		(void) ret;
		VLOG(1) << "completed callbacks for " << numEventsGot;
		// if we are called from external handler
	// and enuf ctx got freed up, submit more IO
		if (isExternalCall && not ctx_.isEmpty()) {
			ProcessRequestQueue(CallType::COMPLETION);
		}
	} else if (numEventsGot < 0) {
		if (isExternalCall) {
			LOG(ERROR) << "getevents error=" << numEventsGot
				<< " min=" << minEvents
				<< " max=" << maxEvents;
		}
	}

	return numEventsGot;
}

int32_t IOExecutor::ProcessCallbacks(io_event *events, const int32_t numEvents) {
	int32_t error = 0;

	for (int32_t idx = 0; idx < numEvents; ++idx) {
		// io_event.obj = pointer to iocb, which was submitted in io_submit()
		// io_event.res = on success, it is size of buffer submitted
		//                on failure, it is negative errno
		// io_event.res2 = always seem to be 0 as per test
		// io_event.data = the iocb.data that was set during io_submit()
		FilerJob *job = reinterpret_cast<FilerJob *>(events[idx].data);

		if ((ssize_t)events[idx].res < 0) {
			job->retcode_ = events[idx].res;
			LOG(ERROR) << "IOerror for job=" << (void *)job << ":fd=" << job->fd_
				<< ":op=" << job->op_ << ":size=" << job->userSize_
				<< ":offset=" << job->offset_ << ":error=" << job->retcode_;
		} else if ((events[idx].res != job->userSize_) &&
				(events[idx].res != job->userSize_)) {

			job->retcode_ = -EIO;
			LOG(ERROR) << "partial read/write for job=" << (void *)job
				<< ":fd=" << job->fd_ << ":op=" << job->op_
				<< ":expected size=" << job->userSize_
				<< ":actual size=" << events[idx].res
				<< ":offset=" << job->offset_;
		} else {
			job->retcode_ = 0;
		}

		doPostProcessingOfJob(job);
		ctx_.incrementNumAvailable();
	}

	return error;
}

/**
 * In success or failure, first set job->retcode_
 * and then call this function
 */
int32_t IOExecutor::doPostProcessingOfJob(FilerJob *job) {
	job->reset();
	// incrementOps() has to be done after reset() because
	// reset() sets serviceTime , which is used by stats
	stats_.incrementOps(job);
	if (job->closeFileHandle_) {
		close(job->fd_);
	}
	if (job->canBeFreed_) {
		delete job;
	}
	return 0;
}

std::string IOExecutor::getState() const {
	std::ostringstream s;
	s << "{" << ctx_.getState() << "," << stats_.getState() << "}" << std::endl;
	return s.str();
}
} // namespace
