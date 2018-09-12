#ifndef QUEUEABLE_H
#define QUEUEABLE_H

#include <assert.h>
#include <memory>
#include "Timer.h"
#include "LangUtils.h"

namespace GaneshaDB {

/**
 * Any item which goes into a queue can have its
 * wait time and service time recorded, by deriving
 * from this class.  Statistics framework will
 * use this info
 */
class Queueable {
	int64_t waitTime_{0};
	// time diff between call to submitTask and job submit to kernel

	int64_t serviceTime_{0};
	// time spent in kernel io execution

	GaneshaDB::stats::Timer timer_;

public:
	friend class IOExecutor;

	explicit Queueable() : waitTime_(0), serviceTime_(0) {}

	~Queueable() { waitTime_ = serviceTime_ = 0; }

	DISALLOW_COPY(Queueable);
	DISALLOW_MOVE(Queueable);

	int64_t waitTime() const { return waitTime_; }
	int64_t serviceTime() const { return serviceTime_; }

	void setSubmitTime() {
		timer_.start(); // capture current time
		assert(waitTime_ == 0);
		assert(serviceTime_ == 0);
	}

	void setWaitTime() {
		assert(waitTime_ == 0);
		assert(serviceTime_ == 0);
		waitTime_ = timer_.elapsedNanoseconds();
		timer_.start();
	}

	// this method called on error or when doing sync io
	// in these cases, the job does not go through ioexec queue
	// so just set waitTime to 1 
	void setSubmitAndWaitTime() {
		waitTime_ = 1LL;
		timer_.start();
	}

	void setServiceTime() {
		assert(waitTime_ != 0);
		assert(serviceTime_ == 0);
		serviceTime_ = timer_.elapsedNanoseconds();
	}
};

}

#endif
