#pragma once

#include <cstdint>
#include <atomic>
#include <string>

#include "Stats.h"


// cannot use namespace because this class used in typedef in gIOExecFile.h

struct EventFD {
	struct Statistics {
		std::atomic<uint64_t> read_eintr_{0}; // how many times EINTR hit on read
		std::atomic<uint64_t> read_eagain_{0}; // how many times EAGAIN hit  on read
		GaneshaDB::stats::StatsCounter<int64_t> read_ctr_; // average read value

		std::atomic<uint64_t> write_eintr_{0}; // how many times EAGAIN hit  on write
		std::atomic<uint64_t> write_eagain_{0}; // how many times EAGAIN hit  on read
		std::atomic<uint64_t> write_ctr_{0}; // how many times write called

		void clear();
		std::string ToString() const;
	} stats_;

	EventFD();

	~EventFD();

	EventFD(const EventFD &) = delete;

	EventFD(EventFD &&other) {
		evfd_ = other.evfd_;
		other.evfd_ = -1;
	}

	EventFD &operator=(const EventFD &) = delete;

	operator int() const { return evfd_; }

	int getfd() const { return evfd_; }

	// made func static so it can be called when EventFD object not available
	static int readfd(int fd, EventFD* evfd = nullptr);

	int readfd();

	int writefd();

private:
	int evfd_{-1};
};
