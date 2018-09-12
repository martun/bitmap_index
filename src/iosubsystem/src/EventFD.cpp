#include "EventFD.h"

#include <sys/eventfd.h>
#include <unistd.h>
#include <glog/logging.h>
#include <sstream>

void EventFD::Statistics::clear() {
	read_ctr_.reset();
	read_eagain_ = 0;
	read_eintr_ = 0;

	write_ctr_ = 0;
	write_eagain_ = 0;
	write_eintr_ = 0;
}

std::string EventFD::Statistics::ToString() const {
	std::ostringstream os;
	os << ",read_ctr=" << read_ctr_
		<< ",read_eintr=" << read_eintr_
		<< ",read_eagain=" << read_eagain_
		<< ",write_ctr=" << write_ctr_
		<< ",write_eintr=" << write_eintr_
		<< ",write_eagain=" << write_eagain_;
	return os.str();
}

EventFD::EventFD() {
	evfd_ = eventfd(0, EFD_NONBLOCK);
	if (evfd_ < 0) {
		throw std::runtime_error("failed to create eventfd");
	}
	LOG(INFO) << "created eventfd=" << (void*)this << " fd=" << evfd_;
}

EventFD::~EventFD() {
	if (evfd_ != -1) {
		close(evfd_);
		LOG(INFO) << "destroyed eventfd=" << (void*)this << " fd=" << evfd_;
		evfd_ = -1;
	}
}

// made func static so it can be called when EventFD object not available
int EventFD::readfd(int fd, EventFD* evfd) {
	int ret = -1;
	eventfd_t value = 0;
	int32_t numIntrLoops = 0;
	do {
		ret = eventfd_read(fd, &value);
		numIntrLoops ++;
	} while (ret < 0 && errno == EINTR);

	if (evfd) {
		evfd->stats_.read_eintr_ = numIntrLoops;
	}

	if (ret == 0) {
		ret = value;
		if (evfd) {
			evfd->stats_.read_ctr_ = value;
		}
	} else if (errno != EAGAIN) {
		throw std::runtime_error("failed to read eventfd=" + std::to_string(fd));
	} else {
		// it must be eagain !
		if (evfd) {
			evfd->stats_.read_eagain_ ++;
		}
	}
	return ret;
}

int EventFD::readfd() {
	return readfd(evfd_, this);
}

int EventFD::writefd() {
	uint64_t u = 1;
	int ret = 0;
	do {
		ret = eventfd_write(evfd_, static_cast<eventfd_t>(u));
		if (errno == EINTR) {
			stats_.write_eintr_ ++;
		} else if (errno == EAGAIN) {
			stats_.read_eintr_ ++;
		}
	} while (ret < 0 && (errno == EINTR || errno == EAGAIN));
	if (ret < 0) {
		throw std::runtime_error("failed to write eventfd=" + std::to_string(evfd_));
	}
	stats_.write_ctr_ ++;
	return ret;
}
