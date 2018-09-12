#include <unistd.h>
#include <glog/logging.h>
#include "FilerJob.h"
#include "OSUtils.h"

#include <sstream>

namespace GaneshaDB {

using GaneshaDB::os::FD_INVALID;
using GaneshaDB::os::IsDirectIOAligned;
using GaneshaDB::os::RoundToNext512;

ManagedBuffer allocateBuffer(size_t size) {
	void *bufp = {nullptr};
	size_t alignedSize = RoundToNext512(size);
	auto rc    = posix_memalign(&bufp, GaneshaDB::os::DirectIOSize, alignedSize);
	assert(rc == 0 && bufp != nullptr);
	(void)rc;
	return ManagedBuffer(reinterpret_cast<char *>(bufp), std::free);
}

std::ostream &operator<<(std::ostream &os, FileOp op) {
	switch (op) {
	case FileOp::Nop:
		os << "Nop";
		break;
	case FileOp::Write:
		os << "Write";
		break;
	case FileOp::Read:
		os << "Read";
		break;
	default:
		os << "Unknown";
		break;
	}
	return os;
}

FilerJob::FilerJob(const char *filename, const FileOp op, const int eventFD) :
		op_(op), fileName_(filename), eventFD_(eventFD) {
	assert("disabled.  open fd in this func to enable" == 0);
	assert(waitTime() == 0);
	assert(serviceTime() == 0);
	assert(filename != nullptr);
}

FilerJob::FilerJob(const int fd, FileOp op, const int eventFD) :
		op_(op), fd_(fd), eventFD_(eventFD) {
	assert(waitTime() == 0);
	assert(serviceTime() == 0);
	assert(fd != FD_INVALID);
}

FilerJob::~FilerJob() {
	if (closeFileHandle_) {
		::close(fd_);
	}
}

int32_t FilerJob::prepareCallblock(iocb *cb) {
	if (this->op_ == FileOp::Write) {
		io_prep_pwrite(cb, this->fd_, this->buffer_.get(), this->userSize_, this->offset_);
	} else if (this->op_ == FileOp::Read) {
		io_prep_pread(cb, this->fd_, this->buffer_.get(), this->userSize_, this->offset_);
	} else {
		assert("which op" == 0);
	}

	cb->data = this; // set back pointer from kernel callback structure to userspace request
	this->setWaitTime();

	VLOG(1) << " job=" << (void *)this << " op=" << this->op_
		<< " fd=" << this->fd_ << " offset=" << this->offset_
		<< " size=" << this->userSize_;

	return 0;
}

/**
 * are job params valid for async io
 */
bool FilerJob::isValid(std::ostringstream &ostr) {
	bool isValid = true;

	if ((op_ != FileOp::Read) && (op_ != FileOp::Write)) {
		return isValid;
	}

	if (not GaneshaDB::os::IsFdOpen(fd_)) {
		ostr << ":fd=" << fd_ << " has errno=" << errno;
		isValid = false;
	}
	if (not IsDirectIOAligned(offset_)) {
		ostr << ":offset=" << offset_ << " not aligned";
		isValid = false;
	}
	if (not IsDirectIOAligned((uint64_t)buffer_.get())) {
		ostr << ":buffer=" << buffer_.get() << " not aligned";
		isValid = false;
	}
	if (not IsDirectIOAligned(userSize_)) {
		ostr << ":size=" << userSize_ << " not aligned";
		isValid = false;
	}

	return isValid;
}

/**
 * we allocate buffer for use in IO
 */
char *FilerJob::prepare(const off_t fileOffset, const size_t size) {
	assert(buffer_ == nullptr);

	userSize_ = size;
	offset_ = fileOffset;
	if (!(IsDirectIOAligned(fileOffset) && IsDirectIOAligned(size))) {
		// this IO will be done synchronously via pwrite/pread
		// we must open a new fd instead of changing flags on current fd
		// because there may be other async IO which could be using the existing fd
		// we dont want them to fail
		int old_flags = fcntl(fd_, F_GETFL);
		assert(old_flags != -1); // TODO Error check
		syncFD_ = dup(fd_);
		if (old_flags & O_DIRECT) {
			int changed_flags = old_flags & (~O_DIRECT);
			(void)changed_flags;
			int ret = fcntl(syncFD_, F_SETFL, changed_flags);
			if (ret != 0) {
				std::stringstream ss;
				ss << "Unable to copy file descriptor in FilerJob::prepare, error code: " << errno;
				throw std::runtime_error(ss.str());
			}
			(void)ret;
		}
	}

	buffer_ = allocateBuffer(userSize_);
	assert(waitTime() == 0);
	assert(serviceTime() == 0);
	return buffer_.get();
}

/**
 * Called when IO is complete
 */
void FilerJob::reset() {
	auto resultp     = std::make_unique<FilerJobResult>();
	resultp->op      = this->op_;
	resultp->offset  = this->offset_;
	resultp->size    = this->userSize_;
	resultp->retcode = this->retcode_;
	resultp->buffer  = std::move(this->buffer_);

	if (syncFD_ != GaneshaDB::os::FD_INVALID) {
		int ret = close(syncFD_);
		assert(ret == 0);
		(void) ret;
	}

	assert(waitTime() != 0);
	setServiceTime();
	promise_.setValue(std::move(resultp)); // wake up the future
}

}
