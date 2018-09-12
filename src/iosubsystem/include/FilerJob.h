#ifndef FILERJOB_H
#define FILERJOB_H

#include <libaio.h>
#include <sstream>
#include <string>
#include <folly/futures/Future.h>
#include "Queueable.h"
#include "OSUtils.h"

namespace GaneshaDB {

class IOExecutor;

enum class FileOp : int32_t {
	Nop = 0,
	Write = 1,
	Read = 2,
	// STOP - ENSURE u add to the ostream operator when you change this
};

std::ostream &operator<<(std::ostream &os, const FileOp op);

using ManagedBuffer = std::unique_ptr<char, decltype(&std::free)>;

ManagedBuffer allocateBuffer(size_t size);

struct FilerJobResult {
	FileOp        op{FileOp::Nop};
	off_t         offset{0};
	size_t        size{0};
	int           retcode{-EDOM};
	ManagedBuffer buffer{nullptr, std::free};
public:
	FileOp getIOOp () const {
		return op;
	}

	off_t getIOOffset() const {
		return offset;
	}

	size_t getIOSize() const {
		return size;
	}

	int getIOResult() const {
		return retcode;
	}

	const char* getIOBuffer() const {
		return buffer.get();
	}
};

/**
 * I/Os are issued as FilerJob. FilerJob contains device
 * to which I/O to be issued.
 *
 */
class FilerJob : public Queueable {
public:
	FileOp op_{FileOp::Nop};
	off_t offset_{0};
	size_t userSize_{0}; // size given by user

	// returned from kernel
	// default is set to value never returned from IO subsystem
	int retcode_{-EDOM};
	folly::Promise<std::unique_ptr<FilerJobResult>> promise_;

	ManagedBuffer buffer_{nullptr, std::free};

	// can ioexecutor free this job after execution ?
	bool canBeFreed_{true};
	// can ioexecutor close fd after execution
	bool closeFileHandle_{false};
	// fd to use in case of non-aligned IO
	int syncFD_{GaneshaDB::os::FD_INVALID};

	IOExecutor *executor_{nullptr}; // who executed it

	// Device/File Fd
	int fd_{GaneshaDB::os::FD_INVALID};
	// fileName set only in case of delete file
	std::string fileName_;
	// fd on which to signify io completion
	int eventFD_{GaneshaDB::os::FD_INVALID};

public:
	explicit FilerJob(const char *filename, const FileOp op, const int eventFD);

	explicit FilerJob(const int fd, const FileOp op, const int eventFD);

	~FilerJob();

	DISALLOW_COPY(FilerJob);
	DISALLOW_MOVE(FilerJob);

	// check if job params are safe for async io
	bool isValid(std::ostringstream &ostr);

	int32_t prepareCallblock(iocb *cb);

	void setBuffer(const off_t fileOffset, char *buffer, const size_t size);
	char* prepare(const off_t fileOffset, const size_t size);
	void reset();
};

}

#endif
