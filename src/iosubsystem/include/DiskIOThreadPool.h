#ifndef DISK_IO_THREAD_POOL_H
#define DISK_IO_THREAD_POOL_H

#include <vector>
#include <memory>

#include <IOExecutor.h>
#include <FilerJob.h>
#include <EventFD.h>
#include <FutureExecutor.h>
#include <FiberIOExecutor.h>
#include <LangUtils.h>

namespace folly {
	class EventHandler;
}

class DiskIOThreadPool {

	private:

	struct PerCoreInfo {
		GaneshaDB::IOExecutor* io_exec_ptr_{nullptr}; // TODO shared_ptr
		EventFD event_fd_;
		std::shared_ptr<GaneshaDB::FutureExecutor<GaneshaDB::FiberIOExecutor>> future_exec_;
		folly::EventHandler* event_handler_{nullptr};

		explicit PerCoreInfo(CoreId core_id);
		~PerCoreInfo();
	};

	std::vector<std::unique_ptr<PerCoreInfo>> per_core_vec_;

	public:

	static DiskIOThreadPool& getInstance();

	// for each core
	// create IOExecutor, EventFD and FiberManager with one thread
	explicit DiskIOThreadPool();

	DISALLOW_COPY(DiskIOThreadPool);
	DISALLOW_MOVE(DiskIOThreadPool);

	~DiskIOThreadPool();

	folly::Future<std::unique_ptr<GaneshaDB::FilerJobResult>> submitWriteTask(const int fd,
		const off_t offset, const size_t size, const char *bufferp);

	folly::Future<std::unique_ptr<GaneshaDB::FilerJobResult>> submitReadTask(const int fd,
		const off_t offset, const size_t size);

	std::shared_ptr<GaneshaDB::FutureExecutor<GaneshaDB::FiberIOExecutor>> getFiberExecutor();

	// when to call ?
	// ensure no outstanding IO
	void shutdown();

private:
	GaneshaDB::IOExecutor* getIOExecutor();

	int getEventFD();
};

#endif
