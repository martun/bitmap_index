# IOExecutor design notes

## Handling of non-aligned reads & writes

IOExecutor is a wrapper over io_submit(2).  io_submit() requires a file descriptor which has been opened with O_DIRECT.  But pwrite(2)/pread(2) on such an fd at an unaligned offset or with unaligned size fails.

How should IOExecutor handle such an IO request?

There are two solutions

1. Duplicate fd and remove the O_DIRECT flag from it.

   Do dup(fd) and remove the O_DIRECT flag on it using fcntl(F_SETFL).  The problem with this approach is that it can introduce inconsistent reads/writes.  The IO done with O_DIRECT can bypass the VFS buffer cache.  What if you did a pwrite() at an offset which enters the buffer cache.  Then you did async async read at same offset using io_submit().  The io_submit() is going to read stale data.

2. Read-modify-write

   An fd opened with O_DIRECT can do read of unaligned size(not 512-aligned). But it cannot do a read at unaligned offset.  To address this, the IOExecutor will have to allocate a bigger internal buffer and read at an adjusted offset.  It also cannot do write at unaligned offset or size.  To address this, the IOExecutor will have to a read, modify, write.  This will require additional read FilerJobs at both the front and the back of the desired write [offset, offset+size] range.

For now, it was decided to take Solution 1, because IOExecutor will be invoked in the context of Parquet writes and reads.  The read of the Parquet file (via Cursor) will be done only after the entire Parquet file is written and synced to disk. This ensures that the inconsistency mentioned in Solution 1 will not occur.

## Interface with folly::EventBase 

See the tests for an example of how to interface IOExecutor with folly::EventBase

1. EventHandler

You have to derive from folly::EventHandler and in the handlerReady() function.  you have to call IOExecutor->handleDiskCompletion() method

2. AsyncTimeout

Since IOExecutor submits IO to the kernel only when requestQueueSize exceeds minSubmitSize there can be cases where some IOs get stuck in the requestQueue and need to be flushed.  For this, an async timeout handler needs to be added to EventBase.



