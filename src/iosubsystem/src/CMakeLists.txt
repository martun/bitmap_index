add_library(ioexec
        IOExecutor.cpp
        EventFD.cpp
        FilerJob.cpp
        DiskIOThreadPool.cpp
        )

target_link_libraries(ioexec
  pthread_nonshared
  pthread
  glog
  gflags
  folly
  dl
  aio
  boost_context
  ) 




