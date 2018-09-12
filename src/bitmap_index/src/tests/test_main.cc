#include <gtest/gtest.h>
#include <glog/logging.h>

#include <folly/init/Init.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

// this is top-level project dir
// it is used to find the path for the filter json files
std::string topLevelProjectDir;

int main(int argc, char* argv[])
{
	::testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);

  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  // Tell folly to use as many threads as there are cores on the machine.
  folly::setCPUExecutor(executor);

	// If you run "make test", the add_test() macro in this_dir/CMakeLists.txt
	// will automatically pass the directory name as first argument to the test
	// If you are running executable manually, you have to pass the top-level
	// src directory name on the command line
	if (argc == 1) {
		// if top level directory has not been passed use
		// the current directory
		topLevelProjectDir = "./";
	} else {
		topLevelProjectDir = argv[1];
	}
  	return RUN_ALL_TESTS();
}

