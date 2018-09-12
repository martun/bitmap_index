#
# See
# https://cmake.org/cmake/help/v3.0/manual/cmake-commands.7.html
# http://www.slideshare.net/DanielPfeifer1/cmake-48475415
#
cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

# Project(<name> VERSION <version> LANGUAGES CXX)
project(Index C CXX ASM)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" "${CMAKE_MODULE_PATH}")

# turns on colors in generated Makefile
set(CMAKE_COLOR_MAKEFILE)

# overwriting the source and binary directories with the current ones
# this is useful for other projects reusing this project as a subcomponent
set(CMAKE_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(CMAKE_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR})

# this will prohibit in-source-builds
if(${CMAKE_BINARY_DIR} STREQUAL ${CMAKE_SOURCE_DIR})
    message(STATUS "In-source-builds are not allowed")
    message(STATUS "Clean your source directory (e.g. delete the CMakeCache.txt file)")
    message(FATAL_ERROR "Please create a separate build directory and call CMake again")
endif(${CMAKE_BINARY_DIR} STREQUAL ${CMAKE_SOURCE_DIR})

# CMAKE_BUILD_TYPE can be Release/Debug
if (NOT CMAKE_BUILD_TYPE)
    message(STATUS "No build type selected, default to Debug")
    message(STATUS "To change pass -DCMAKE_BUILD_TYPE on command line")
    set(CMAKE_BUILD_TYPE "Debug")
endif()

set (CXX_STANDARD "c++14")

#include(FindGTEST)
enable_testing()

# separate out the C++ options which are not used in C
# add boost logger macro
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=${CXX_STANDARD} -Woverloaded-virtual -DBOOST_LOG_DYN_LINK")

# these below apply to C and C++ files
add_compile_options(
        -Wall
        -Wextra
        -Werror
        -Wstrict-aliasing
        -Wno-unused-variable
        -Wno-unused-function
        -Wno-unused-parameter
        -Wno-missing-field-initializers
        -Wno-unused-but-set-parameter
        -Wno-deprecated-declarations # boost_iostreams uses auto_ptr
        -Wchar-subscripts
        -Wpointer-arith
        -Wno-sign-compare
        -Wformat
        -Wformat-security
        -Werror=format-security
        -fstack-protector-all
        -fPIE
        -fpie
        -fPIC
        -fpic
        -pipe
        -fdata-sections
        -ffunction-sections
        -DRAPIDJSON_HAS_STDSTRING
)
include(CTest)

link_directories(

        ${CMAKE_SOURCE_DIR}/third_parties/lib/
)

# all headers specified
include_directories(
        ${CMAKE_SOURCE_DIR}/src/bitmap_index
        ${CMAKE_SOURCE_DIR}/src/common
        ${CMAKE_SOURCE_DIR}/src/iosubsystem/include
        ${CMAKE_SOURCE_DIR}/third_parties/include
)


message(${CMAKE_SOURCE_DIR})


add_subdirectory(src)
