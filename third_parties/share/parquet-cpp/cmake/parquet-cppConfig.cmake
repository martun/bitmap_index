# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(PARQUET-CPP_VERSION x.y.z)


####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was parquet-cppConfig.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

macro(check_required_components _NAME)
  foreach(comp ${${_NAME}_FIND_COMPONENTS})
    if(NOT ${_NAME}_${comp}_FOUND)
      if(${_NAME}_FIND_REQUIRED_${comp})
        set(${_NAME}_FOUND FALSE)
      endif()
    endif()
  endforeach()
endmacro()

####################################################################################

set_and_check(PARQUET-CPP_INCLUDE_DIR "${PACKAGE_PREFIX_DIR}/include")
set_and_check(PARQUET-CPP_LIB_DIR "${PACKAGE_PREFIX_DIR}/lib")

set(_TMP_SUFFIX ${CMAKE_FIND_LIBRARY_SUFFIXES})
FIND_LIBRARY(PARQUET_LIBRARY NAMES parquet PATHS ${PARQUET-CPP_LIB_DIR})
set(PARQUET-CPP_LIBRARIES ${PARQUET_LIBRARY})

set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
FIND_LIBRARY(PARQUET_STATIC_LIBRARY NAMES parquet PATHS ${PARQUET-CPP_LIB_DIR})
set(PARQUET-CPP_STATIC_LIBRARIES ${PARQUET_STATIC_LIBRARY})
set(CMAKE_FIND_LIBRARY_SUFFIXES ${_TMP_SUFFIX})
