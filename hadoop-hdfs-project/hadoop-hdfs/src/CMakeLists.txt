#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

# Default to release builds
set(CMAKE_BUILD_TYPE, Release)

# If JVM_ARCH_DATA_MODEL is 32, compile all binaries as 32-bit.
# This variable is set by maven.
if (JVM_ARCH_DATA_MODEL EQUAL 32)
    # force 32-bit code generation on amd64/x86_64, ppc64, sparc64
    if (CMAKE_COMPILER_IS_GNUCC AND CMAKE_SYSTEM_PROCESSOR MATCHES ".*64")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m32")
        set(CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} -m32")
    endif ()
    if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
        set(CMAKE_SYSTEM_PROCESSOR "i686")
    endif ()
endif (JVM_ARCH_DATA_MODEL EQUAL 32)

# Compile a library with both shared and static variants
function(add_dual_library LIBNAME)
    add_library(${LIBNAME} SHARED ${ARGN})
    add_library(${LIBNAME}_static STATIC ${ARGN})
    set_target_properties(${LIBNAME}_static PROPERTIES OUTPUT_NAME ${LIBNAME})
endfunction(add_dual_library)

# Link both a static and a dynamic target against some libraries
function(target_link_dual_libraries LIBNAME)
    target_link_libraries(${LIBNAME} ${ARGN})
    target_link_libraries(${LIBNAME}_static ${ARGN})
endfunction(target_link_dual_libraries)

function(output_directory TGT DIR)
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction(output_directory TGT DIR)

function(dual_output_directory TGT DIR)
    output_directory(${TGT} "${DIR}")
    output_directory(${TGT}_static "${DIR}")
endfunction(dual_output_directory TGT DIR)

# Flatten a list into a string.
function(FLATTEN_LIST INPUT SEPARATOR OUTPUT)
  string (REPLACE ";" "${SEPARATOR}" _TMPS "${INPUT}")
  set (${OUTPUT} "${_TMPS}" PARENT_SCOPE)
endfunction()

# Check to see if our compiler and linker support the __thread attribute.
# On Linux and some other operating systems, this is a more efficient 
# alternative to POSIX thread local storage. 
INCLUDE(CheckCSourceCompiles)
CHECK_C_SOURCE_COMPILES("int main(void) { static __thread int i = 0; return 0; }" HAVE_BETTER_TLS)

find_package(JNI REQUIRED)
if (NOT GENERATED_JAVAH)
    # Must identify where the generated headers have been placed
    MESSAGE(FATAL_ERROR "You must set the CMake variable GENERATED_JAVAH")
endif (NOT GENERATED_JAVAH)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -O2 -D_GNU_SOURCE")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_REENTRANT -D_FILE_OFFSET_BITS=64")

include_directories(
    ${GENERATED_JAVAH}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS}
    main/native/
)

set(_FUSE_DFS_VERSION 0.1.0)
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

add_dual_library(hdfs
    main/native/hdfs.c
    main/native/hdfsJniHelper.c
)
target_link_dual_libraries(hdfs
    ${JAVA_JVM_LIBRARY}
)
dual_output_directory(hdfs target/usr/local/lib)
set(LIBHDFS_VERSION "0.0.0")
set_target_properties(hdfs PROPERTIES
    SOVERSION ${LIBHDFS_VERSION})

add_executable(hdfs_test
    main/native/hdfs_test.c
)
target_link_libraries(hdfs_test
    hdfs
    ${JAVA_JVM_LIBRARY}
)
output_directory(hdfs_test target/usr/local/bin)

add_executable(hdfs_read
    main/native/hdfs_read.c
)
target_link_libraries(hdfs_read
    hdfs
    ${JAVA_JVM_LIBRARY}
)
output_directory(hdfs_read target/usr/local/bin)

add_executable(hdfs_write
    main/native/hdfs_write.c
)
target_link_libraries(hdfs_write
    hdfs
    ${JAVA_JVM_LIBRARY}
)
output_directory(hdfs_write target/usr/local/bin)

add_library(native_mini_dfs
    main/native/native_mini_dfs.c
)
target_link_libraries(native_mini_dfs
    hdfs
)

add_executable(test_native_mini_dfs
    main/native/test_native_mini_dfs.c
)
target_link_libraries(test_native_mini_dfs
    native_mini_dfs
)

add_executable(test_libhdfs_threaded
    main/native/test_libhdfs_threaded.c
)
target_link_libraries(test_libhdfs_threaded
    hdfs
    native_mini_dfs
    pthread
)

add_subdirectory(contrib/fuse-dfs/src)

