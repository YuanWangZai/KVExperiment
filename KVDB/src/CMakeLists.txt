include(CheckCXXCompilerFlag)
include(CheckCCompilerFlag)
option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" ON)
option(LINK_TCMALLOC "Link tcmalloc if possible" )

execute_process(
    COMMAND bash -c "find ${CMAKE_SOURCE_DIR}/../.. -type d -path \"*output/include/braft\" | xargs dirname | xargs dirname | tr -d '\n'"
    OUTPUT_VARIABLE OUTPUT_PATH
)

set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

include(FindThreads)
include(FindProtobuf)

if (NOT PROTOBUF_PROTOC_EXECUTABLE)
    get_filename_component(PROTO_LIB_DIR ${PROTOBUF_LIBRARY} DIRECTORY)
    set (PROTOBUF_PROTOC_EXECUTABLE "${PROTO_LIB_DIR}/../bin/protoc")
endif()

message("generate protobuf source for kv api, dir : " ${CMAKE_CURRENT_SOURCE_DIR}/proto/kv_api.proto)
message("binary dir：" ${CMAKE_CURRENT_BINARY_DIR})
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/proto)
set(PROTO_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto)
file(GLOB PROTO_FILES ${PROTO_SRC_DIR}/*.proto)
foreach(PROTO ${PROTO_FILES})
    get_filename_component(PROTO_WE ${PROTO} NAME_WE)
    list(APPEND PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/proto/${PROTO_WE}.pb.cc")
    execute_process(
        COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} ${PROTO_FLAGS}
        --cpp_out=${PROTO_GEN_DIR}
        --proto_path=${PROTOBUF_INCLUDE_DIR}
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto ${PROTO}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        ERROR_VARIABLE PROTO_ERROR
        RESULT_VARIABLE PROTO_RESULT
    )
    if (${PROTO_RESULT} EQUAL 0) 
    else ()
        message (FATAL_ERROR "Fail to generate cpp of ${PROTO} : ${PROTO_ERROR}")
    endif()
endforeach()

file(GLOB KVAPI_RPOTO_HEADERS ${PROTO_GEN_DIR}/*.pb.h)
file(GLOB KVAPI_RPOTO_SOURCES ${PROTO_GEN_DIR}/*.pb.cc)

# include PROTO_HEADER
include_directories(${PROTOBUF_INCLUDE_DIR})
include_directories(${PROTO_GEN_DIR})
#include source dir
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
if(EXAMPLE_LINK_SO)
    find_library(BRPC_LIB NAMES brpc)
    find_library(BRAFT_LIB NAMES braft)
    message("动态库")
else()
    find_library(BRPC_LIB NAMES libbrpc.a brpc)
    find_library(BRAFT_LIB NAMES libbraft.a braft)
endif()

if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()
include_directories(${BRPC_INCLUDE_PATH})

find_path(BRAFT_INCLUDE_PATH NAMES braft/raft.h)
if ((NOT BRAFT_INCLUDE_PATH) OR (NOT BRAFT_LIB))
    message (FATAL_ERROR "Fail to find braft")
endif()
include_directories(${BRAFT_INCLUDE_PATH})

find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()

find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
include_directories(${GFLAGS_INCLUDE_PATH})

execute_process(
    COMMAND bash -c "grep \"namespace [_A-Za-z0-9]\\+ {\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $2}' | tr -d '\n'"
    OUTPUT_VARIABLE GFLAGS_NS
)
if(${GFLAGS_NS} STREQUAL "GFLAGS_NAMESPACE")
    execute_process(
        COMMAND bash -c "grep \"#define GFLAGS_NAMESPACE [_A-Za-z0-9]\\+\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $3}' | tr -d '\n'"
        OUTPUT_VARIABLE GFLAGS_NS
    )
endif()

find_path(GPERFTOOLS_INCLUDE_DIR NAMES gperftools/heap-profiler.h)
find_library(GPERFTOOLS_LIBRARIES NAMES tcmalloc_and_profiler)
if (LINK_TCMALLOC)
    #find_path(GPERFTOOLS_INCLUDE_DIR NAMES gperftools/heap-profiler.h)
    #find_library(GPERFTOOLS_LIBRARIES NAMES tcmalloc_and_profiler)
    if (GPERFTOOLS_INCLUDE_DIR AND GPERFTOOLS_LIBRARIES)
        set(CMAKE_CXX_FLAGS "-DBRPC_ENABLE_CPU_PROFILER")
        include_directories(${GPERFTOOLS_INCLUDE_DIR})
    else ()
        set (GPERFTOOLS_LIBRARIES "")
    endif ()
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CPP_FLAGS} -DGFLAGS_NS=${GFLAGS_NS} -DNDEBUG -O2 -D__const__=__unused__ -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # require at least gcc 4.8
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC is too old, please install a newer version supporting C++11")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # require at least clang 3.3
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang is too old, please install a newer version supporting C++11")
    endif()
else()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang and GCC.")
endif()

if(CMAKE_VERSION VERSION_LESS "3.1.3")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
    endif()
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
    endif()
else()
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()
message("building rocksdb...")
include(BuildRocksDB)
build_rocksdb()

get_target_property(rocksdb_include_dir RocksDB::RocksDB INTERFACE_INCLUDE_DIRECTORIES)
message("rocksdb dir x: " ${rocksdb_include_dir})
include_directories(${rocksdb_include_dir})
add_subdirectory(server)
add_subdirectory(client)
add_subdirectory(tests)
add_subdirectory(tools)
