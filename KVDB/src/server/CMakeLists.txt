include(CheckCXXCompilerFlag)
include(CheckCCompilerFlag)
include(BuildRocksDB)
include_directories(${rocksdb_INCLUDE_DIR}/include)
message("proto source : " ${KVAPI_RPOTO_SOURCES})
add_executable(kvdb_server kv_server.cpp ${KVAPI_RPOTO_SOURCES} ${KVAPI_RPOTO_HEADERS})
set(ROCKSDB_INCLUDE_DIR "/home/icfs/db/kvdb_backup_2/third-party/rocksdb/include")
# target_include_directories(kvdb_server PRIVATE
#     $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
target_include_directories(kvdb_server PRIVATE ${ROCKSDB_INCLUDE_DIR})

set(DYNAMIC_LIB
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${GPERFTOOLS_LIBRARIES}
    ${LEVELDB_LIB}
    ${BRAFT_LIB}
    ${BRPC_LIB}
    rt
    ssl
    crypto
    dl
    z
    )
# get_target_property(rocksdb_lib_location RocksDB::RocksDB IMPORTED_LOCATION)
# message("RocksDB Library Location: ${rocksdb_lib_location}")
# 设置RocksDB的头文件路径


# 设置RocksDB的库文件路径
set(ROCKSDB_LIBRARY_DIR "/home/icfs/db/kvdb_backup_2/third-party/rocksdb/output")
find_library(ROCKSDB_LIB NAMES rocksdb PATHS ${ROCKSDB_LIBRARY_DIR} NO_DEFAULT_PATH)
target_link_libraries(kvdb_server ${ROCKSDB_LIB} ${DYNAMIC_LIB} #${rocksdb_lib_location})# ${ALLOCATOR})
                     #$<TARGET_PROPERTY:RocksDB::RocksDB,IMPORTED_LOCATION>)
                    #  RocksDB::RocksDB
                     glog
                     pthread)

target_compile_options(kvdb_server PRIVATE -DBRPC_ENABLE_CPU_PROFILER)
