include(CheckCXXCompilerFlag)
include(CheckCCompilerFlag)
include_directories(${rocksdb_INCLUDE_DIR}/include)
message("proto source : " ${KVAPI_RPOTO_SOURCES})
add_executable(kvdb_client kv_client.cpp ${KVAPI_RPOTO_SOURCES} ${KVAPI_RPOTO_HEADERS})
target_include_directories(kvdb_client PRIVATE
    $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)

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
message("all libs " ${DYNAMIC_LIB})
#get_target_property(rocksdb_lib_location RocksDB::RocksDB IMPORTED_LOCATION)
target_link_libraries(kvdb_client ${DYNAMIC_LIB} glog)