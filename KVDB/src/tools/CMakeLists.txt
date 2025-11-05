include(CheckCXXCompilerFlag)
include(CheckCCompilerFlag)
include(BuildRocksDB)
include_directories(${rocksdb_INCLUDE_DIR}/include)
#message("proto source : " ${KVAPI_RPOTO_SOURCES})
#add_executable(db_statis db_statis.cpp ${KVAPI_RPOTO_SOURCES} ${KVAPI_RPOTO_HEADERS})
add_executable(db_statis db_statis.cpp)
target_include_directories(db_statis PRIVATE
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
#get_target_property(rocksdb_lib_location RocksDB::RocksDB IMPORTED_LOCATION)

target_link_libraries(db_statis ${DYNAMIC_LIB} #${rocksdb_lib_location})# ${ALLOCATOR})
                     #$<TARGET_PROPERTY:RocksDB::RocksDB,IMPORTED_LOCATION>)
                     RocksDB::RocksDB
                     glog)
