cmake_minimum_required(VERSION 3.4)
# 定义插件名称
set(PLUGIN_NAME "blobfs")

# find_library(BRAFT_LIB NAMES braft)
# find_library(BPRC_LIB NAMES brpc)
include_directories("/usr/include" "/usr/local/include")
set(ENV{PKG_CONFIG_PATH} "/usr/local/lib/pkgconfig")
find_package(PkgConfig REQUIRED)
pkg_check_modules(SPDK_NVME REQUIRED spdk_nvme)
pkg_check_modules(SPDK_ENV_DPDK REQUIRED spdk_env_dpdk)
pkg_check_modules(SPDK_BDEV REQUIRED spdk_bdev)
pkg_check_modules(SPDK_EVENT REQUIRED spdk_event)
pkg_check_modules(SPDK_EVENT_BDEV REQUIRED spdk_event_bdev)
add_definitions("-Wall -g")

set(${PLUGIN_NAME}_SOURCES "env_spdk.cc" PARENT_SCOPE)
set(${PLUGIN_NAME}_COMPILE_FLAGS "-Wall -g" PARENT_SCOPE)
set(${PLUGIN_NAME}_INCLUDE_PATHS
    "/usr/include"
    "/usr/local/include"
    PARENT_SCOPE
)
set(${PLUGIN_NAME}_LIBS
    ${SPDK_NVME_LIBRARIES} 
    ${SPDK_ENV_DPDK_LIBRARIES} 
    ${SPDK_EVENT_LIBRARIES} 
    ${SPDK_BDEV_LIBRARIES} 
    ${SPDK_EVENT_BDEV_LIBRARIES} 
    # ${CMAKE_THREAD_LIBS_INIT}
    # ${BRAFT_LIB}
    # ${BRPC_LIB}
    "braft"            # 链接 braft 库
    "brpc"             # 链接 brpc 库
    "protobuf"         # 链接 Protobuf
    "gflags"           # 链接 gflags 库
    "rt"               # 链接实时库 (librt)
    "dl"               # 链接动态库支持 (libdl)
    "z"
    "pthread"
    "isal"
    "isal_crypto"
    "glog"
    
    PARENT_SCOPE
)
set(${PLUGIN_NAME}_LINK_PATHS
    "/usr/local/lib"

    PARENT_SCOPE
)

set(${PLUGIN_NAME}_CMAKE_EXE_LINKER_FLAGS "-Wl,--no-as-needed" PARENT_SCOPE)