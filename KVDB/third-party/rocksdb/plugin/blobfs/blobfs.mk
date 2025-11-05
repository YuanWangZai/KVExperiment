# 定义插件名称
PLUGIN_NAME := blobfs

# 包含的目录
$(PLUGIN_NAME)_INCLUDE_PATHS := /usr/include /usr/local/include

# 编译器标志
$(PLUGIN_NAME)_CXXFLAGS := -Wall -g `pkg-config --cflags spdk_nvme spdk_env_dpdk spdk_bdev spdk_event spdk_event_bdev`

LIBS = -lpthread -lbvar -lbutil -lbraft -lbrpc -lgflags -lglog -lprotobuf -lrt -ldl -lz
# 链接器标志
$(PLUGIN_NAME)_LDFLAGS := `pkg-config --libs -Wl,--no-as-needed spdk_nvme spdk_env_dpdk spdk_bdev spdk_event spdk_event_bdev` -L/usr/local/lib  -Wl,--as-needed -pthread -lisal -lisal_crypto -lglog $(LIBS)

# 源文件
$(PLUGIN_NAME)_SOURCES := env_spdk.cc
