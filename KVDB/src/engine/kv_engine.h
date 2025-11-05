#pragma once
#include <string>
#include <butil/iobuf.h>
#include <rocksdb/db.h>
#include <glog/logging.h>
#include <butil/file_util.h>
namespace inspur_kvdb {
struct DBConf {
    std::string db_path;
};
class KvEngine {
public:
    virtual int32_t open_db(const DBConf& conf) = 0;
    virtual int32_t kv_put(const std::string& key, const std::string& value) = 0;
    virtual int32_t kv_get(const std::string& key, std::string& value_buf) = 0;
    virtual int32_t tranx_put() = 0;
    virtual int32_t tranx_start() = 0;
    virtual int32_t tranx_pre_write() = 0;
    virtual int32_t trax_commit() = 0;
    virtual void snapshot_save() = 0;
};

class RocksdbEngine : public KvEngine {
public:
    virtual int32_t open_db(const DBConf& conf) {
        if (_db != NULL) {
            LOG(INFO) << "rocksdb already opened";
            return 0;
        }

        auto&& db_path = conf.db_path;
        if (!butil::CreateDirectory(butil::FilePath(db_path))) {
            DLOG(WARNING) << "CreateDirectory " << db_path << " failed";
            return -1;
        }

        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, db_path, &_db);
        if (!status.ok()) {
            LOG(WARNING) << "open rocksdb " << db_path << " failed, msg: " << status.ToString();
            return -1;
        }

        LOG(INFO) << "rocksdb open success!";
        return 0;
    }
    
    virtual int32_t kv_put(const std::string& key, const std::string& value) {
        return 0;
    }
    
    virtual int32_t kv_get(const std::string& key, std::string& value_buf) {
        return 0;
    }
    
    virtual int32_t tranx_put() {
        return 0;
    }
    
    virtual int32_t tranx_start() {
        return 0;
    }
    
    virtual int32_t tranx_pre_write() {
        return 0;
    }
    
    virtual int32_t trax_commit() {
        return 0;
    }
    
    virtual void snapshot_save() {

    }

private:
    rocksdb::DB* _db; 
};

static KvEngine* create_engine(const std::string& engine_name, const std::string& engine_path) {
    KvEngine* inst = nullptr;
    DBConf conf;
    conf.db_path = engine_path;
    if (engine_name == "rocksdb") {
        inst = new RocksdbEngine;
        assert(inst->open_db(conf) == 0); 
    }
    return inst;
} 
}