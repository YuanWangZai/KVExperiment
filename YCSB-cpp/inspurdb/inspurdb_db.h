//
//  leveldb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_LEVELDB_DB_H_
#define YCSB_C_LEVELDB_DB_H_

#include <future>
#include <iostream>
#include <string>
#include <mutex>

#include "core/db.h"
#include "utils/properties.h"

#include <butil/atomicops.h>
#include <gflags/gflags.h>
// #include <butil/logging.h>
// #include <butil/comlog_sink.h>
#include <bthread/bthread.h>
#include <butil/file_util.h>
#include <butil/files/file_enumerator.h>
// #include <bthread/bthread_unstable.h>
#include "rocksdb/db.h"
#include "rocksdb/utilities/checkpoint.h"
#include <braft/storage.h>
#include <braft/util.h>
#include <brpc/controller.h>
#include <brpc/server.h>
// #include "state_machine.h"
#include "../replica/state_machine.h"
#include "braft/cli_service.h"
#include "kv_api.pb.h"
#include "raft_cli.pb.h"
#include <glog/logging.h>
#include <target/target_mgr.h>
// cpu alloc
#include <base/hwloc_mgr.h>

#include <bthread/bthread.h>

#include <unistd.h>
#include <signal.h>


namespace ycsbc {

struct taskctx {
  std::string key;
  std::string value;
  int is_put = 0; // 读
};

static bvar::LatencyRecorder alphadb_put_latency("alphadb_put");
static bvar::LatencyRecorder alphadb_get_latency("alphadb_get");

class InspurDB : public DB {
 public:
  InspurDB() {}
  ~InspurDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result) {
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    attr.tag = inspur_kvdb::TARGET_ONLINE_BTHREAD_TAG;
    attr.task_type = 3;

    taskctx ctx{std::move(key), "", 0};
    bthread_start_on_worker(0, &tid, &attr, InspurDB::BthreadTask, &ctx);
    bthread_join(tid, NULL);

    assert (fields == nullptr);
    DeserializeRow(result, ctx.value);
    // std::cout << "value" << ctx.value << std::endl;
    assert(result.size() == static_cast<size_t>(fieldcount_));
    
    return kOK;
  }

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result){
                return Status();
  }

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values){
    std::string data;
    SerializeRow(values, data);
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    attr.tag = inspur_kvdb::TARGET_ONLINE_BTHREAD_TAG;
    attr.task_type = 3;

    taskctx ctx{std::move(key), std::move(data), 1};
    bthread_start_on_worker(0, &tid, &attr, InspurDB::BthreadTask, &ctx);
    bthread_join(tid, NULL);

    return Status();
  }

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values){
    std::string data;
    SerializeRow(values, data);
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    attr.tag = inspur_kvdb::TARGET_ONLINE_BTHREAD_TAG;
    attr.task_type = 3;

    taskctx ctx{std::move(key), std::move(data), 1};
    bthread_start_on_worker(0, &tid, &attr, InspurDB::BthreadTask, &ctx);
    bthread_join(tid, NULL);

    return Status();
  }

  Status Delete(const std::string &table, const std::string &key){
                return Status();
  }

  static void SerializeRow(const std::vector<Field> &values, std::string &data);
  static void DeserializeRow(std::vector<Field> &values, const char *p, const char *lim);
  static void DeserializeRow(std::vector<Field> &values, const std::string &data);

  static void* BthreadTask(void *args) {
    taskctx *arg = static_cast<taskctx*>(args);
    rocksdb::Status status;
    if (arg->is_put) {
      int64_t start_time = butil::cpuwide_time_us();
      status = db_->kv_put(arg->key, arg->value);
      alphadb_put_latency << (butil::cpuwide_time_us() - start_time);
    } else {
      int64_t start_time = butil::cpuwide_time_us();
      status = db_->kv_get(arg->key, arg->value);
      alphadb_get_latency << (butil::cpuwide_time_us() - start_time);
    }
    
    if (!status.ok()) {
        std::cout << "Failed, key:" << arg->key << "error:" << status.ToString() << std::endl;
    } else {
        // LOG(INFO) << "Put success, key:" <<key;
    }

    return nullptr;
  }

  static void *close_db(void* ctx){
    printf("bthread: close_db\n");
    db_->close_db();
    rocksdb::SpdkEnv_shutdown_blobfs();
    return nullptr;
  }
 private:
  void RunMonitorServer();

  int fieldcount_;
  std::string field_prefix_;

  // static leveldb::DB *db_;
  static inspur_kvdb::TargetDBInst *db_;
  static int ref_cnt_;
  static std::mutex mu_;

  std::promise<void> init_promise_;  // 用于线程间通信
  std::future<void> init_future_;   // 用于主线程等待
};


DB *NewLeveldbDB();

} // ycsbc

#endif // YCSB_C_LEVELDB_DB_H_

