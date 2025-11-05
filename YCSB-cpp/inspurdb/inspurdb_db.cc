//
//  leveldb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "inspurdb_db.h"
#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/utils.h"



namespace {
  const std::string PROP_NAME = "leveldb.dbname";
  const std::string PROP_NAME_DEFAULT = "";

} // anonymous

namespace ycsbc {

inspur_kvdb::TargetDBInst * InspurDB::db_ = nullptr;
int InspurDB::ref_cnt_ = 0;
std::mutex InspurDB::mu_;

void InspurDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);
  const utils::Properties &props = *props_;
  if (db_) {
    ref_cnt_++;
    return;
  }

  std::thread monitor_thread(&InspurDB::RunMonitorServer, this);
  monitor_thread.detach();

  init_future_ = init_promise_.get_future();
  init_future_.wait();

  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));
  std::cout << "Initialization complete." << std::endl;
}

void InspurDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
  std::cout << "OUT\n"; 
  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  attr.tag = inspur_kvdb::TARGET_ONLINE_BTHREAD_TAG;
  attr.task_type = inspur_kvdb::TARGET_TASK_TYPE_DEFAULT;
  bthread_start_on_worker(0, &tid, &attr, close_db, nullptr);
  bthread_join(tid, NULL);
  delete db_;
}

void InspurDB::RunMonitorServer() {
    
    std::unique_ptr<inspur_kvdb::TargetGroup> target_group = std::make_unique<inspur_kvdb::TargetGroup>();
    if (target_group->init() != 0) {
        LOG(FATAL) << "init target group failed.";
        return;
    }

    ref_cnt_++;
    db_ = target_group->_target_list.find("test_target_1")->second->_target_db_inst.get();

    init_promise_.set_value();

    brpc::Server monitor_server;
    brpc::ServerOptions opt;
    opt.num_threads = 4;
    opt.bthread_tag = 0;

    if (monitor_server.Start(6059, &opt) != 0) {
        LOG(FATAL) << "Failed to start monitor server.";
        return;
    }

    std::cout << "Monitor server started on port 6059." << std::endl;

    LOG(INFO) << "Wait until server stopped.";
    monitor_server.RunUntilAskedToQuit();

    LOG(INFO) << "DbServer is going to quit.";
    google::ShutdownGoogleLogging();
}

void InspurDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    values.push_back({field, value});
  }
}

void InspurDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = field.value.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void InspurDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRow(values, p, lim);
}



DB *NewInspurDB() {
  return new InspurDB;
}

const bool registered = DBFactory::RegisterDB("inspurdb", NewInspurDB);

} // ycsbc
