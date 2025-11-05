#include <butil/atomicops.h>
#include <gflags/gflags.h>
#include <butil/file_util.h>
#include <butil/files/file_enumerator.h>
#include <bthread/bthread.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <braft/util.h>
#include <braft/storage.h>
#include "rocksdb/db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "braft/cli_service.h"
#include "kv_api.pb.h"
#include "raft_cli.pb.h"
#include <glog/logging.h>

//you should put db_statis tool in kvdb_server directory
int main(int argc, char* argv[]) {
    rocksdb::DB* db;
    std::string db_path = "./data/rocksdb_data";
    if (!butil::CreateDirectory(butil::FilePath(db_path))) {
        std::cout << "CreateDirectory " << db_path << " failed" << std::endl;
        return -1;
    }
    
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
    if (!status.ok()) {
        std::cout << "open rocksdb " << db_path << " failed, msg: " << status.ToString() << std::endl;
        return -1;
    }
    
    std::cout << "rocksdb open success!" << std::endl;
    
    std::string statis;
    if (db->GetProperty("rocksdb.stats", &statis)) {
        std::cout << "*****************************************rocksdb statis******************************************************"<< std::endl;
        std::cout << statis << std::endl;
    }

    return 0;
}
