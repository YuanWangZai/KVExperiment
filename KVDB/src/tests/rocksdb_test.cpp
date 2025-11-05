#include <rocksdb/db.h>
#include <butil/file_util.h>
#include <bthread/bthread.h>
#include "kv_api.pb.h"
#include <butil/time.h>
#include <thread>
#include <gflags/gflags.h>
DEFINE_int32(put_num, 10000, "put test number");
namespace inspur_kvdb {
namespace engine_test {
class LocalRocksEngine {

};

rocksdb::DB* g_rocksdb = nullptr;

static void* test_put(void*) {
    pthread_t thr_id = pthread_self();
    butil::Timer timer;
    timer.start();
    for (int i = 0; i < FLAGS_put_num; i++) {
        rocksdb::WriteOptions opt;
        PutRequest req;
        std::stringstream ss;
        ss << "test_" << thr_id << " " <<i;
        req.set_key(ss.str());
        req.set_value(ss.str() + "value");        
    }
    timer.stop();
    std::cout << "put time cost " << timer.m_elapsed() << std::endl 
                <<" avg cost in nanosec: " << timer.n_elapsed() / FLAGS_put_num << std::endl;
    return nullptr;
}
}
}

int main(int argc, char **argv) {
    const std::string db_path = "./data/rocksdb_data";
    rocksdb::Options db_option;
    rocksdb::Status status = rocksdb::DB::Open(db_option, db_path, &inspur_kvdb::engine_test::g_rocksdb);
    //std::thread test_thr      
    bthread_t thr;
    bthread_start_background(&thr, NULL, inspur_kvdb::engine_test::test_put, nullptr); 
    bthread_join(thr, nullptr);
    //inspur_kvdb::engine_test::test_put();
    return 0;
}