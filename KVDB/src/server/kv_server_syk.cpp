// libraft - Quorum-based replication of states accross machines.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (wangyao02@baidu.com)
//         YangWu(yangwu@baidu.com)
// Date: 2017/02/21 13:45:46

#include <butil/atomicops.h>
#include <gflags/gflags.h>
//#include <butil/logging.h>
//#include <butil/comlog_sink.h>
#include <butil/file_util.h>
#include <butil/files/file_enumerator.h>
#include <bthread/bthread.h>
//#include <bthread/bthread_unstable.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <braft/util.h>
#include <braft/storage.h>
#include "rocksdb/db.h"
#include "rocksdb/utilities/checkpoint.h"
//#include "state_machine.h"
#include "braft/cli_service.h"
#include "kv_api.pb.h"
#include "raft_cli.pb.h"
#include "../replica/state_machine.h"
#include <glog/logging.h>
#include "rocksdb/perf_context.h"
#include "rocksdb/statistics.h"
#include "rocksdb/iostats_context.h"
#include <fstream>

DEFINE_string(ip_and_port, "127.0.0.1:8000", "server listen address");
DEFINE_string(name, "test", "Name of the braft group");
DEFINE_string(peers, "", "cluster peer set");
DEFINE_int32(snapshot_interval, 10, "Interval between each snapshot");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election after no message received from leader in such time");
DEFINE_string(group, "KVDB", "Id of the replication group");
DEFINE_int32(compaction, 1, "auto compaction yes or not");
DEFINE_int32(db_by_raft, 0, "db io by raft");
DEFINE_int32(rpc_by_db, 1, "rpc by db yes or not");
DEFINE_int32(db_local_io, 0, "rocksdb local io");
DEFINE_int32(delete_db, 0, "delete db when start");
DEFINE_string(server_op, "", "help");

DEFINE_int32(io_type, 0, "0 is seq, 1 is random");
DEFINE_int32(thread_num, 256, "thread num");
DEFINE_int32(no_client, 0, "no client");
DEFINE_int32(disable_wal, 0, "disable wal");
DEFINE_string(key_suffix, "202407170000", "key suffix");
DEFINE_string(db_op, "", "get/put");

//DEFINE_string(log_dir, "./log", "log dir");
///DEFINE_bool(logtostderr, false, "log to std err");
//DEFINE_int32(mainloglevel, google::INFO, "log level default int");

#define APPW_RANDOM_KEY_FILE "/mnt/nvme0/append_write/key_shuf"
#define APPW_SEQ_KEY_FILE "/mnt/nvme0/append_write/key"
//1k length
#define APPW_VALUE "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705" \
        "p_id: 157258, l_off: [0, 8192, 0], p_off: 827392, seq 0, type: 8705"


namespace inspur_kvdb {

static bvar::LatencyRecorder g_rocksdb_put_latency("rocksdb_put");
static bvar::LatencyRecorder g_raft_put_latency("raft_put");
static bvar::LatencyRecorder g_rocksdb_get_latency("rocksdb_get");

class DbServiceImpl : public DbService {
public:
    explicit DbServiceImpl(RaftStateMahine* _sm) : _state_machine(_sm), _db(NULL), last_statis(0) {}
    DbServiceImpl(): _state_machine(NULL), _db(NULL), last_statis(0) {
        last_statis = butil::cpuwide_time_us();
    }

    ~DbServiceImpl() {
        if (_state_machine != NULL) {
            delete _state_machine;
            _state_machine = NULL;
        }
        if (_db != NULL) {
            delete _db;
	        _db = NULL;
	    }
    }

    // rpc method
    virtual void get(::google::protobuf::RpcController* controller,
                       const GetRequest* request,
                       GetResponse* response,
                       ::google::protobuf::Closure* done) {
        //LOG(INFO) << "get start";
        brpc::ClosureGuard done_guard(done);
        if (1 == FLAGS_db_by_raft) {
            _state_machine->get(request->key(), response, controller);
        } else {
            brpc::Controller* cntl = (brpc::Controller*)controller;
            int64_t now = 0;
            int64_t delta_time_us = 0;
            now = butil::cpuwide_time_us();

            std::string value;
            if (1 == FLAGS_rpc_by_db) {
                rocksdb::Status status = _db->Get(rocksdb::ReadOptions(), request->key(), &value);
                if (status.ok()) {
                    //LOG(INFO) << "this is leader, get request, key:" << key << " value:" << value;
                    response->set_value(value);
                } else {
                    LOG(WARNING) << "get failed, key:(" << request->key() << ")";
                    cntl->SetFailed(brpc::EREQUEST, status.ToString().c_str());
                }
            } else {
                value = "hello world not from db";
                response->set_value(value);
            }

            delta_time_us = butil::cpuwide_time_us() - now;
            g_rocksdb_get_latency << delta_time_us;
        }
        //LOG(INFO) << "get end";
    }

    virtual void put(::google::protobuf::RpcController* controller,
                       const PutRequest* request,
                       PutResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = (brpc::Controller*)controller;

        if (1 == FLAGS_db_by_raft) {
            int64_t start_time = butil::cpuwide_time_us();

            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper(&data);
            if (!request->SerializeToZeroCopyStream(&wrapper)) {
                cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                return;
            }

            DbClosure* c = new DbClosure(cntl, response, done_guard.release());
            _state_machine->apply(&data, c);

            g_raft_put_latency << (butil::cpuwide_time_us() - start_time);
        } else {
            int64_t delta_time_us = 0;
            int64_t start_time = butil::cpuwide_time_us();

            if (1 == FLAGS_rpc_by_db) {
                rocksdb::WriteOptions options;
		if (0 == FLAGS_disable_wal) {
                    options.disableWAL = false;
		} else {
                    options.disableWAL = true;
		}
                rocksdb::Status status = _db->Put(options, request->key(), request->value());
                if (!status.ok()) {
                    LOG(WARNING) << "Put failed, key:" << request->key() << " value.size:" << request->value().size();
                    cntl->SetFailed(brpc::EREQUEST, status.ToString().c_str());
                } else {
                    //LOG(INFO) << "Put success, key:" << request->key() << " value.size:" << request->value().size();
                }
            }

            int64_t now = butil::cpuwide_time_us();
            delta_time_us = now - start_time;
            g_rocksdb_put_latency << delta_time_us;

            if (1 == FLAGS_rpc_by_db && (now - last_statis) >= 5000000) {
                last_statis = now;
                std::string stats;
                if (_db->GetProperty("rocksdb.stats", &stats)) {
                    LOG(INFO) << "***********************************rocksdb stats*********************************************" << stats;
                }
            }
        }
    }

    int init_rocksdb() {
        if (_db != NULL) {
            //LOG(INFO) << "rocksdb already opened";
            std::cout << "rocksdb already opened" << std::endl;
            return 0;
        }

        std::string db_path = "./data/rocksdb_data";
        if (!butil::CreateDirectory(butil::FilePath(db_path))) {
            //LOG(WARNING) << "CreateDirectory " << db_path << " failed";
            std::cout << "CreateDirectory " << db_path << " failed" << std::endl;
            return -1;
        }

        rocksdb::Options options;
        /*options.write_buffer_size = 1024*1048576;
        options.max_write_buffer_number = 128;
        options.min_write_buffer_number_to_merge = 128;
        options.num_levels = 3;
        options.max_background_flushes = 256;
        options.max_background_compactions = 64;
        */
        if (0 == FLAGS_compaction) {
            std::cout << "disable auto compaction" << std::endl;
            options.disable_auto_compactions = true;
        } else {
            std::cout << "enable auto compaction" << std::endl;
        }
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, db_path, &_db);
        if (!status.ok()) {
            //LOG(WARNING) << "open rocksdb " << db_path << " failed, msg: " << status.ToString();
	    std::cout << "open rocksdb " << db_path << " failed, msg: " << status.ToString() << std::endl;
            return -1;
        }

        //LOG(INFO) << "rocksdb open success!";
        std::cout << "rocksdb open success!" << std::endl;
        return 0;
    }

    void set_sm(RaftStateMahine *_sm) { _state_machine = _sm; }
private:
    RaftStateMahine* _state_machine;
    rocksdb::DB* _db;
    int64_t last_statis;
};

class CliServiceImpl : public RaftCliService {
public:
    //explicit CliServiceImpl(RaftStateMahine* state_machine) : _state_machine(state_machine) {}
    CliServiceImpl() : _state_machine(NULL) {}
    ~CliServiceImpl() {
        delete _state_machine;
	_state_machine = NULL;
    }


    virtual void leader(::google::protobuf::RpcController* cntl,
                        const GetLeaderRequest* request,
                        GetLeaderResponse* response,
                        ::google::protobuf::Closure* done) {
            
    }

    void set_sm(RaftStateMahine *_sm) { _state_machine = _sm; }
private:
    RaftStateMahine* _state_machine;
};

static rocksdb::DB* local_db = NULL;
static std::string key_file;
static std::string put_value = APPW_VALUE;
static std::vector<pthread_t> get_threads;
static std::vector<pthread_t> put_threads;
//static std::vector<bthread_t> get_threads;
//static std::vector<bthread_t> put_threads;
int64_t last_localdb_statis = 0;

struct thread_params {
  int32_t thread_num;
  thread_params(): thread_num(0) {}
};

//format: key_FLAGS_key_suffix_threadnum, eg:key_202407171030_0
void get_key(std::string &key, int32_t thread_num)
{
    key += "_";
    key += FLAGS_key_suffix;
    key += "_";
    key += std::to_string(thread_num);
    //LOG(INFO) << "key:(" << key << ")";
    //std::cout << "key:(" << key << ")" << std::endl;
}

void *put_func(void *args)
{
    /*rocksdb::get_perf_context()->EnablePerLevelPerfContext();
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTime);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_iostats_context()->Reset();*/
    
    pthread_t tid = pthread_self();
    struct thread_params *thread_args = (thread_params*)(args);
    int32_t thread_num = thread_args->thread_num;
    std::ifstream skey(key_file);
    std::string key;
    
    int num = 0;
    if (skey.is_open()) {
        while (getline(skey, key)) {
    	    ++num;
    	    get_key(key, thread_num);

    	    rocksdb::WriteOptions options;
	    if (0 == FLAGS_disable_wal) {
                options.disableWAL = false;
	    } else {
                options.disableWAL = true;
	    }

            int64_t start_time = butil::cpuwide_time_us();
            rocksdb::Status status = local_db->Put(options, key, put_value);
	    int64_t now = butil::cpuwide_time_us();
            g_rocksdb_put_latency << (now - start_time);

    	    if (!status.ok()) {
    	        LOG(WARNING) << "Put failed, key:(" << key << ")";
    	    } else {
                //LOG(INFO) << "Put success, key:(" << key << ")";
    	    }

	    if (now - last_localdb_statis >= 5000000) {
                last_localdb_statis = now;
                std::string stats;
                if (local_db->GetProperty("rocksdb.stats", &stats)) {
                    LOG(INFO) << "***********************************rocksdb stats*********************************************" << stats;
                }
            }
        }
        skey.close();
    } else {
        std::cout << " --" << tid << ": open failed, keyfile:" << key_file << std::endl;
    }
    
    std::cout << " --" << tid << ", thread_num:" << thread_num << " keyfile:" << key_file << ", put num:" << num << std::endl;
    
    //std::cout << "***************rocksdb perf context******************\n" << rocksdb::get_perf_context()->ToString(true) << std::endl;
    //std::cout << "***************rocksdb iostats context******************\n" << rocksdb::get_iostats_context()->ToString(true) << std::endl;
    
    delete thread_args;
    thread_args = NULL;

    return nullptr;
}

void *get_func(void *args)
{
    /*rocksdb::get_perf_context()->EnablePerLevelPerfContext();
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTime);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_iostats_context()->Reset();*/
    
    pthread_t tid = pthread_self();
    struct thread_params *thread_args = (thread_params*)(args);
    int32_t thread_num = thread_args->thread_num;
    std::ifstream skey(key_file);
    std::string key;
    
    int num = 0;
    if (skey.is_open()) {
        while (getline(skey, key)) {
    	    ++num;
    	    get_key(key, thread_num);

            std::string value;

            int64_t start_time = butil::cpuwide_time_us();
            rocksdb::Status status = local_db->Get(rocksdb::ReadOptions(), key, &value);
            g_rocksdb_get_latency << (butil::cpuwide_time_us() - start_time);

    	    if (status.ok()) {
                //LOG(INFO) << "get success, key:(" << key << "), value:(" << value << ")";
    	    } else {
    	        LOG(WARNING) << "get failed, key:(" << key << ")";
    	    }
        }
        skey.close();
    } else {
        std::cout << " --" << tid << ", thread_num:" << thread_num << ", open failed, keyfile:" << key_file << std::endl;
    }
    
    std::cout << " --" << tid << ", thread_num:" << thread_num << ", keyfile:" << key_file << ", get num:" << num << std::endl;
    
    //std::cout << "***************rocksdb perf context******************\n" << rocksdb::get_perf_context()->ToString(true) << std::endl;
    //std::cout << "***************rocksdb iostats context******************\n" << rocksdb::get_iostats_context()->ToString(true) << std::endl;
    
    delete thread_args;
    thread_args = NULL;

    return nullptr;
}

void test_get()
{
    std::cout << " create get bthread:" << FLAGS_thread_num << std::endl;
    for (int32_t i=0; i<FLAGS_thread_num; ++i) {
	struct thread_params *args = new thread_params();
        args->thread_num = i;

        /*bthread_t thr;
        bthread_start_background(&thr, NULL, get_func, &thread_num);
        get_threads.push_back(thr);
	*/
        pthread_t ptid;
        int res = pthread_create(&ptid, NULL, get_func, args);
        if (res != 0) {
    	    std::cout << " create get thread failed" << std::endl;
    	    continue;
        }
        get_threads.push_back(ptid);
    }
}

void test_put()
{
    std::cout << " create put bthread:" << FLAGS_thread_num << std::endl;
    for (int32_t i=0; i<FLAGS_thread_num; ++i) {
	struct thread_params *args = new thread_params();
        args->thread_num = i;
        /*bthread_t thr;
        bthread_start_background(&thr, NULL, put_func, &thread_num);
        put_threads.push_back(thr);
	*/
        pthread_t ptid;
        int res = pthread_create(&ptid, NULL, put_func, args);
        if (res != 0) {
    	    std::cout << " create put thread failed" << std::endl;
    	    continue;
        }
        put_threads.push_back(ptid);
    }
}

static int init_local_db()
{
    std::cout << " no client, rocksdb local io" << std::endl;
    if (local_db != NULL) {
        return 0;
    }
    
    const std::string db_path = "./data/rocksdb_data";
    if (1 == FLAGS_delete_db) {
        if (!butil::DeleteFile(butil::FilePath(db_path), true)) {
            LOG(WARNING) << "rm " << db_path << " failed";
            std::cout << "delete db data failed" << std::endl;
            return -1;
        } else {
            std::cout << "delete db data ok" << std::endl;
        }
    } else {
        std::cout << "not delete db" << std::endl;
    }

    if (!butil::CreateDirectory(butil::FilePath(db_path))) {
        std::cout << "CreateDirectory " << db_path << " failed" << std::endl;
        return -1;
    }
    rocksdb::Options db_option;
    db_option.create_if_missing = true;
    //db_option.write_buffer_size = *1024*1024; //64MB
    db_option.max_write_buffer_number = 64; //2
    db_option.min_write_buffer_number_to_merge = 8;//1

    db_option.max_background_jobs = 64;//2
    db_option.max_background_compactions = 32;//-1
    db_option.max_background_flushes = 32; //-1

    db_option.num_levels = 4; //7
    db_option.target_file_size_base = 512*1024*1024;//64MB
    //db_option.max_bytes_for_level_base = 1024*1024*1024;//256MB
    db_option.level_compaction_dynamic_level_bytes = 0; //1
    db_option.target_file_size_multiplier = 10; //1
    db_option.level0_slowdown_writes_trigger = 60; //20
    db_option.level0_stop_writes_trigger = 80; //36
    db_option.level0_file_num_compaction_trigger = 20; //4
    db_option.soft_pending_compaction_bytes_limit = 4*68719476736;//64GB
    db_option.hard_pending_compaction_bytes_limit = 4*274877906944;//256GB
    if (0 == FLAGS_compaction) {
        std::cout << "disable auto compaction" << std::endl;
        db_option.disable_auto_compactions = true;
    } else {
        std::cout << "enable auto compaction" << std::endl;
        db_option.disable_auto_compactions = false;
    }
    //db_option.statistics = rocksdb::CreateDBStatistics();
    
    rocksdb::Status status = rocksdb::DB::Open(db_option, db_path, &local_db);
    if (!status.ok()) {
        std::cout << "open db error " << status.ToString() << std::endl;
        return -1;
    }
    std::cout << "open db success " << std::endl;

    return 0;
}

void wait_thread_finish() {
    for (std::vector<pthread_t>::iterator git=get_threads.begin(); git!=get_threads.end(); ++git) {
        std::cout << " wait get thread finish:" << *git << std::endl;
        pthread_join(*git, NULL);
    }
    
    for (std::vector<pthread_t>::iterator pit=put_threads.begin(); pit!=put_threads.end(); ++pit) {
        std::cout << " wait put thread finish:" << *pit << std::endl;
        pthread_join(*pit, NULL);
    }

    /*for (std::vector<bthread_t>::iterator git=get_threads.begin(); git!=get_threads.end(); ++git) {
        std::cout << " wait get thread finish" << std::endl;
	bthread_join(*git, nullptr);
    }
    
    for (std::vector<bthread_t>::iterator pit=put_threads.begin(); pit!=put_threads.end(); ++pit) {
        std::cout << " wait put thread finish" << std::endl;
	bthread_join(*pit, nullptr);
    }*/
} 
}  // namespace inspur_kvdb

int main(int argc, char* argv[]) {
    std::cout << "[Usage]"<< std::endl;
    std::cout << "./kvdb_server --ip_and_port=127.0.0.1:8000 -delete_db=[0|1] --disable_wal=[0|1] --db_local_io=[0|1] --db_by_raft=[0|1] --peers=127.0.0.1:8000 --rpc_by_db=[0|1] --compaction=[0|1] --bthread_concurrency=64"<< std::endl;
    std::cout << "./kvdb_server --no_client=1 --io_type=[0|1] --delete_db=[0|1] --disable_wal=[0|1] --compaction=[0|1] --db_op=[get|put]  --key_suffix='202407310000' --thread_num=256"<< std::endl;

    google::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_log_dir = "./log";
    FLAGS_logtostderr = 0;

    if (1 == FLAGS_no_client) {
	if (0 == FLAGS_io_type) {
            std::cout << " sequential" << std::endl;
            inspur_kvdb::key_file = APPW_SEQ_KEY_FILE;
        } else {
            std::cout << " random" << std::endl;
            inspur_kvdb::key_file = APPW_RANDOM_KEY_FILE;
        }

        inspur_kvdb::init_local_db();

	auto time_start = std::chrono::steady_clock::now();

        if (0 == FLAGS_db_op.compare("get")) {
            inspur_kvdb::test_get();
        } else if (0 == FLAGS_db_op.compare("put")) {
            inspur_kvdb::test_put();
        } else {
            std::cout << " not supported op:" << FLAGS_db_op << std::endl;
            return -1;
        }

        brpc::Server monitor_server;
        monitor_server.Start(6059, nullptr);

        inspur_kvdb::wait_thread_finish();

	auto time_end = std::chrono::steady_clock::now();
        std::cout << "***************total time cost:" << std::chrono::duration_cast<std::chrono::nanoseconds>((time_end - time_start)).count() << std::endl;
        //std::cout << "*****************************db stats*************************\n" << stat->ToString() << std::endl;
        std::string stats;
        if (inspur_kvdb::local_db != NULL && inspur_kvdb::local_db->GetProperty("rocksdb.stats", &stats)) {
		std::cout << "***********************************rocksdb stats*********************************************" << stats << std::endl;
        }

        LOG(INFO) << "Wait until server stopped";
        std::cout << " Wait until server stopped" << std::endl;
        monitor_server.RunUntilAskedToQuit();
        LOG(INFO) << "DbServer is going to quit";
        std::cout << " DbServer is going to quit" << std::endl;
        google::ShutdownGoogleLogging();
        return 0;
    }

    // [ Setup from ComlogSinkOptions ]
    /*logging::ComlogSinkOptions options;
    //options.async = true;
    options.process_name = "db_server";
    options.print_vlog_as_warning = false;
    options.split_type = logging::COMLOG_SPLIT_SIZECUT;
    if (logging::ComlogSink::GetInstance()->Setup(&options) != 0) {
        LOG(ERROR) << "Fail to setup comlog";
        return -1;
    }
    logging::SetLogSink(logging::ComlogSink::GetInstance());
*/
    // add service
    //assert(logging::InitLogging(log_opt) == true);
    if (0 == FLAGS_rpc_by_db) {
        std::cout << "rpc no db" << std::endl;
    } else {
        std::cout << "rpc by db" << std::endl;
    }

    brpc::Server server;
    inspur_kvdb::DbServiceImpl service;
    inspur_kvdb::CliServiceImpl cli_service_impl;
    if (1 == FLAGS_db_by_raft) {
        std::cout << " rocksdb by raft" << std::endl;
        if (braft::add_service(&server, FLAGS_ip_and_port.c_str()) != 0) {
            LOG(FATAL) << "Fail to init braft";
            return -1;
        }

        // init peers
        std::vector<braft::PeerId> peers;
        const char* the_string_to_split = FLAGS_peers.c_str();
        for (butil::StringSplitter s(the_string_to_split, ','); s; ++s) {
            braft::PeerId peer(std::string(s.field(), s.length()));
            peers.push_back(peer);
            LOG(INFO) << "add peer " << peer;
        }

        butil::EndPoint addr;
        butil::str2endpoint(FLAGS_ip_and_port.c_str(), &addr);
        if (butil::IP_ANY == addr.ip) {
            addr.ip = butil::my_ip();
        }

        inspur_kvdb::RaftStateMahine* state_machine =
            new inspur_kvdb::RaftStateMahine(FLAGS_name, braft::PeerId(addr, 0));
        braft::NodeOptions node_options;
        node_options.election_timeout_ms = FLAGS_election_timeout_ms;
        node_options.fsm = state_machine;
        node_options.initial_conf = braft::Configuration(peers); // bootstrap need
        node_options.snapshot_interval_s = FLAGS_snapshot_interval;
        //node_options.log_uri = "memory://data/log";
        node_options.log_uri = "local://./data/raft_log";
        node_options.raft_meta_uri = "local://./data/stable";
        node_options.snapshot_uri = "local://./data/snapshot";

        if (1 == FLAGS_delete_db) {
            std::string db_path = "./data/rocksdb_data";
            if (!butil::DeleteFile(butil::FilePath(db_path), true)) {
                LOG(WARNING) << "rm " << db_path << " failed";
                std::cout << "delete db data failed" << std::endl;
                return -1;
            } else {
                std::cout << "delete db data ok" << std::endl;
            }
        } else {
            std::cout << "not delete db" << std::endl;
	}

        // init_rocksdb MUST before Node::init, maybe single node become leader
        // and restore log but db not inited
        if (state_machine->init_rocksdb() != 0) {
            LOG(WARNING) << "init_rocksdb failed";
            std::cout << " init_rocksdb failed" << std::endl;
            return -1;
        } else {
            std::cout << " init_rocksdb ok" << std::endl;
        }

        // init will call on_snapshot_load if has snapshot,
        // rocksdb restore from braft's snapshot, then restore log
        if (0 != state_machine->init(node_options)) {
            LOG(FATAL) << "Fail to init node";
            std::cout << " Fail to init node" << std::endl;
            return -1;
        } else {
            LOG(INFO) << "init Node success";
            std::cout << " init Node success" << std::endl;
        }

        //inspur_kvdb::DbServiceImpl service(state_machine);
	service.set_sm(state_machine);
        if (0 != server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
            LOG(FATAL) << "Fail to AddService";
            std::cout << " Fail to AddService DbService" << std::endl;
            return -1;
        } else {
            std::cout << " AddService DbService ok" << std::endl;
        }

        //inspur_kvdb::CliServiceImpl cli_service_impl(state_machine);
	cli_service_impl.set_sm(state_machine);
        if (0 != server.AddService(&cli_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE)) {
            LOG(FATAL) << "Fail to AddService";
            std::cout << " Fail to AddService CliService" << std::endl;
            return -1;
        } else {
            std::cout << " AddService CliService ok" << std::endl;
        }
    } else {
        std::cout << "rocksdb no raft" << std::endl;
        if (1 == FLAGS_delete_db) {
            std::string db_path = "./data/rocksdb_data";
            if (!butil::DeleteFile(butil::FilePath(db_path), true)) {
                LOG(WARNING) << "rm " << db_path << " failed";
                std::cout << "delete db data failed" << std::endl;
                return -1;
            } else {
                std::cout << "delete db data ok" << std::endl;
            }
        } else {
            std::cout << "not delete db" << std::endl;
	}
        // init_rocksdb MUST before Node::init, maybe single node become leader
        // and restore log but db not inited
        //inspur_kvdb::DbServiceImpl service;
        if (service.init_rocksdb() != 0) {
            LOG(WARNING) << "init_rocksdb failed";
            std::cout << " init_rocksdb failed" << std::endl;
            return -1;
        } else {
            std::cout << " init_rocksdb ok" << std::endl;
        }

        if (0 != server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
            LOG(FATAL) << "Fail to AddService";
            std::cout << " Fail to AddService" << std::endl;
            return -1;
        } else {
            std::cout << " AddService ok" << std::endl;
        }
    }
    
    if (server.Start(FLAGS_ip_and_port.c_str(), NULL) != 0) {
        LOG(FATAL) << "Fail to start server";
        std::cout << " Fail to start server" << std::endl;
        return -1;
    } else {
        std::cout << " start server ok" << std::endl;
    }

    brpc::Server monitor_server;
    monitor_server.Start(6059, nullptr);

    LOG(INFO) << "Wait until server stopped";
    std::cout << " Wait until server stopped" << std::endl;
    server.RunUntilAskedToQuit();
    LOG(INFO) << "DbServer is going to quit";
    std::cout << " DbServer is going to quit" << std::endl;
    google::ShutdownGoogleLogging();
    return 0;
}
