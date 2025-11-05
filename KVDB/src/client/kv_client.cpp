#include <gflags/gflags.h>
#include <butil/string_splitter.h>
#include <brpc/channel.h>
#include "kv_api.pb.h"
#include "raft_cli.pb.h"
#include "braft/raft.h"
#include "braft/util.h"
#include <glog/logging.h>
#include <sys/types.h>
#include <fcntl.h>
#include <dirent.h>
#include <fstream>

DEFINE_int32(timeout_ms, 3000, "Timeout for each request");
DEFINE_string(cluster_ns, "", "Name service for the cluster, list://demo.com:1234,1.2.3.4:1234");
DEFINE_string(peer, "", "add/remove peer");
DEFINE_int32(log_level, 1, "min log level");
DEFINE_string(db_op, "get", "db op: get/put/put_files/put_appw");
DEFINE_string(key, "test", "get key or put prefix key");
DEFINE_string(value, "test", "put value");
DEFINE_int32(put_num, 1000000, "put value");
DEFINE_int32(thread_num, 256, "thread num");
DEFINE_int32(value_len, 0, "put value length, key or 1kvalue");
DEFINE_int32(appw_key_type, 0, "0 is seq, 1 is random");
DEFINE_string(key_suffix, "202407170000", "key suffix");


//DEFINE_string(log_dir, "./log", "log dir");
//DEFINE_bool(logtostderr, false, "log to std err");
//DEFINE_int32(mainloglevel, google::INFO, "log level default int");

#define FILE_METADATA_DIR "./file_metadata"
#define APPW_RANDOM_KEY_FILE "./append_write/key_shuf"
#define APPW_SEQ_KEY_FILE "./append_write/key"

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

class DbClient {
public:
    DbClient(const std::vector<braft::PeerId>& peers) : _peers(peers) {
        reset_leader();
        int ret = _cluster.Init(FLAGS_cluster_ns.c_str(), nullptr);
        CHECK_EQ(0, ret) << "cluster channel init failed, cluster: " << FLAGS_cluster_ns;
    }

    ~DbClient() {}

    void get(const std::string &key, std::string& value) {
        CHECK(!key.empty());
        //while (true) {
            get_leader();

            // get request
            DbService_Stub stub(&_channel);
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);

            GetRequest request;
            request.set_key(key);
            GetResponse response;

            stub.get(&cntl, &request, &response, nullptr);
            if (!cntl.Failed()) {
                value = response.value();
                //LOG(INFO) << "get success, key:(" << key << "),value:(" << response.value() << ")";
                //break;
            } else {
                LOG(WARNING) << "rpc error, key: " << key
                    << " err_code:" << cntl.ErrorCode()
                    << ", err_msg:" << cntl.ErrorText();
                //reset_leader();
                //get_leader();
            }
        //}
    }

    void put(const std::string& key, const std::string& value) {
        CHECK(!key.empty());
        while (true) {
            get_leader();

            DbService_Stub stub(&_channel);
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);


	    //std::cout << " key:(" << key << "),value.len:" << value.size() << std::endl;
            PutRequest request;
            PutResponse response;
            request.set_key(key);
	    if (0 == FLAGS_value_len) {
                request.set_value(key);
	    } else {
                request.set_value(value);
	    }

            stub.put(&cntl, &request, &response, nullptr);
            if (!cntl.Failed()) {
                //LOG(INFO) << "put success, key: " << request.key() << " value: " << request.value();
                break;
            } else {
                LOG(WARNING) << "put failed, key: " << request.key()
                    << ", err_code:" << cntl.ErrorCode()
                    << ", err_msg:" << cntl.ErrorText();
                // 尝试重新获取leader 
                reset_leader();
                get_leader();
            }
        }
    }

private:
    void reset_leader() {
        std::lock_guard<bthread::Mutex> lock_guard(_mutex);
        //_leader.ip = "127.0.0.1";//butil::IP_ANY;
        //_leader.port = 8000;
        butil::str2endpoint("127.0.0.1:8000", &_leader);
        if (_channel.Init(_leader, nullptr) != 0) {
            LOG(WARNING) << "leader_channel init failed.";
            assert(0);
        }
    }

    int get_leader() {
        {
            std::lock_guard<bthread::Mutex> lock_guard(_mutex);
            if (_leader.ip != butil::IP_ANY && _leader.port != 0) {
                return 0;
            }
        }

        GetLeaderRequest request;
        GetLeaderResponse response;
        do {
            RaftCliService_Stub stub(&_cluster);
            brpc::Controller cntl;
            cntl.set_timeout_ms(FLAGS_timeout_ms);
            stub.leader(&cntl, &request, &response, nullptr);
            if (cntl.Failed() || !response.success()) {
                if (cntl.Failed()) {
                    LOG(WARNING) << "cntl failed, msg:" << cntl.ErrorText();
                }
                if (!response.success()) {
                    LOG(WARNING) << "not success";
                }
                continue;
            }
            {
                std::lock_guard<bthread::Mutex> lock_guard(_mutex);
                butil::str2endpoint(response.leader_addr().c_str(), &_leader);
            }
        } while (_leader.ip == butil::IP_ANY || _leader.port == 0);

        if (_channel.Init(_leader, nullptr) != 0) {
            LOG(WARNING) << "leader_channel init failed.";
            return EINVAL;
        }
        LOG(INFO) << "get_leader and init channel success, leader addr:"
            << response.leader_addr();
        return 0;
    }

    brpc::Channel _cluster;
    brpc::Channel _channel;
    butil::EndPoint _leader;
    bthread::Mutex _mutex;
private:
    std::vector<braft::PeerId> _peers;
};

struct put_file_args
{
    std::string dirname;
    std::vector<braft::PeerId> peers;
};

struct appw_args
{
    int32_t thread_num;
    std::vector<braft::PeerId> peers;
    appw_args(): thread_num(0) {}
};

class DbTest {
public:
    DbTest(std::vector<braft::PeerId> &c_peers): peers(c_peers) {}

    //put test
    static void *put_func(void *args) {
        pthread_t tid = pthread_self();
        std::vector<braft::PeerId> *peers = static_cast<std::vector<braft::PeerId>*>(args);
        inspur_kvdb::DbClient db_client(*peers);

        auto single_time_st = std::chrono::steady_clock::now();
        for (int32_t i = 0; i < FLAGS_put_num; i++) {
            std::string key(FLAGS_key);
            std::string value;
            butil::string_appendf(&key, "_key_%ld_%d", tid, i);
            butil::string_printf(&value, "value_%ld_%d", tid, i);
            //std::cout << "key:" << key << ",value:" << value << std::endl;
            db_client.put(key, value);
        }
        auto single_time_ed = std::chrono::steady_clock::now();
        std::cout << "--tid:" << tid << " time cost:" << std::chrono::duration_cast<std::chrono::nanoseconds>(single_time_ed - single_time_st).count() << std::endl;
        pthread_exit(0);
    }

    void put_test() {
        /*for (int i=0; i<FLAGS_thread_num; ++i) {
            pthread_t ptid;
            int res = pthread_create(&ptid, NULL, put_func, &peers);
            if (res != 0) {
                std::cout << " create thread failed" << std::endl;
                continue;
            }
            std::cout << " create thread ok, threadid:" << ptid << std::endl;
            put_threads.push_back(ptid);
        }*/
       std::cout << " key:(" << FLAGS_key << "),value:(" << FLAGS_value << ")" << std::endl;
       inspur_kvdb::DbClient db_client(peers);
       db_client.put(FLAGS_key, FLAGS_value);
    }

    //put file test
    /*every thread process one dir omap*/
    static void *put_files_func(void *args)
    {
        //pthread_t tid = pthread_self();
        struct put_file_args *pargs = (put_file_args*)(args);
        inspur_kvdb::DbClient db_client(pargs->peers);

        std::string dirname = pargs->dirname;
        //std::cout << " --" << tid << ": dirname:" << dirname << std::endl;
        std::string dirpath = FILE_METADATA_DIR;
        dirpath += "/";
        dirpath += dirname;
        //std::cout << " --" << tid << ": put files from dirpath:" << dirpath << std::endl;

        DIR *dirp = opendir(dirpath.c_str());
        //std::cout << " --" << tid << ": opendir dirpath:" << dirpath << ", dirp:" << dirp << std::endl;
        struct dirent *dp;
        int num = 0;
        while ((dp = readdir(dirp)) != nullptr) {
            //std::cout << " --" << tid << ": dp.d_name:" << dp->d_name << std::endl;
            if (!strncmp(dp->d_name, ".", 2) || !strncmp(dp->d_name, "..", 3)) {
                continue;
            }

            std::string filename = dp->d_name;
            //std::cout << " --" << tid << ": filename:" << filename << std::endl;
            std::string filepath = dirpath;
            filepath += "/";
            filepath += filename;
            //std::cout << " --" << tid << ": filepath:" << filepath << std::endl;

            int fd = open(filepath.c_str(), O_RDONLY);
            if (-1 == fd) {
                //std::cout << " --" << tid << ": open failed, filepath:" << filepath << ", fd:" << fd << std::endl;
                continue;
            } else {
                //std::cout << " --" << tid << ": open ok, filepath:" << filepath << ", fd:" << fd << std::endl;
            }

            char buffer[4096];
            memset(buffer, 0, sizeof(buffer));
            int ret = read(fd, buffer, 4096);
            if (ret < 0) {
                //std::cout << " --" << tid << ": read failed, filename:" << filename << ", ret:" << ret << std::endl;
                continue;
            } else {
                //std::cout << " --" << tid << ": read ok, filename:" << filename << ", ret:" << ret << std::endl;
            }

            ++num;
	    std::string value(buffer, ret);
            //std::cout << " --" << tid << ": put file thread:" << tid << ",key:" << filename << ",value.size:" << value.size() << ", num:" << num << std::endl;
            db_client.put(filename, value);
            close(fd);
        }
        closedir(dirp);

        delete pargs;
        pargs = NULL;

        pthread_exit(0);
    }

    void put_files_test() {
        DIR *dirp = opendir(FILE_METADATA_DIR);
        std::cout << " opendir dirname:"  << FILE_METADATA_DIR << ", dirp:" << dirp << std::endl;
        struct dirent *dp;
        while ((dp = readdir(dirp)) != nullptr) {
            std::cout << "  dp.name:"  << dp->d_name << std::endl;
            if (!strncmp(dp->d_name, ".", 2) || !strncmp(dp->d_name, "..", 3)) {
                continue;
            }
            std::cout << "dirname:" << dp->d_name << std::endl;
            struct put_file_args *args = new put_file_args();
            args->dirname = dp->d_name;
            args->peers = peers;
            pthread_t ptid;
            int res = pthread_create(&ptid, NULL, put_files_func, args);
            if (res != 0) {
                std::cout << " create thread failed" << std::endl;
                continue;
            }
            std::cout << " create thread ok, threadid:" << ptid << std::endl;
            put_file_threads.push_back(ptid);
        }
        closedir(dirp);
    }

    //put append write data test
    static void *put_appw_func(void *args) {
        pthread_t tid = pthread_self();
        struct appw_args *pargs = (appw_args*)(args);
        inspur_kvdb::DbClient db_client(pargs->peers);

        std::string appw_key_file;
        if (0 == FLAGS_appw_key_type) {
            appw_key_file = APPW_SEQ_KEY_FILE;
        } else {
            appw_key_file = APPW_RANDOM_KEY_FILE;
        }

        std::ifstream skey(appw_key_file);
        std::string key;//format: key_FLAGS_key_suffix_threadnum, eg:key_202407171030_0
        std::string value = APPW_VALUE;
        std::cout << " --" << tid << ": keyfile:" << appw_key_file << ",APPW_VALUE.len:" << strlen(APPW_VALUE) << ",value.size:" << value.size()
                << ",thread num:" << pargs->thread_num << std::endl;

        int num = 0;
        if (skey.is_open()) {
            while (getline(skey, key)) {
                ++num;
                key += "_";
                key += FLAGS_key_suffix;
                key += "_";
                key += std::to_string(pargs->thread_num);
                //std::cout << " --" << tid << ": put key:(" << key << "),value:(" << value << "),num:" << num << std::endl;
                db_client.put(key, value);
            }
            skey.close();
        } else {
            std::cout << " --" << tid << ": open failed, keyfile:" << appw_key_file << std::endl;
        }

        std::cout << " --" << tid << ": keyfile:" << appw_key_file << ", put num:" << num << std::endl;

        delete pargs;
        pargs = NULL;

        pthread_exit(0);
    }

    void put_appw_test() {
        std::string appw_key_file;
        if (0 == FLAGS_appw_key_type) {
            std::cout << " put appw sequential" << std::endl;
            appw_key_file = APPW_SEQ_KEY_FILE;
        } else {
            std::cout << " put appw random" << std::endl;
            appw_key_file = APPW_RANDOM_KEY_FILE;
        }

        for (int32_t i=0; i < FLAGS_thread_num; ++i) {
            struct appw_args *args = new appw_args();
            args->thread_num = i;
            args->peers = peers;
            pthread_t ptid;
            int res = pthread_create(&ptid, NULL, put_appw_func, args);
            if (res != 0) {
                std::cout << " create put appw thread failed" << std::endl;
                continue;
            }
            std::cout << " create put appw thread ok, threadid:" << ptid << std::endl;
            put_appw_threads.push_back(ptid);
        }
    }

    static void *get_appw_func(void *args) {
        pthread_t tid = pthread_self();
        struct appw_args *pargs = (appw_args*)(args);
        inspur_kvdb::DbClient db_client(pargs->peers);

        std::string appw_key_file;
        if (0 == FLAGS_appw_key_type) {
            appw_key_file = APPW_SEQ_KEY_FILE;
        } else {
            appw_key_file = APPW_RANDOM_KEY_FILE;
        }

        std::ifstream skey(appw_key_file);
        std::string key;//format: key_FLAGS_key_suffix_threadnum, eg:key_202407171030_0
        std::cout << " --" << tid << ": keyfile:" << appw_key_file << ",thread num:" << pargs->thread_num << std::endl;

        int num = 0;
        if (skey.is_open()) {
            while (getline(skey, key)) {
                ++num;
                key += "_";
                key += FLAGS_key_suffix;
                key += "_";
                key += std::to_string(pargs->thread_num);
                std::string value;
                db_client.get(key, value);
                //std::cout << " --" << tid << ": get key:(" << key << "),value:(" << value << "),num:" << num << std::endl;
            }
            skey.close();
        } else {
            std::cout << " --" << tid << ": open failed, keyfile:" << appw_key_file << std::endl;
        }

        std::cout << " --" << tid << ": keyfile:" << appw_key_file << ", get num:" << num << std::endl;

        delete pargs;
        pargs = NULL;

        pthread_exit(0);
    }

    void get_appw_test() {
        std::string appw_key_file;
        if (0 == FLAGS_appw_key_type) {
            std::cout << " get appw sequential" << std::endl;
            appw_key_file = APPW_SEQ_KEY_FILE;
        } else {
            std::cout << " get appw random" << std::endl;
            appw_key_file = APPW_RANDOM_KEY_FILE;
        }

        for (int32_t i=0; i < FLAGS_thread_num; ++i) {
            struct appw_args *args = new appw_args();
            args->thread_num = i;
            args->peers = peers;
            pthread_t ptid;
            int res = pthread_create(&ptid, NULL, get_appw_func, args);
            if (res != 0) {
                std::cout << " create get appw thread failed" << std::endl;
                continue;
            }
            std::cout << " create get appw thread ok, threadid:" << ptid << std::endl;
            get_appw_threads.push_back(ptid);
        }
    }

    void get_put_appw_test() {
        std::string appw_key_file;
        if (0 == FLAGS_appw_key_type) {
            std::cout << " get appw sequential" << std::endl;
            appw_key_file = APPW_SEQ_KEY_FILE;
        } else {
            std::cout << " get appw random" << std::endl;
            appw_key_file = APPW_RANDOM_KEY_FILE;
        }

        bool op_type = 0;//0 read, 1 write
        int32_t op_num = 0;//6:4 read 3, write 2
        int32_t get_thread_num = 0, put_thread_num = 0;
        for (int32_t i=0; i < FLAGS_thread_num; ++i) {
            ++op_num;
            struct appw_args *args = new appw_args();
            args->thread_num = i;
            args->peers = peers;
            pthread_t ptid;
            if (0 == op_type) {
                int res = pthread_create(&ptid, NULL, get_appw_func, args);
                if (res != 0) {
                    std::cout << " create get appw thread failed" << std::endl;
                    continue;
                }
                std::cout << " create get appw thread ok, threadid:" << ptid << std::endl;

                if (op_num >= 3) {
                    op_type = 1;
                    op_num = 0;
                }
                ++get_thread_num;
            } else {
                int res = pthread_create(&ptid, NULL, put_appw_func, args);
                if (res != 0) {
                    std::cout << " create put appw thread failed" << std::endl;
                    continue;
                }
                std::cout << " create put appw thread ok, threadid:" << ptid << std::endl;

                if (op_num >= 2) {
                    op_type = 0;
                    op_num = 0;
                }
                ++put_thread_num;
            }
            get_put_appw_threads.push_back(ptid);
        }

        std::cout << "get_thread_num:" << get_thread_num << ",put_thread_num:" << put_thread_num << std::endl;
    }

    void get_test() {
       inspur_kvdb::DbClient db_client(peers);
       std::string value;
       std::cout << " key:(" << FLAGS_key << "),value:(" << value << ")" << std::endl;
       db_client.get(FLAGS_key, value);
       std::cout << " key:(" << FLAGS_key << "),value:(" << value << ")" << std::endl;
    }

    void wait_threads_finish() {
        for (std::vector<pthread_t>::iterator it=put_threads.begin(); it!=put_threads.end(); ++it) {
            std::cout << " wait thread finish:" << *it << std::endl;
            pthread_join(*it, NULL);
        }

        for (std::vector<pthread_t>::iterator fit=put_file_threads.begin(); fit!=put_file_threads.end(); ++fit) {
            std::cout << " wait put file thread finish:" << *fit << std::endl;
            pthread_join(*fit, NULL);
        }

        for (std::vector<pthread_t>::iterator pait=put_appw_threads.begin(); pait!=put_appw_threads.end(); ++pait) {
            std::cout << " wait put appw thread finish:" << *pait << std::endl;
            pthread_join(*pait, NULL);
        }

        for (std::vector<pthread_t>::iterator gait=get_appw_threads.begin(); gait!=get_appw_threads.end(); ++gait) {
            std::cout << " wait get appw thread finish:" << *gait << std::endl;
            pthread_join(*gait, NULL);
        }

        for (std::vector<pthread_t>::iterator gpait=get_put_appw_threads.begin(); gpait!=get_put_appw_threads.end(); ++gpait) {
            std::cout << " wait get put appw thread finish:" << *gpait << std::endl;
            pthread_join(*gpait, NULL);
        }
    }

private:
    std::vector<pthread_t> put_threads;
    std::vector<pthread_t> put_file_threads;
    std::vector<pthread_t> put_appw_threads;
    std::vector<pthread_t> get_appw_threads;
    std::vector<pthread_t> get_put_appw_threads;

    std::vector<braft::PeerId> peers;
};

}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    //logging::SetMinLogLevel(FLAGS_log_level);
    google::InitGoogleLogging(argv[0]);
    FLAGS_log_dir = "./log";
    FLAGS_logtostderr = 0;

    // parse cluster
    if (FLAGS_cluster_ns.length() == 0) {
        LOG(ERROR) << "db_client need set peers";
        return -1;
    }
    std::vector<braft::PeerId> peers;
    //const char* the_string_to_split = FLAGS_cluster_ns.c_str() + strlen("list://");
    LOG(ERROR) << "cluster ns " << FLAGS_cluster_ns;
    for (butil::StringSplitter s(FLAGS_cluster_ns.c_str(), ','); s; ++s) {
        braft::PeerId peer(std::string(s.field(), s.length()));
        LOG(ERROR) << "add peer" << peer.to_string();
        peers.push_back(peer);
    }
    if (peers.size() == 0) {
        LOG(ERROR) << "db_client need set cluster_ns";
        return -1;
    }
    
    auto time_st = std::chrono::steady_clock::now();

    inspur_kvdb::DbTest db_test(peers);
    if (0 == FLAGS_db_op.compare("put") && !FLAGS_key.empty() && !FLAGS_value.empty()) {
        db_test.put_test();
    } else if (0 == FLAGS_db_op.compare("get") && !FLAGS_key.empty()) {
        db_test.get_test();
    } else if (0 == FLAGS_db_op.compare("put")) {
        db_test.put_test();
    } else if (0 == FLAGS_db_op.compare("put_files")) {
        db_test.put_files_test();
    } else if (0 == FLAGS_db_op.compare("put_appw") && !FLAGS_key_suffix.empty()) {
        db_test.put_appw_test();
    } else if (0 == FLAGS_db_op.compare("get_appw") && !FLAGS_key_suffix.empty()) {
        db_test.get_appw_test();
    } else if (0 == FLAGS_db_op.compare("get_put_appw") && !FLAGS_key_suffix.empty()) {
         db_test.get_put_appw_test();
    } else {
        std::cout << "[Usage]" << std::endl;
        std::cout << "./kvdb_client --cluster_ns=127.0.0.1:8000 --db_op=get --key=''" << std::endl;
        std::cout << "./kvdb_client --cluster_ns=127.0.0.1:8000 --db_op=put --key='' --value=''" << std::endl;
        std::cout << "./kvdb_client --cluster_ns=127.0.0.1:8000 --appw_key_type=[0|1] --db_op=put_appw --key_suffix='' --thread_num=256 --value_len=[0|1]" << std::endl;
        std::cout << "./kvdb_client --cluster_ns=127.0.0.1:8000 --appw_key_type=[0|1] --db_op=get_appw --key_suffix='' --thread_num=256" << std::endl;
        std::cout << "./kvdb_client --cluster_ns=127.0.0.1:8000 --appw_key_type=[0|1] --db_op=get_put_appw --key_suffix='' --thread_num=256 --value_len=[0|1]" << std::endl;
        return -1;
    }
    db_test.wait_threads_finish();

    auto time_end = std::chrono::steady_clock::now();
    //std::cout << "time cost" << std::chrono::duration_cast<std::chrono::nanoseconds>((time_end - time_st)).count() / FLAGS_put_num << std::endl;
    std::cout << "***********************total time cost:" << std::chrono::duration_cast<std::chrono::nanoseconds>((time_end - time_st)).count() << std::endl;
    LOG(ERROR) << "****** end *******"; 
    google::ShutdownGoogleLogging();
    return 0;
}
