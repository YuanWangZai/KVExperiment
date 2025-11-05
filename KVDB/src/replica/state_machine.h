#ifndef REPLICA_STATEMACHINE_H
#define REPLICA_STATEMACHINE_H
#include <braft/storage.h>
#include <braft/util.h>
#include <brpc/controller.h>
#include <butil/files/file_enumerator.h>
#include <google/protobuf/message.h>
#include <braft/raft.h>
#include <kv_api.pb.h>
#include <bthread/bthread.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/checkpoint.h>
namespace inspur_kvdb {

static bvar::LatencyRecorder g_fsm_put_latency("fsm_put");
static bvar::LatencyRecorder g_raft_fsm_put_latency("raft_fsm_put");

static bool copy_snapshot(const std::string& from_path, const std::string& to_path) {
    struct stat from_stat;
    if (stat(from_path.c_str(), &from_stat) < 0 || !S_ISDIR(from_stat.st_mode)) {
        DLOG(WARNING) << "stat " << from_path << " failed";
        return false;
    }
    if (!butil::CreateDirectory(butil::FilePath(to_path))) {
        DLOG(WARNING) << "CreateDirectory " << to_path << " failed";
        return false;
    }

    butil::FileEnumerator dir_enum(butil::FilePath(from_path),
                                  false, butil::FileEnumerator::FILES);
    for (butil::FilePath name = dir_enum.Next(); !name.empty(); name = dir_enum.Next()) {
        std::string src_file(from_path);
        std::string dst_file(to_path);
        butil::string_appendf(&src_file, "/%s", name.BaseName().value().c_str());
        butil::string_appendf(&dst_file, "/%s", name.BaseName().value().c_str());

        if (0 != link(src_file.c_str(), dst_file.c_str())) {
            if (!butil::CopyFile(butil::FilePath(src_file), butil::FilePath(dst_file))) {
                DLOG(WARNING) << "copy " << src_file << " to " << dst_file << " failed";
                return false;
            }
        }
    }

    return true;
}

struct DbClosure : public braft::Closure {
    DbClosure(brpc::Controller* cntl_, PutResponse* response_,
              google::protobuf::Closure* done_)
        : cntl(cntl_), response(response_), done(done_) {
            start_time = std::chrono::steady_clock::now();
        }
    void Run() {
        if (!status().ok()) {
            cntl->SetFailed(status().error_code(), "%s", status().error_cstr());
        }
        done->Run();
        delete this;
        auto end = std::chrono::steady_clock::now();
        //LOG(INFO) << "call time for request : " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_time).count();
    }

    brpc::Controller* cntl;
    PutResponse* response;
    google::protobuf::Closure* done;
    std::chrono::time_point<std::chrono::steady_clock> start_time;
};

class RaftStateMahine : public braft::StateMachine {
public:
    RaftStateMahine(const braft::GroupId& group_id, const braft::PeerId& peer_id) :
        //: braft::StateMachine(group_id, peer_id),
        _group_id(group_id),
        _peer_id(peer_id), 
        _is_leader(false), _db(nullptr) {
    }

    virtual ~RaftStateMahine() {}

    int init(braft::NodeOptions opt) {
        braft::Node* node = new braft::Node(_group_id, _peer_id);
        if (node->init(opt) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        LOG(INFO) << "init raft node done";
        _node = node;
        return 0;
    }
    int get(const std::string& key,
            GetResponse* response,
            ::google::protobuf::RpcController* controller) {
        brpc::Controller* cntl = (brpc::Controller*)controller;
        if (!_is_leader.load(butil::memory_order_acquire)) {
            LOG(INFO) << "this is not leader, get request, key:" << key;
            cntl->SetFailed(brpc::EREQUEST, "not leader");
            return -1;
        }

        CHECK(_db != NULL);

        std::string value;
        rocksdb::Status status = _db->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok()) {
            LOG(INFO) << "this is leader, get request, key:" << key << " value:" << value; 
            response->set_value(value);
        } else {
            LOG(WARNING) << "get failed, key:" << key;
            cntl->SetFailed(brpc::EREQUEST, status.ToString().c_str());
        }

        return 0;
    }

    // FSM method
    void on_apply(braft::Iterator& iter) {
        //LOG(INFO) << "on_apply";

        for (; iter.valid(); iter.next()) {
            braft::Closure* done = iter.done();
            brpc::ClosureGuard done_guard(done);
            const butil::IOBuf& data = iter.data();

            fsm_put(static_cast<DbClosure*>(done), data);

            if (done) {
                braft::run_closure_in_bthread_nosig(done_guard.release());
            }
        }
        bthread_flush();
    }

    void on_shutdown() {
        LOG(INFO) << "on_shutdown";
        // FIXME: should be more elegant
        exit(0);
    }

    void on_error(const braft::Error& e) {
        LOG(INFO) << "on_error " << e;
        // FIXME: should be more elegant
        exit(0);
    }

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
        CHECK(_db != NULL);

        LOG(INFO) << "on_snapshot_save";
        brpc::ClosureGuard done_guard(done);

        std::string snapshot_path = writer->get_path() + "/rocksdb_snapshot";

        // 使用rocksdb的checkpoint特性实现snapshot
        rocksdb::Checkpoint* checkpoint = NULL;
        rocksdb::Status status = rocksdb::Checkpoint::Create(_db, &checkpoint);
        if (!status.ok()) {
            LOG(WARNING) << "Checkpoint Create failed, msg:" << status.ToString();
            return;
        }

        std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);
        status = checkpoint->CreateCheckpoint(snapshot_path);
        if (!status.ok()) {
            LOG(WARNING) << "Checkpoint CreateCheckpoint failed, msg:" << status.ToString();
            return;
        }

        // list出所有的文件 加到snapshot中
        butil::FileEnumerator dir_enum(butil::FilePath(snapshot_path),
                                      false, butil::FileEnumerator::FILES);
        for (butil::FilePath name = dir_enum.Next(); !name.empty(); name = dir_enum.Next()) {
            std::string file_name = "rocksdb_snapshot/" + name.BaseName().value();
            writer->add_file(file_name);
        }
    }

    int on_snapshot_load(braft::SnapshotReader* reader) {
        LOG(INFO) << "on_snapshot_load";

        if (_db != NULL) {
            delete _db;
            _db = NULL;
        }

        std::string snapshot_path = reader->get_path();
        snapshot_path.append("/rocksdb_snapshot");

        // 先删除原来的db 使用snapshot和wal还原一个最新的db
        std::string db_path = "./data/rocksdb_data";
        if (!butil::DeleteFile(butil::FilePath(db_path), true)) {
            LOG(WARNING) << "rm " << db_path << " failed";
            return -1;
        }

        LOG(INFO) << "rm " << db_path << " success";
        //TODO: try use link instead of copy
        if (!copy_snapshot(snapshot_path, db_path)) {
            LOG(WARNING) << "copy snapshot " << snapshot_path << " to " << db_path << " failed";
            return -1;
        }

        LOG(INFO) << "copy snapshot " << snapshot_path << " to " << db_path << " success";
        // 重新打开db
        return init_rocksdb();
    }

    // Acutally we don't care now
    void on_leader_start(int64_t term) {
        LOG(INFO) << "leader start at term: " << term;
        _is_leader.store(true, butil::memory_order_release);
    }

    void on_leader_stop() {
        LOG(INFO) << "leader stop";
        _is_leader.store(false, butil::memory_order_release);
    }

    void apply(butil::IOBuf *iobuf, braft::Closure* done) {
        //LOG(INFO) << "node apply call";
        braft::Task task;
        task.data = iobuf;
        task.done = done;
        return _node->apply(task);
    }

    int init_rocksdb() {
        if (_db != NULL) {
            LOG(INFO) << "rocksdb already opened";
            return 0;
        }

        std::string db_path = "./data/rocksdb_data";
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

	std::string stats;
        if (_db->GetProperty("rocksdb.stats", &stats)) {
            LOG(INFO) << "***********************************rocksdb stats:" << stats;
        }

        return 0;
    }

    bool leader() {
        return false;
    }
private:
    void fsm_put(DbClosure* done, const butil::IOBuf& data) {
        butil::IOBufAsZeroCopyInputStream wrapper(data);
        PutRequest req;

        int64_t start_time = butil::cpuwide_time_us();

        if (!req.ParseFromZeroCopyStream(&wrapper)) {
            if (done) {
                done->status().set_error(brpc::EREQUEST,
                                "Fail to parse buffer");
            }
            LOG(WARNING) << "Fail to parse PutRequest";
            return;
        }

        //LOG(INFO) << "get put request:" << req.ShortDebugString();

        g_raft_fsm_put_latency << (butil::cpuwide_time_us() - start_time);
	return;

        /*int64_t now = 0;
        int64_t delta_time_us = 0;
        now = butil::cpuwide_time_us();
	*/

        // WriteOptions not need WAL, use braft's WAL
        rocksdb::WriteOptions options;
        options.disableWAL = true;
        rocksdb::Status status = _db->Put(options, req.key(), req.value());
        if (!status.ok()) {
            if (done) {
                done->status().set_error(brpc::EREQUEST, status.ToString());
            }
            LOG(WARNING) << "Put failed, key:" << req.key() << " value:" << req.value();
        } else {
            //LOG(INFO) << "Put success, key:" << req.key() << " value:" << req.value();
            /*delta_time_us = butil::cpuwide_time_us() - now;
            g_fsm_put_latency << delta_time_us;*/
        }
    }

private:
    braft::GroupId _group_id;
    braft::PeerId  _peer_id;
    butil::atomic<bool> _is_leader;
    rocksdb::DB* _db;
    braft::Node* volatile _node;
    butil::atomic<int64_t> _leader_term;
    
};

class ReplicaStateMachine : public braft::StateMachine {
public:
     
};
}
#endif
