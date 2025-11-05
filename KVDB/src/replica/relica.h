#ifndef REPLICA_REPLICABASE_H
#define REPLICA_REPLICABASE_H
#include <cstdint>
#include <google/protobuf/message.h>
#include <string>
#include <braft/raft.h>
#include "state_machine.h"
/*
一个节点上，replica 的数据结构
*/
namespace inspur_kvdb {
class Replica {
public:

private:
    std::pair<std::string, std::string> _range;
private:
    braft::GroupId  _group;
    braft::PeerId _peer_id;
    RaftStateMahine _state_machine;
};
}
#endif