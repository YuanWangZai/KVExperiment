#ifndef REPLICA_REPLICA_MGR_H
#define REPLICA_REPLICA_MGR_H
#include <braft/node_manager.h>
namespace inspur_kvdb {
class ReplicaMgr {
public:
    ReplicaMgr() {

    }
    
private:
    ReplicaMgr(ReplicaMgr& ) = delete;
    ReplicaMgr operator = (const ReplicaMgr&) = delete;
private:
    braft::NodeManager* _node_mgr;
};
}
#endif