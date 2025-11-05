#ifndef REPLICA_DISTRIBUTED_REPLICA
#define REPLICA_DISTRIBUTED_REPLICA
/*
基于client 的分发复制 
client 将数据直接分发至所有副本。
延迟有好的表现，但问题在：
1）对client 资源要求高；
2）如果要求所有节点返回，慢节点影响大；
3) 不能有多个client 同时写同key数据，一致性处理困难，需要引入 mvcc 机制及重试机制。
4）client 故障时，无法判定写入数据是否有效，对读取者来说产生困惑
*/
namespace inspur_kvdb {
//
class ConsensusImpl {
public:
    virtual void on_apply_call();

private:

};

}
#endif