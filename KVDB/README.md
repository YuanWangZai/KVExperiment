## kv db
高性能、高可用、高扩展的KV数据库，支持异构机型
0.1 原型，仅支持对等机器，多副本读写;每个节点单个复制组副本。
0.1.x 计划
1. 实现压力工具，构造符合业务特点的数据集合;
2. 建设出观测性，包括rocksdb 引擎的内部指标：perf_context, compaction / leveled 信息统计输出等, 使用日志、signal触发等方式导出;
0.2 计划：
1. rdma 打开;
2. 单节点多复制组，实现 Multi-raft
### kvserver
1.启动命令
{deploy_dir}/kvdb_server --peers=127.0.0.1:8000 --log_dir=./log --logtostderr=false --minloglevel=0
说明 ：
1）二进制位置 ;
2）peers 为集群节点列表，多个使用逗号隔开，rdma 版本还未完善;
3）log_dir: 指定 log 位置 
4) logtostderr 同步日志到err;
5) minloglevel 0，> warning 级别输出;INFO 对应级别 1
### kvclient
{deploy_dir}/client/kvdb_client --cluster_ns=127.0.0.1:8000, --db_op=put  --put_num=1000
cluster_ns 集群列表 
db_op 指定 操作是put 还是get; 
put_num 指定put 操作时，放入的总个数。