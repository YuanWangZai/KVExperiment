#include <butil/file_util.h>
#include <bthread/execution_queue.h>
#include <braft/log_manager.h>
#include <braft/node.h>

namespace inspur_kvdb {
namespace base_test {

struct LogEntryAndClosure {
    braft::LogEntry* entry;
    braft::Closure* done;
    int64_t expected_term;
};

bthread::ExecutionQueueId<LogEntryAndClosure> g_apply_queue_id;

static void test_exec() {
    braft::Task task;
    
}
}
}