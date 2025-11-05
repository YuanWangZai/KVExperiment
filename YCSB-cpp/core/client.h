//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <iostream>
#include <string>

#include "db.h"
#include "core_workload.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/utils.h"

#include <base/hwloc_mgr.h>

namespace ycsbc {

static std::mutex bind_mutex;

inline int ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops, bool is_loading,
                        bool init_db, bool cleanup_db, utils::CountDownLatch *latch, utils::RateLimiter *rlim, int numa_node, int core_idx) {

  try {
    if (init_db) {
      db->Init();
    }
    // 添加绑核逻辑
    // 绑定线程到特定的 NUMA 节点和 CPU 核心

    // 绑定线程
    
    {
      std::lock_guard<std::mutex> lock(bind_mutex);
      if (inspur_kvdb::base::g_hw_mgr.alloc_hw_and_bind(numa_node) != 0) {
      LOG(ERROR) << "Failed to bind thread to NUMA node " << 2;
      return -1;
      }
    }

    // 打印绑核信息
    // cpu_set_t cpuset;
    // sched_getaffinity(0, sizeof(cpu_set_t), &cpuset);

    // std::cout << "Thread " << std::this_thread::get_id() << " is bound to CPUs: ";
    // for (int i = 0; i < CPU_SETSIZE; i++) {
    //     if (CPU_ISSET(i, &cpuset)) {
    //         std::cout << i << " ";
    //     }
    // }
    // std::cout << std::endl;

    int ops = 0;
    for (int i = 0; i < num_ops; ++i) {
      if (rlim) {
        rlim->Consume(1);
      }

      if (is_loading) {
        wl->DoInsert(*db);
      } else {
        wl->DoTransaction(*db);
      }
      ops++;
    }

    if (cleanup_db) {
      db->Cleanup();
    }

    latch->CountDown();
    return ops;
  } catch (const utils::Exception &e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
    exit(1);
  }
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
