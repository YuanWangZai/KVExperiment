//
//  ycsbc.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <ctime>

#include <string>
#include <iostream>
#include <vector>
#include <thread>
#include <future>
#include <chrono>
#include <iomanip>

#include "client.h"
#include "core_workload.h"
#include "db_factory.h"
#include "measurements.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/timer.h"
#include "utils/utils.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include "config/config.h"

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
void ParseCommandLine(int argc, char *argv[], ycsbc::utils::Properties &props);

// 定义 gflags 参数
DEFINE_bool(load, false, "Run the loading phase of the workload");
DEFINE_bool(run, false, "Run the transactions phase of the workload");
DEFINE_int32(threads, 1, "Number of threads to use");
DEFINE_string(db, "basic", "Name of the DB to use");
DEFINE_string(propertyfile, "", "Load properties from the given file");
DEFINE_string(properties, "", "Specify properties in key=value format");
DEFINE_bool(status, false, "Print status every 10 seconds");

void StatusThread(ycsbc::Measurements *measurements, ycsbc::utils::CountDownLatch *latch, int interval) {
  using namespace std::chrono;
  time_point<system_clock> start = system_clock::now();
  bool done = false;
  while (1) {
    time_point<system_clock> now = system_clock::now();
    std::time_t now_c = system_clock::to_time_t(now);
    duration<double> elapsed_time = now - start;

    std::cout << std::put_time(std::localtime(&now_c), "%F %T") << ' '
              << static_cast<long long>(elapsed_time.count()) << " sec: ";

    std::cout << measurements->GetStatusMsg() << std::endl;

    if (done) {
      break;
    }
    done = latch->AwaitFor(interval);
  };
}

void RateLimitThread(std::string rate_file, std::vector<ycsbc::utils::RateLimiter *> rate_limiters,
                     ycsbc::utils::CountDownLatch *latch) {
  std::ifstream ifs;
  ifs.open(rate_file);

  if (!ifs.is_open()) {
    ycsbc::utils::Exception("failed to open: " + rate_file);
  }

  int64_t num_threads = rate_limiters.size();

  int64_t last_time = 0;
  while (!ifs.eof()) {
    int64_t next_time;
    int64_t next_rate;
    ifs >> next_time >> next_rate;

    if (next_time <= last_time) {
      ycsbc::utils::Exception("invalid rate file");
    }

    bool done = latch->AwaitFor(next_time - last_time);
    if (done) {
      break;
    }
    last_time = next_time;

    for (auto x : rate_limiters) {
      x->SetRate(next_rate / num_threads);
    }
  }
}

int main(int argc, char *argv[]) {


  // add service
  google::InitGoogleLogging(argv[0]);
  google::SetStderrLogging(google::ERROR);

  ycsbc::utils::Properties props;
  ParseCommandLine(argc, argv, props);

  const bool do_load = (props.GetProperty("doload", "false") == "true");
  const bool do_transaction = (props.GetProperty("dotransaction", "false") == "true");
  if (!do_load && !do_transaction) {
    std::cerr << "No operation to do" << std::endl;
    exit(1);
  }

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  ycsbc::Measurements *measurements = ycsbc::CreateMeasurements(&props);
  if (measurements == nullptr) {
    std::cerr << "Unknown measurements name" << std::endl;
    exit(1);
  }

  std::vector<ycsbc::DB *> dbs;
  for (int i = 0; i < num_threads; i++) {
    ycsbc::DB *db = ycsbc::DBFactory::CreateDB(&props, measurements);
    if (db == nullptr) {
      std::cerr << "Unknown database name " << props["dbname"] << std::endl;
      exit(1);
    }
    dbs.push_back(db);
  }

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  CHECK(inspur_kvdb::load_gloabl_conf() == 0);
  inspur_kvdb::base::g_hw_mgr.init();

  // print status periodically
  const bool show_status = (props.GetProperty("status", "false") == "true");
  const int status_interval = std::stoi(props.GetProperty("status.interval", "10"));

  // load phase
  if (do_load) {
    const int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);

    ycsbc::utils::CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<double> timer;

    timer.Start();
    std::future<void> status_future;
    if (show_status) {
      status_future = std::async(std::launch::async, StatusThread,
                                 measurements, &latch, status_interval);
    }
    std::vector<std::future<int>> client_threads;
    int numa_node = 2; // numa0运行target numa1运行brpc monitor
    int core_idx = 0;
    for (int i = 0; i < num_threads; ++i) {
      int thread_ops = total_ops / num_threads;
      if (i < total_ops % num_threads) {
        thread_ops++;
      }

      client_threads.emplace_back(std::async(std::launch::async, ycsbc::ClientThread, dbs[i], &wl,
                                             thread_ops, true, true, !do_transaction, &latch, nullptr, numa_node, core_idx));
      core_idx ++;
      if (core_idx == 6) {
        core_idx = 0;
        numa_node ++;
      }
    }
    assert((int)client_threads.size() == num_threads);

    int sum = 0;
    for (auto &n : client_threads) {
      assert(n.valid());
      sum += n.get();
    }
    double runtime = timer.End();

    if (show_status) {
      status_future.wait();
    }

    std::cout << "Load runtime(sec): " << runtime << std::endl;
    std::cout << "Load operations(ops): " << sum << std::endl;
    std::cout << "Load throughput(ops/sec): " << sum / runtime << std::endl;
  }

  measurements->Reset();
  std::this_thread::sleep_for(std::chrono::seconds(stoi(props.GetProperty("sleepafterload", "0"))));


  // transaction phase
  if (do_transaction) {
    // initial ops per second, unlimited if <= 0
    const int64_t ops_limit = std::stoi(props.GetProperty("limit.ops", "0"));
    // rate file path for dynamic rate limiting, format "time_stamp_sec new_ops_per_second" per line
    std::string rate_file = props.GetProperty("limit.file", "");

    const int total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);

    ycsbc::utils::CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<double> timer;

    timer.Start();
    std::future<void> status_future;
    if (show_status) {
      status_future = std::async(std::launch::async, StatusThread,
                                 measurements, &latch, status_interval);
    }
    std::vector<std::future<int>> client_threads;
    std::vector<ycsbc::utils::RateLimiter *> rate_limiters;
    int numa_node = 2; // numa0运行target numa1运行brpc monitor
    int core_idx = 0;
    for (int i = 0; i < num_threads; ++i) {
      int thread_ops = total_ops / num_threads;
      if (i < total_ops % num_threads) {
        thread_ops++;
      }
      ycsbc::utils::RateLimiter *rlim = nullptr;
      if (ops_limit > 0 || rate_file != "") {
        int64_t per_thread_ops = ops_limit / num_threads;
        rlim = new ycsbc::utils::RateLimiter(per_thread_ops, per_thread_ops);
      }
      rate_limiters.push_back(rlim);
      client_threads.emplace_back(std::async(std::launch::async, ycsbc::ClientThread, dbs[i], &wl,
                                             thread_ops, false, !do_load, true, &latch, rlim, numa_node, core_idx));
      core_idx ++;
      if (core_idx == 6) {
        core_idx = 0;
        numa_node ++;
      }
    }

    std::future<void> rlim_future;
    if (rate_file != "") {
      rlim_future = std::async(std::launch::async, RateLimitThread, rate_file, rate_limiters, &latch);
    }

    assert((int)client_threads.size() == num_threads);

    int sum = 0;
    for (auto &n : client_threads) {
      assert(n.valid());
      sum += n.get();
    }
    double runtime = timer.End();

    if (show_status) {
      status_future.wait();
    }

    std::cout << "Run runtime(sec): " << runtime << std::endl;
    std::cout << "Run operations(ops): " << sum << std::endl;
    std::cout << "Run throughput(ops/sec): " << sum / runtime << std::endl;
  }

  for (int i = 0; i < num_threads; i++) {
    delete dbs[i];
  }
}

void ParseCommandLine(int argc, char *argv[], ycsbc::utils::Properties &props) {
    // Parse command line arguments using gflags
    google::ParseCommandLineFlags(&argc, &argv, true);

    // Set properties based on gflags values
    props.SetProperty("doload", FLAGS_load ? "true" : "false");
    props.SetProperty("dotransaction", FLAGS_run ? "true" : "false");
    props.SetProperty("threadcount", std::to_string(FLAGS_threads));
    props.SetProperty("dbname", FLAGS_db);

    // Load properties from file if specified
    if (!FLAGS_propertyfile.empty()) {
        std::ifstream input(FLAGS_propertyfile);
        if (!input.is_open()) {
            std::cerr << "Failed to open property file: " << FLAGS_propertyfile << std::endl;
            exit(1);
        }
        try {
            props.Load(input);
        } catch (const std::string &message) {
            std::cerr << message << std::endl;
            exit(1);
        }
        input.close();
    }

    // Parse additional properties specified via -properties flag
    if (!FLAGS_properties.empty()) {
        std::istringstream props_stream(FLAGS_properties);
        std::string prop;
        while (std::getline(props_stream, prop, ',')) {
            size_t eq = prop.find('=');
            if (eq == std::string::npos) {
                std::cerr << "Invalid property format: " << prop << std::endl;
                exit(1);
            }
            std::string key = ycsbc::utils::Trim(prop.substr(0, eq));
            std::string value = ycsbc::utils::Trim(prop.substr(eq + 1));
            props.SetProperty(key, value);
        }
    }

    props.SetProperty("status", FLAGS_status ? "true" : "false");
}

void UsageMessage(const char *command) {
    std::cout <<
        "Usage: " << command << " [options]\n"
        "Options:\n"
        "  --doload: run the loading phase of the workload\n"
        "  --dotransaction: run the transactions phase of the workload\n"
        "  --threads: execute using n threads (default: 1)\n"
        "  --db: specify the name of the DB to use (default: basic)\n"
        "  --propertyfile: load properties from the given file\n"
        "  --properties: specify properties in key=value format (comma-separated)\n"
        "  --status: print status every 10 seconds\n"
        << std::endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

