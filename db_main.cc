#include <chrono>
#include <fstream>
#include <iostream>
#include <numeric>
#include <pthread.h>
#include <sched.h>
#include <time.h>
#include <vector>

#include "db_client.h"
#include "db_manager.h"
#include "utils.h"

using namespace std;

// TODOs:
// create a make file
// restructure db - move more logic to manager as single entity that knows all
//      options
// profile how long queue logic is taking - what's time between completing op
//      and starting next one?
// per-client queue max between pipeline steps (creates synchrony from async)
// clean up scripts: 1) per-client timeseries, 2) per-worker timeseries
//                   3) handle ops that were started by readers but not finished
//                   by computers
// add util method for logging and add timestamps
// separate logs for each worker, then merge them

int main() {
  srand(time(0));
  size_t db_size = 40e9; // Number of elements in the db (40B ints, 160GB)
  size_t datatype_size = sizeof(int);

  int run_duration_s = 30; // Workload duration before shutdown

  // Maximum outstanding queries per client
  int max_outstanding_queries = 256;
  // Maximum length of queues inside the DB
  int max_interop_queueing = 256;

  size_t client0_read_size = 0.1 * 1e9;
  int client0_compute_ms = 0.1 * 1000;
  //   int client0_query_interval_ms = 0 * 1000;
  int client0_query_interval_ms = 0.1 * 1000;
  vector<int> task_order0{0, 1};

  size_t client1_read_size = 0.1 * 1e9;
  int client1_compute_ms = 0.1 * 1000;
  int client1_query_interval_ms = .2 * 1000;
  vector<int> task_order1{0, 1};

  int num_worker_threads = 2;
  int num_clients = 2;

  // Core IDs for clients and workers. Clients share a core.
  std::vector<int> worker_cores{3, 2, 1};
  int client_core = 0;

  // Only one worker per thread (for now).
  if (worker_cores.size() < num_worker_threads) {
    cout << "Too many worker threads for given cores" << endl;
    return 0;
  }

  int num_runs = 1;
  // int num_random = 1024;  // For random read clients.

  for (int i = 0; i < num_runs; ++i) {
    std::vector<std::thread> client_threads;
    std::atomic<bool> stop(false);

    auto client0_cpu_queue =
        std::make_shared<ReaderWriterQueue<DBRequest>>(max_interop_queueing);
    auto client1_cpu_queue =
        std::make_shared<ReaderWriterQueue<DBRequest>>(max_interop_queueing);
    auto client0_disk_queue =
        std::make_shared<ReaderWriterQueue<DBRequest>>(max_interop_queueing);
    auto client1_disk_queue =
        std::make_shared<ReaderWriterQueue<DBRequest>>(max_interop_queueing);
    auto client0_completion_queue =
        std::make_shared<ReaderWriterQueue<DBRequest>>(max_interop_queueing);
    auto client1_completion_queue =
        std::make_shared<ReaderWriterQueue<DBRequest>>(max_interop_queueing);
    std::shared_ptr<std::mutex> to_cpu_mutex = std::make_shared<std::mutex>();
    std::shared_ptr<std::mutex> to_disk_mutex = std::make_shared<std::mutex>();

    // Start Client 0
    DBClientOptions client_options0{
        .to_cpu_queue = client0_cpu_queue,
        .to_cpu_mutex = to_cpu_mutex,
        .to_disk_queue = client0_disk_queue,
        .to_disk_mutex = to_disk_mutex,
        .completion_queue = client0_completion_queue,
        .query_interval_ms = client0_query_interval_ms,
        .max_outstanding = max_outstanding_queries,
    };
    DBClient client0(/*client_id=*/0, client_options0, db_size, task_order0,
                     client0_read_size / datatype_size, client0_compute_ms);
    // TODO: before changing to random, update db_worker assumption of
    //       single read and single compute ops
    client_threads.push_back(client0.RunSequential(stop));
    SetThreadAffinity(client_threads[0], client_core);

    // Start Client 1
    DBClientOptions client_options1{
        .to_cpu_queue = client1_cpu_queue,
        .to_cpu_mutex = to_cpu_mutex,
        .to_disk_queue = client1_disk_queue,
        .to_disk_mutex = to_disk_mutex,
        .completion_queue = client1_completion_queue,
        .query_interval_ms = client1_query_interval_ms,
        .max_outstanding = max_outstanding_queries,
    };
    DBClient client1(/*client_id=*/1, client_options1, db_size, task_order1,
                     client1_read_size / datatype_size, client1_compute_ms);
    // TODO: before changing to random, update db_worker assumption of
    //       single read and single compute ops
    client_threads.push_back(client1.RunSequential(stop));
    // client_threads.push_back(client1.RunRandom(stop, num_random));
    SetThreadAffinity(client_threads[1], client_core);

    std::vector<ClientQueueState> to_cpu_queue_state{client0_cpu_queue,
                                                     client1_cpu_queue};
    auto all_to_cpu_queue_state =
        std::make_shared<AllQueueState>(to_cpu_queue_state);
    std::vector<ClientQueueState> to_disk_queue_state{client0_disk_queue,
                                                      client1_disk_queue};
    auto all_to_disk_queue_state =
        std::make_shared<AllQueueState>(to_disk_queue_state);

    std::shared_ptr<std::mutex> to_completion_mutex =
        std::make_shared<std::mutex>();
    std::shared_ptr<std::mutex> disk_input_mutex =
        std::make_shared<std::mutex>();
    std::shared_ptr<std::mutex> cpu_input_mutex =
        std::make_shared<std::mutex>();

    // Worker 0 - Disk Read
    DBWorkerOptions worker0_options{
        .input_queue_state = all_to_disk_queue_state,
        .input_queue_mutex = disk_input_mutex,
        .to_cpu_queue0 = client0_cpu_queue,
        .to_cpu_queue1 = client1_cpu_queue,
        .to_cpu_mutex = to_cpu_mutex,
        .to_disk_queue0 = client0_disk_queue,
        .to_disk_queue1 = client1_disk_queue,
        .to_disk_mutex = to_disk_mutex,
        .to_completion_queue0 = client0_completion_queue,
        .to_completion_queue1 = client1_completion_queue,
        .to_completion_mutex = to_completion_mutex,
        .scheduler_type = SchedulerType::PER_RESOURCE_FAIR,
        // .scheduler_type = SchedulerType::FIFO,
        // .scheduler_type = SchedulerType::ROUND_ROBIN,
        .task = 0, // Read
    };

    // Worker 1 - CPU
    DBWorkerOptions worker1_options{
        .input_queue_state = all_to_cpu_queue_state,
        .input_queue_mutex = cpu_input_mutex,
        .to_cpu_queue0 = client0_cpu_queue,
        .to_cpu_queue1 = client1_cpu_queue,
        .to_cpu_mutex = to_cpu_mutex,
        .to_disk_queue0 = client0_disk_queue,
        .to_disk_queue1 = client1_disk_queue,
        .to_disk_mutex = to_disk_mutex,
        .to_completion_queue0 = client0_completion_queue,
        .to_completion_queue1 = client1_completion_queue,
        .to_completion_mutex = to_completion_mutex,
        .scheduler_type = SchedulerType::PER_RESOURCE_FAIR,
        // .scheduler_type = SchedulerType::FIFO,
        // .scheduler_type = SchedulerType::ROUND_ROBIN,
        .task = 1, // Compute
    };

    std::vector<DBWorkerOptions> worker_options{worker0_options,
                                                worker1_options};

    if (num_worker_threads != worker_options.size()) {
      cout << "Incorrect worker thread and options configuration";
      return 0;
    }

    // Start database
    FairDBManager db_manager{db_size};
    if (!db_manager.Init()) {
      cout << "DB Init failed." << endl;
      return 1;
    }
    db_manager.Run(worker_options, num_worker_threads, worker_cores,
                   run_duration_s, num_clients);

    stop.store(true);
    for (int i = 0; i < client_threads.size(); ++i) {
      client_threads[i].join();
    }
  }
  return 0;
}