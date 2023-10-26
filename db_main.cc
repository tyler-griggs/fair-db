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
// Is least service based on queue right now?
// per-client sync vs async
// bursty client
// per-client queue max between pipeline steps (creates synchrony from async)
// update queue naming from "client queues"
// clean up scripts: 1) per-client timeline, 2) per-worker timeline
//                   3) handle ops that were started by readers but not finished
//                   by computers
// real DRFQ

int main() {
  srand(time(0));
  size_t db_size = 40e9; // Number of elements in the db (40B ints, 160GB)
  size_t datatype_size = sizeof(int);

  int run_duration_s = 60; // Workload duration before shutdown

  // Max queue length for each client.
  int max_queue_length = 16;
  int max_interop_queueing = 16;

  size_t client0_read_size = 0.5 * 1e9;
  int client0_compute_ms = 1.5 * 1000;
  int client0_query_interval_ms = 7 * 1000;
  // int client0_query_interval_ms = 0;
  vector<int> task_order0{0, 1, 1, 1};

  size_t client1_read_size = 0.5 * 1e9;
  int client1_compute_ms = 0.5 * 1000;
  vector<int> task_order1{0, 1, 1, 1};

  int num_worker_threads = 2;
  int num_clients = 2;

  // Core IDs for clients and workers. Clients share a core.
  // TODO: move this from client core
  std::vector<int> worker_cores{3, 2, 1, 0};
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
    };
    DBClient client1(/*client_id=*/1, client_options1, db_size, task_order1,
                     client1_read_size / datatype_size, client1_compute_ms);
    // TODO: before changing to random, update db_worker assumption of
    //       single read and single compute ops
    client_threads.push_back(client1.RunSequential(stop));
    // client_threads.push_back(client1.RunRandom(stop, num_random));
    SetThreadAffinity(client_threads[1], client_core);

    // TODO: this is a hack to reduce num cores needed
    // Let clients fill queues
    // std::this_thread::sleep_for(std::chrono::seconds(5));
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
        .scheduler = SchedulerType::PER_RESOURCE_FAIR,
        // .scheduler = SchedulerType::FIFO,
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
        .scheduler = SchedulerType::PER_RESOURCE_FAIR,
        // .scheduler = SchedulerType::FIFO,
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