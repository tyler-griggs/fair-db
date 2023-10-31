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

int main() {
  srand(time(0));
  /* ======== Database Parameters ======== */
  size_t db_size = 40e9; // Number of elements in the database (40B ints, 160GB)
  size_t datatype_size = sizeof(int); // Size of each element
  int run_duration_s = 30; // Duration to run workload before shutdown

  int max_outstanding_queries = 4; // Maximum outstanding queries per client
  int max_interop_queueing = 16;   // Maximum length of queues inside the DB

  /* ======== Client Parameters ======== */
  int num_clients = 2;
  int client_core = 0;

  size_t client0_read_size = 0.2 * 1e9;
  int client0_compute_ms = 1 * 1000;
  int client0_query_interval_ms = 0 * 1000;
  vector<TaskType> task_order0{
      TaskType::DISK_READ,
      TaskType::COMPUTE,
      TaskType::DISK_READ,
      TaskType::COMPUTE,
  };

  size_t client1_read_size = 0.1 * 1e9;
  int client1_compute_ms = 0.5 * 1000;
  int client1_query_interval_ms = 0 * 1000;
  vector<TaskType> task_order1{
      TaskType::DISK_READ,
      TaskType::COMPUTE,
  };

  /* ======== DB Worker Parameters ======== */
  int num_worker_threads = 3;

  // Core IDs for clients and workers. Clients share a core.
  std::vector<int> worker_cores{3, 2, 1};

  // Only one worker per core.
  if (worker_cores.size() < num_worker_threads) {
    Logger::log("Too many worker threads for given cores");
    return 0;
  }

  /* ======== Build DB Architecture ======== */

  // Create a queue for each client and each task type, and the mutexes
  // that cover enqueue.
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
  std::shared_ptr<std::mutex> to_completion_mutex =
      std::make_shared<std::mutex>();

  std::vector<std::thread> client_threads;
  std::atomic<bool> stop(false);

  // Start Client 0
  DBClientOptions client_options0{
      .to_cpu_queue = client0_cpu_queue,
      .to_cpu_mutex = to_cpu_mutex,
      .to_disk_queue = client0_disk_queue,
      .to_disk_mutex = to_disk_mutex,
      .completion_queue = client0_completion_queue,
      .task_order = task_order0,
      .read_size = client0_read_size / datatype_size,
      .compute_duration_ms = client0_compute_ms,
      .query_interval_ms = client0_query_interval_ms,
      .max_outstanding = max_outstanding_queries,
  };
  DBClient client0(/*client_id=*/0, client_options0, db_size);
  client_threads.push_back(client0.RunSequential(stop));
  SetThreadAffinity(client_threads[0], client_core);

  // Start Client 1
  DBClientOptions client_options1{
      .to_cpu_queue = client1_cpu_queue,
      .to_cpu_mutex = to_cpu_mutex,
      .to_disk_queue = client1_disk_queue,
      .to_disk_mutex = to_disk_mutex,
      .completion_queue = client1_completion_queue,
      .task_order = task_order1,
      .read_size = client1_read_size / datatype_size,
      .compute_duration_ms = client1_compute_ms,
      .query_interval_ms = client1_query_interval_ms,
      .max_outstanding = max_outstanding_queries,
  };
  DBClient client1(/*client_id=*/1, client_options1, db_size);
  client_threads.push_back(client1.RunSequential(stop));
  SetThreadAffinity(client_threads[1], client_core);

  // Create mutexes that cover dequeueing.
  std::mutex from_disk_mutex;
  std::mutex from_cpu_mutex;

  // Aggregate all state for each resource.
  std::vector<ClientQueueState> to_cpu_queue_state{client0_cpu_queue,
                                                   client1_cpu_queue};
  auto all_to_cpu_queue_state =
      std::make_shared<AllQueueState>(to_cpu_queue_state, from_cpu_mutex);
  std::vector<ClientQueueState> to_disk_queue_state{client0_disk_queue,
                                                    client1_disk_queue};
  auto all_to_disk_queue_state =
      std::make_shared<AllQueueState>(to_disk_queue_state, from_disk_mutex);

  // Worker 0 - Disk Read
  DBWorkerOptions worker0_options{
      .input_queue_state = all_to_disk_queue_state,

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
      .task = TaskType::DISK_READ,
  };

  // Worker 1 - CPU
  DBWorkerOptions worker1_options{
      .input_queue_state = all_to_cpu_queue_state,

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
      .task = TaskType::COMPUTE,
  };

  // Worker 2 - CPU
  DBWorkerOptions worker2_options{
      .input_queue_state = all_to_cpu_queue_state,

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
      .task = TaskType::COMPUTE,
  };

  std::vector<DBWorkerOptions> worker_options{worker0_options, worker1_options,
                                              worker2_options};

  if (num_worker_threads != worker_options.size()) {
    Logger::log("Incorrect worker thread and options configuration");
    return 0;
  }

  // Start the database.
  FairDBManager db_manager{db_size};
  if (!db_manager.Init()) {
    Logger::log("DB initialization failed.");
    return 1;
  }
  db_manager.Run(worker_options, num_worker_threads, worker_cores,
                 run_duration_s, num_clients);

  // Cancel clients.
  stop.store(true);
  for (int i = 0; i < client_threads.size(); ++i) {
    client_threads[i].join();
  }
  return 0;
}