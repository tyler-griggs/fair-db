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

// TODO: Track the global 
int main() {
  srand(time(0));
  size_t db_size = 40e9; // Number of elements in the db (40B ints, 160GB)
  size_t datatype_size = sizeof(int);

  int num_queries = 10;                // Queries per worker until DB shuts down.
  // size_t client1_read_size = 1e9; // Bytes per request.  (cur ~= 2GB)
  // int client1_compute_ms = 100;       // Fake compute task duration.

  size_t client1_read_size = 1e9 / 3; // Bytes per request.  (cur ~= 1/3GB)
  int client1_compute_ms = 667;       // Fake compute task duration.

  size_t client2_read_size = 1e9; // Bytes per request.  (cur ~= 1GB)
  int client2_compute_ms = 167;   // Fake compute task duration.

  int num_worker_threads = 4;

  // Core IDs for clients and workers. Clients share a core.
  int client_core = 0;

  // TODO: move this from client core
  std::vector<int> worker_cores{3, 2, 1, 0};

  // Only one worker per thread (for now).
  if (worker_cores.size() < num_worker_threads) {
    cout << "Too many worker threads for given cores" << endl;
    return 0;
  }

  int num_runs = 1;
  // Max queue length for each client.
  int max_query_queue_length = num_queries * 4;
  // int num_random = 1024;  // For random read clients.

  for (int i = 0; i < num_runs; ++i) {
    std::vector<ClientQueueState> client_queue_states;
    std::vector<std::thread> client_threads;
    std::atomic<bool> stop(false);

    // Start Client 1
    auto queue1 =
        std::make_shared<ReaderWriterQueue<DBRequest>>(max_query_queue_length);
    client_queue_states.push_back(ClientQueueState(queue1));
    DBClient client1(/*client_id=*/0, queue1, db_size,
                     client1_read_size / datatype_size, client1_compute_ms);
    // TODO: before changing to random, update db_worker assumption of
    //       single read and single compute ops
    client_threads.push_back(client1.RunSequential(stop));
    SetThreadAffinity(client_threads[0], client_core);

    // Start Client 2
    auto queue2 =
        std::make_shared<ReaderWriterQueue<DBRequest>>(max_query_queue_length);
    client_queue_states.push_back(ClientQueueState(queue2));
    DBClient client2(/*client_id=*/1, queue2, db_size,
                     client2_read_size / datatype_size, client2_compute_ms);
    // TODO: before changing to random, update db_worker assumption of
    //       single read and single compute ops
    client_threads.push_back(client2.RunSequential(stop));
    // client_threads.push_back(client2.RunRandom(stop, num_random));
    SetThreadAffinity(client_threads[1], client_core);

    auto client_queue_state = std::make_shared<AllQueueState>(client_queue_states);



    // Build architecture of database workers.
    // Current: 2 read core, 2 compute core
    // Worker 1 reads and passes to Worker 2 for compute
    // Worker 3 reads and passes to Worker 4 for compute


    std::shared_ptr<std::mutex> client_queue_mutex = std::make_shared<std::mutex>();

    // Worker 1
    auto worker1_to_worker2 = std::make_shared<ReaderWriterQueue<DBRequest>>(max_query_queue_length);
    DBWorkerOptions worker1_options{
      .input_queue_state = client_queue_state,
      .input_queue_mutex = client_queue_mutex,
      .output_queue = worker1_to_worker2,
      .scheduler = 0,  // Round Robin
      .task = 0,  // Read
      .next_task = 0  // Compute
    };

    // Worker 3
    auto worker3_to_worker4 = std::make_shared<ReaderWriterQueue<DBRequest>>(max_query_queue_length);
    DBWorkerOptions worker3_options{
      .input_queue_state = client_queue_state,
      .input_queue_mutex = client_queue_mutex,
      .output_queue = worker3_to_worker4,
      .scheduler = 0,  // Round Robin
      .task = 0,  // Read
      .next_task = 0  // Compute
    };

    // Worker 2
    std::vector<ClientQueueState> worker_1_2_queue_state;
    worker_1_2_queue_state.push_back(ClientQueueState(worker1_to_worker2));
    auto all_worker_1_2_queue_state = std::make_shared<AllQueueState>(worker_1_2_queue_state);
    std::shared_ptr<std::mutex> worker_1_2_queue_mutex = std::make_shared<std::mutex>();
    DBWorkerOptions worker2_options{
      .input_queue_state = all_worker_1_2_queue_state,
      .input_queue_mutex = worker_1_2_queue_mutex,
      .output_queue = nullptr,
      .scheduler = 0,  // Round Robin
      .task = 1,  // Compute
      .next_task = -1  // None
    };

    // Worker 4
    std::vector<ClientQueueState> worker_3_4_queue_state;
    worker_3_4_queue_state.push_back(ClientQueueState(worker3_to_worker4));
    auto all_worker_3_4_queue_state = std::make_shared<AllQueueState>(worker_3_4_queue_state);
    std::shared_ptr<std::mutex> worker_3_4_queue_mutex = std::make_shared<std::mutex>();
    DBWorkerOptions worker4_options{
      .input_queue_state = all_worker_3_4_queue_state,
      .input_queue_mutex = worker_3_4_queue_mutex,
      .output_queue = nullptr,
      .scheduler = 0,  // Round Robin
      .task = 1,  // Compute
      .next_task = -1  // None
    };

    std::vector<DBWorkerOptions> worker_options{worker1_options, worker2_options,
                                                worker3_options, worker4_options};

    if (num_worker_threads != worker_options.size()) {
      cout << "Incorrect worker thread and options configuration";
      return 0;
    }

    // TODO: this is a hack to reduce num cores needed
    // Let clients fill queries
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Start database
    FairDBManager db_manager{db_size};
    db_manager.Init();
    std::vector<std::thread> worker_threads = db_manager.Run(worker_options, num_worker_threads, worker_cores, num_queries, /*num_clients=*/client_queue_states.size());

    // db_manager.Stop();
    worker_threads[0].join();
    worker_threads[2].join();

    // TODO: better method for this
    // Let the queues drain
    std::this_thread::sleep_for(std::chrono::seconds(10));
    db_manager.Stop();
    worker_threads[1].join();
    worker_threads[3].join();

    stop.store(true);
    for (int i = 0; i < client_threads.size(); ++i) {
      client_threads[i].join();
    }
  }

  return 0;
}