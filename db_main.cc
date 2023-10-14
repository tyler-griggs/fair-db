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
using namespace moodycamel;

int main() {
  srand(time(0));
  size_t db_size = 20e9; // Number of elements in the db (cur ~= 80GB)
  size_t datatype_size = sizeof(int);

  int num_queries = 2;  // Queries per worker until DB shuts down.
  size_t client1_read_size = 1e9 / 3; // Bytes per request.  (cur ~= 1/3GB)
  int client1_compute_ms = 667;  // Fake compute task duration.

  size_t client2_read_size = 1e9; // Bytes per request.  (cur ~= 1GB)
  int client2_compute_ms = 167;  // Fake compute task duration.

  int num_worker_threads = 1;

  // Core IDs for clients and workers. Clients share a core.
  int client_core = 0;
  std::vector<int> worker_cores{1, 2, 3};

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
    std::vector<shared_ptr<ReaderWriterQueue<DBRequest>>> queues;
    std::vector<std::thread> client_threads;
    std::atomic<bool> stop(false);

    // Start Client 1
    auto queue1 = std::make_shared<ReaderWriterQueue<DBRequest>>(max_query_queue_length);
    queues.push_back(queue1);
    DBClient client1(/*client_id=*/1, queue1, db_size, client1_read_size / datatype_size,
                     client1_compute_ms);
    client_threads.push_back(client1.RunSequential(stop));
    SetThreadAffinity(client_threads[0], client_core);

    // Start Client 2
    auto queue2 = std::make_shared<ReaderWriterQueue<DBRequest>>(max_query_queue_length);
    queues.push_back(queue2);
    DBClient client2(/*client_id=*/2, queue2, db_size, client2_read_size / datatype_size,
                     client2_compute_ms);
    client_threads.push_back(client2.RunSequential(stop));
    // client_threads.push_back(client2.RunSequential(stop, num_random));
    SetThreadAffinity(client_threads[1], client_core);

    // Start database
    auto db_manager = FairDBManager(db_size);
    db_manager.Init();
    db_manager.Run(queues, num_worker_threads, worker_cores, num_queries);

    stop.store(true);
    for (int i = 0; i < client_threads.size(); ++i) {
      client_threads[i].join();
    }
  }

  return 0;
}