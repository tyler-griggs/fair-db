#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <numeric>
#include <pthread.h>
#include <sched.h>
#include <time.h>
#include <vector>
// #include <nlohmann/json.hpp>
// using json = nlohmann::json;

#include "db_client.h"
// #include "db_worker.h"
// #include "fair_db.h"
#include "fair_db_manager.h"
#include "utils.h"

using namespace std;
using namespace moodycamel;

int main() {
  srand(time(0));
  size_t db_size = 40e9; // Number of integers in the db (cur ~= 160GB)
  // size_t db_size = 4e9; // Number of integers in the db (cur ~= 16GB)
  size_t datatype_size = sizeof(int);

  size_t num_queries = 4; // Number of requests per worker until DB shuts down.
  size_t client1_read_size = 1e9; // Bytes per request.  (cur ~= 8GB)
  // size_t client2_read_size = 1e7;             // Bytes per request.  (cur ~=
  // 10MB)
  size_t client2_read_size = 1e9; // Bytes per request.  (cur ~= 1GB)
  size_t num_worker_threads = 2;
  size_t client_timeout_seconds = 120; // Duration until clients shut down.

  size_t max_outstanding = 16;
  size_t num_runs = 1;
  size_t num_clients = 1;

  for (int i = 0; i < num_runs; ++i) {
    std::vector<ReaderWriterQueue<DBRequest> *> queues;
    std::vector<std::thread> client_threads;

    ReaderWriterQueue<DBRequest> q1(max_outstanding);
    queues.push_back(&q1);
    DBClient client1(1, &q1, db_size, client1_read_size / datatype_size,
                     client_timeout_seconds, /*compute_duration_ms=*/0);
    client_threads.push_back(client1.RunSequential());
    SetThreadAffinity(client_threads[0], 0);
    // client_threads.push_back(client_thread1);

    // ReaderWriterQueue<DBRequest> q2(max_outstanding);
    // queues.push_back(&q2);
    // DBClient client2(2, &q2, db_size, client2_read_size / datatype_size,
    //                  client_timeout_seconds, /*compute_duration_ms=*/0);
    // // std::thread client_thread2 = client2.RunRandom();
    // client_threads.push_back(client2.RunSequential());
    // SetThreadAffinity(client_threads[1], 0);
    // client_threads.push_back(client_thread2);

    auto db_manager = FairDBManager(db_size);
    db_manager.Init();
    db_manager.Run(queues, num_worker_threads, num_queries);

    // TODO: send message to clients to stop sending requests
    for (int i = 0; i < client_threads.size(); ++i) {
      client_threads[i].join();
    }
  }

  return 0;
}