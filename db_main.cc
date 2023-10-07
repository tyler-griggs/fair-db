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
#include "db_worker.h"
#include "fair_db.h"

using namespace std;
using namespace moodycamel;

void SetThreadAffinity(std::thread &t, int core_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);         // Initialize the CPU set
  CPU_SET(core_id, &cpuset); // Set the desired core

  pthread_t native_thread_handle = t.native_handle();

  if (pthread_setaffinity_np(native_thread_handle, sizeof(cpu_set_t),
                             &cpuset) != 0) {
    std::cerr << "Error setting thread CPU affinity." << std::endl;
  }
}

void SetCurrentAffinity(int core_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);         // Initialize the CPU set
  CPU_SET(core_id, &cpuset); // Set the desired core

  if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
    std::cerr << "Error setting current CPU affinity." << std::endl;
  }
}

int main() {
  srand(time(0));
  size_t db_size = 1e9; // Number of integers in the db (cur ~= 4GB)
  size_t datatype_size = sizeof(int);

  size_t num_reads = 5000; // Number of requests per client
  size_t read_size = 1e7; // Bytes per requst.  (cur ~= 10MB)
  size_t max_outstanding = 16;
  size_t num_runs = 1;
  size_t num_clients = 2;

  for (int i = 0; i < num_runs; ++i) {
    std::vector<ReaderWriterQueue<DBRequest> *> queues;

    ReaderWriterQueue<DBRequest> q1(max_outstanding);
    queues.push_back(&q1);
    DBClient client1(1, &q1, db_size, read_size / datatype_size, num_reads);
    std::thread client_thread1 = client1.RunSequential();
    SetThreadAffinity(client_thread1, 0);

    ReaderWriterQueue<DBRequest> q2(max_outstanding);
    queues.push_back(&q2);
    DBClient client2(2, &q2, db_size, read_size / datatype_size, num_reads);
    std::thread client_thread2 = client2.RunRandom();
    SetThreadAffinity(client_thread2, 1);

    auto db = FairDB(db_size);
    db.Init();

    auto database = db.database();
    int worker_reads = num_reads/4;

    // TODO: simplify multi-workers
    std::mutex queue_mutex;

    std::thread worker_thread1([database, queues, &queue_mutex, worker_reads] {
      DBWorker(database, queues, &queue_mutex).Run(worker_reads);
    });
    SetThreadAffinity(worker_thread1, 3);

    std::thread worker_thread2([database, queues, &queue_mutex, worker_reads] {
      DBWorker(database, queues, &queue_mutex).Run(worker_reads);
    });
    SetThreadAffinity(worker_thread2, 4);


    worker_thread1.join();
    worker_thread2.join();




    // RunStats stats = worker.Run(num_reads * 2);
    // vector<vector<int>> per_client_durations;
    // per_client_durations.resize(num_clients);
    // for (const auto query : stats.query_stats) {
    //   per_client_durations[query.queue_idx].push_back(query.duration);
    // }
    // for (const auto durs : per_client_durations) {
    //   int duration_avg =
    //       std::accumulate(durs.begin(), durs.end(), 0) / durs.size();
    //   cout << "Avg: " << duration_avg << " (dummy=" << stats.dummy << ")"
    //        << endl;
    // }

    // TODO: json output
    // json output_data;
    // output_data["queries"] = stats.query_stats;

    // std::ofstream output_file("results.txt");
    // std::streambuf* cout_buffer = std::cout.rdbuf();
    // cout.rdbuf(output_file.rdbuf());

    // for (const auto query : stats.query_stats) {
    //   cout << query.queue_idx << ", ";
    // }
    // cout << endl;
    // cout << endl;
    // for (const auto durs : per_client_durations) {
    //   for (const auto d : durs) {
    //     cout << d << ", ";
    //   }
    //   cout << endl;
    //   cout << endl;
    // }
    // cout << endl;
    // std::cout.rdbuf(cout_buffer); // Restore cout's original buffer
    // output_file.close();
    

    // Then Run the clients
    // for client in clients: client.Run()

    // TODO: send message to clients to stop sending requests

    client_thread1.join();
    client_thread2.join();
  }

  return 0;
}