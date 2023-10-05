#include <chrono>
#include <iostream>
#include <numeric>
#include <time.h>
#include <vector>

#include "db_client.h"
#include "fair_db.h"

using namespace std;
using namespace moodycamel;

int main() {
  srand(time(0));
  size_t db_size = 2e9; // Number of integers in the db (cur ~= 8GB)
  size_t datatype_size = sizeof(int);

  // TODO: These are client
  size_t num_reads = 1;   // Number of requests per client
  size_t read_size = 1e7; // Bytes per requst.  (cur ~= 10MB)
  size_t max_outstanding = 10;
  size_t num_runs = 1;

  std::vector<ReaderWriterQueue<DBRequest> *> queues;

  ReaderWriterQueue<DBRequest> q1(max_outstanding);
  queues.push_back(&q1);
  DBClient client1(1, &q1, db_size, read_size / datatype_size, num_reads);
  std::thread client_thread1 = client1.RunSequential();

  ReaderWriterQueue<DBRequest> q2(max_outstanding);
  queues.push_back(&q2);
  DBClient client2(2, &q2, db_size, read_size / datatype_size, num_reads);
  std::thread client_thread2 = client2.RunRandom();

  auto db = FairDB(db_size, queues);
  db.Init();

  // Run the DB
  // auto results = db.Run();
  RunStats stats = db.Run(num_reads * 2);
  for (const auto durs : stats.query_durations) {
    int duration_avg =
        std::accumulate(durs.begin(), durs.end(), 0) / durs.size();
    cout << "Avg: " << duration_avg << " (dummy=" << stats.dummy << ")" << endl;
  }
  cout << endl;

  // Then Run the clients
  // for client in clients: client.Run()

  client_thread1.join();
  client_thread2.join();

  return 0;
}