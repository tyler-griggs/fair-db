#ifndef DB_WORKER_H
#define DB_WORKER_H

#include <algorithm>
#include <chrono>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <time.h>
#include <vector>

#include "db_client.h"
#include "fair_db.h"

using namespace std;

struct ClientQueueState {
  const std::shared_ptr<ReaderWriterQueue<DBRequest>> &queue;
  int service_us = 0;  // Service in microseconds given to this queue
  int cur_cpu = 0;     // Current CPU in use
  int cur_disk_bw = 0; // Current disk bw in use

  // All in normalized microseconds.
  int cpu_virtual_start = 0;
  int cpu_virtual_finish = 0;
  int disk_virtual_start = 0;
  int disk_virtual_finish = 0;

  ClientQueueState(const std::shared_ptr<ReaderWriterQueue<DBRequest>> &queue)
      : queue(queue) {}
};

struct AllQueueState {
  vector<ClientQueueState> &client_queues;
  int cur_queue_idx = -1;

  AllQueueState(vector<ClientQueueState> &client_queues)
      : client_queues(client_queues) {}
};

// TODO: histogram, nicer output format
struct QueryStats {
  int queue_idx;
  int client_id;
  int64_t start;  // microseconds since epoch
  // int64_t read_duration;  // microseconds
  int64_t total_duration;  // microseconds
};

struct RunStats {
  vector<int> read_counts;
  int total_reads = 0;
  long dummy = 0;
  vector<QueryStats> query_stats;

  RunStats(size_t num_clients, size_t num_queries) {
    read_counts = std::vector<int>(num_clients, 0);
    query_stats.resize(num_queries);
  }
};


// TODO:
// Create an options struct with these fields:
  // input queue
  // output queue (or null)
  // scheduling algorithm
  // task to perform
  // next task (or null)

struct DBWorkerOptions {
  std::shared_ptr<AllQueueState> input_queue_state;
  std::shared_ptr<std::mutex> input_queue_mutex;

  std::shared_ptr<ReaderWriterQueue<DBRequest>> output_queue;

  // 0 - RR
  // 1 - DRFQ
  // 2 - DRF
  // 3 - Fair share
  int scheduler;

  // 0 - read
  // 1 - compute
  // 2 - read+compute
  int task;

  // -1 - none
  // 0 - read
  // 1 - compute
  int next_task;
};

class DBWorker {
public:
  DBWorker(int worker_id,
           const shared_ptr<FairDB> db,
           DBWorkerOptions options)
      : worker_id_(worker_id), db_(db), queue_state_(options.input_queue_state), 
        queue_mutex_(options.input_queue_mutex), output_queue_(options.output_queue),
        scheduler_(options.scheduler), task_(options.task), next_task_(options.task) {}

  void Run(size_t num_queries, size_t num_clients, std::atomic<bool> &stop) {

    // TODO: WRONG
    // size_t num_clients = queue_state_->client_queues.size();
    auto stats = RunStats(num_clients, num_queries);

    DBRequest req(-1, {});
    while (stats.total_reads < num_queries && !stop.load()) {
      int queue_idx = PullRequestFromQueues(req);

      auto start = std::chrono::high_resolution_clock::now();
      const auto read = req.reads[0];
      if (task_ == 0 || task_ == 2) {
        /* ====== READ PHASE ======= */
        // TODO: currently assuming a single read
        // cout << "Executing read phase on worker " << worker_id_ << endl;
        db_->Read(stats.dummy, read.start_idx, read.read_size);
      }
      if (task_ == 1 || task_ == 2) {
        // cout << "Executing compute phase on worker " << worker_id_ << endl;
        /* ====== COMPUTE PHASE ======= */
        if (read.compute_duration > 0) {
          auto compute_start = std::chrono::high_resolution_clock::now();
          auto timeout = std::chrono::milliseconds(read.compute_duration);
          volatile int vol_dummy = 0;
          while (std::chrono::high_resolution_clock::now() - compute_start < timeout) {
            for (int i = 0; i < 500000; ++i) {
              vol_dummy += i;
            }
            for (int i = 0; i < 500000; ++i) {
              vol_dummy -= i;
            }
          }
          stats.dummy += vol_dummy;
        }
      }



      // int64_t read_duration;
      // for (const auto read : req.reads) {
      //   // cout << "read size: " << read.read_size << endl;
      //   // cout << "start: " << read.start_idx << endl;
      //   // cout << "end: " << read.start_idx + read.read_size << endl;

      //   /* ====== READ PHASE ======= */
      //   db_->Read(stats.dummy, read.start_idx, read.read_size);
      //   auto read_end = std::chrono::high_resolution_clock::now();
      //   read_duration = std::chrono::duration_cast<std::chrono::microseconds>(read_end - start).count();

      //   /* ====== COMPUTE PHASE ======= */
      //   if (read.compute_duration > 0) {
      //     auto compute_start = std::chrono::high_resolution_clock::now();
      //     auto timeout = std::chrono::milliseconds(read.compute_duration);
      //     volatile int vol_dummy = 0;
      //     while (std::chrono::high_resolution_clock::now() - compute_start < timeout) {
      //       for (int i = 0; i < 500000; ++i) {
      //         vol_dummy += i;
      //       }
      //       for (int i = 0; i < 500000; ++i) {
      //         vol_dummy -= i;
      //       }
      //     }
      //     stats.dummy += vol_dummy;
      //   }
      // }
      auto stop = std::chrono::high_resolution_clock::now();
      const int64_t total_duration =
          std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
      // cout << "dur: " << total_duration.count() << endl;;

      auto time_since_epoch_us =
          std::chrono::duration_cast<std::chrono::microseconds>(
              start.time_since_epoch())
              .count();

      stats.query_stats[stats.total_reads] =
          QueryStats{.queue_idx = queue_idx,
                     .client_id = req.client_id,
                     .start = time_since_epoch_us,
                    //  .read_duration = read_duration,
                     .total_duration = total_duration};
      // ++stats.read_counts[queue_idx];
      ++stats.read_counts[req.client_id];
      ++stats.total_reads;

      // TODO: huge update needed to this to separate queue_idx and client_id
      PushResultsToInputQueues(queue_idx, total_duration);

      PassToNextQueue(req);
    }
    DumpStats(stats, num_clients);
  }

private:
  int worker_id_;
  const shared_ptr<FairDB> db_;

  std::shared_ptr<AllQueueState> queue_state_;
  std::shared_ptr<std::mutex> queue_mutex_;

  std::shared_ptr<ReaderWriterQueue<DBRequest>> output_queue_;
  int scheduler_;
  int task_;
  int next_task_;

  // TODO: improve multi-threading. Currently multiple threads will choose
  // the same least-service queue
  int MinimumServiceScheduling() {
    int min_service = std::numeric_limits<int>::max();
    int min_idx;
    for (int i = 0; i < queue_state_->client_queues.size(); ++i) {
      const auto &q = queue_state_->client_queues[i];
      if (q.service_us < min_service) {
        min_service = q.service_us;
        min_idx = i;
      }
    }
    return min_idx;
  }

  int DRFQScheduling() {
    // TODO: configure this
    int delta = 0;

    // Client 1
    // 1. S(p) for CPU
    // a. V(a) for CPU <---- for now this is arrival to front of queue
      // i. S(p) for Client 2 CPU
      // i. S(p)-Delta for Client 2 Disk
    
    // DONE
    // b. B1(p^k-1) for CPU
      // i. F(p^k-1) for CPU
      // ii. F(p^k-1)-delta for Disk

    // Client1 - CPU
    // TODO: make sure other client's virtual start is updated when idle
    // TODO: base case when no packets are processed
    int client1_cpu_v =
        std::max(queue_state_->client_queues[1].cpu_virtual_start,
                 queue_state_->client_queues[1].disk_virtual_start - delta);

    // TODO: Currently assuming we know op cost: we should update this finish
    //       time as soon as the op starts.
    int client1_cpu_b1 = 
        std::max(queue_state_->client_queues[0].cpu_virtual_finish, 
                 queue_state_->client_queues[0].disk_virtual_finish - delta);

    int client1_cpu_s = std::max(client1_cpu_v, client1_cpu_b1);


    // Client1 - Disk
    int client1_disk_v =
        std::max(queue_state_->client_queues[1].disk_virtual_start,
                 queue_state_->client_queues[1].cpu_virtual_start - delta);
    int client1_disk_b1 = 
        std::max(queue_state_->client_queues[0].disk_virtual_finish, 
                 queue_state_->client_queues[0].cpu_virtual_finish - delta);
    int client1_disk_s = std::max(client1_disk_v, client1_disk_b1);

    int client1_start = std::max(client1_cpu_s, client1_disk_s);
    

    // Client2 - CPU
    int client2_cpu_v =
        std::max(queue_state_->client_queues[0].cpu_virtual_start,
                 queue_state_->client_queues[0].disk_virtual_start - delta);
    int client2_cpu_b1 = 
        std::max(queue_state_->client_queues[1].cpu_virtual_finish, 
                 queue_state_->client_queues[1].disk_virtual_finish - delta);
    int client2_cpu_s = std::max(client2_cpu_v, client2_cpu_b1);


    // Client2 - Disk
    int client2_disk_v =
        std::max(queue_state_->client_queues[0].disk_virtual_start,
                 queue_state_->client_queues[0].cpu_virtual_start - delta);
    int client2_disk_b1 = 
        std::max(queue_state_->client_queues[1].disk_virtual_finish, 
                 queue_state_->client_queues[1].cpu_virtual_finish - delta);
    int client2_disk_s = std::max(client2_disk_v, client2_disk_b1);

    int client2_start = std::max(client2_cpu_s, client2_disk_s);
    
    // Choose the client with the earliest start time.
    int next_client_idx = client1_start < client2_start ? 0 : 1;

    // For the chosen client, update the start time for each resource. 
    // TODO: Currently assuming we know op cost: update finish times too.
    if (next_client_idx == 0) {
      // Task A: 1/3GB read (out of 3GB/s), 2/3s CPU (out of 3CPUs)
      queue_state_->client_queues[0].disk_virtual_start = client1_disk_s;
      queue_state_->client_queues[0].disk_virtual_finish = client1_disk_s + 111111;  // 1/9s
      queue_state_->client_queues[0].cpu_virtual_start = client1_cpu_s;
      queue_state_->client_queues[0].cpu_virtual_finish = client1_cpu_s + 222222; // 2/9s
    } else {
      // Task B: 1GB read (out of 3GB/s), 1/6s CPU (out of 3CPUs)
      queue_state_->client_queues[1].disk_virtual_start = client2_disk_s;
      queue_state_->client_queues[1].disk_virtual_finish = client2_disk_s + 333333; // 1/3s
      queue_state_->client_queues[1].cpu_virtual_start = client2_cpu_s;
      queue_state_->client_queues[1].cpu_virtual_finish = client2_cpu_s + 55555;  // 1/18s
    }
    return next_client_idx;
  }

  // Based on current usage (ie, outstanding ops)
  int DRFScheduling() {
    const float max_cpu = 18.0;
    const float max_disk_bw = 9.0;

    float client1_disk_ratio =
        queue_state_->client_queues[0].cur_disk_bw / max_disk_bw;
    float client1_cpu_ratio = queue_state_->client_queues[0].cur_cpu / max_cpu;
    float client1_max_ratio = std::max(client1_cpu_ratio, client1_disk_ratio);

    float client2_disk_ratio =
        queue_state_->client_queues[1].cur_disk_bw / max_disk_bw;
    float client2_cpu_ratio = queue_state_->client_queues[1].cur_cpu / max_cpu;
    float client2_max_ratio = std::max(client2_cpu_ratio, client2_disk_ratio);

    int idx = client2_max_ratio < client1_max_ratio;

    // cout << "Before: " << idx << ": "
    //      << queue_state_->client_queues[0].cur_disk_bw << ", "
    //      << queue_state_->client_queues[0].cur_cpu << ", "
    //      << queue_state_->client_queues[1].cur_disk_bw << ","
    //      << queue_state_->client_queues[1].cur_cpu << endl;
    if (idx == 0) {
      // Task A: 1/3GB read, 2/3s CPU
      queue_state_->client_queues[0].cur_disk_bw += 1; // out of 9
      queue_state_->client_queues[0].cur_cpu += 4;     // out of 18
    } else {
      // Task B: 1GB read, 1/6s CPU
      queue_state_->client_queues[1].cur_disk_bw += 3; // out of 9
      queue_state_->client_queues[1].cur_cpu += 1;     // out of 18
    }
    // cout << "After: " << idx << ": "
    //      << queue_state_->client_queues[0].cur_disk_bw << ", "
    //      << queue_state_->client_queues[0].cur_cpu << ", "
    //      << queue_state_->client_queues[1].cur_disk_bw << ","
    //      << queue_state_->client_queues[1].cur_cpu << endl;
    return idx;
  }

  int RoundRobinScheduling() {
    return (queue_state_->cur_queue_idx + 1) %
           queue_state_->client_queues.size();
  }

  // TODO: DWRR - give each queue quantum each round.
  // int DeficitWeightedRoundRobin(DBRequest &request) {}


  // TODO: release lock earlier. Need to remove assumption that
  //       there are always backlogged requests.
  int PullRequestFromQueues(DBRequest &request) {
    std::lock_guard<std::mutex> lock(*queue_mutex_);

    // TODO: so gross, clean this up
    // 0 - RR
    // 1 - DRFQ
    // 2 - DRF
    // 3 - Fair share
    if (scheduler_ == 0) {
      queue_state_->cur_queue_idx = RoundRobinScheduling();
    } else if (scheduler_ == 3) {
      queue_state_->cur_queue_idx = MinimumServiceScheduling();
    } else {
      cout << "Using default RR scheduler." << endl;
      queue_state_->cur_queue_idx = RoundRobinScheduling();
    }

    // queue_state_->cur_queue_idx = MinimumServiceScheduling();
    // queue_state_->cur_queue_idx = RoundRobinScheduling();
    // queue_state_->cur_queue_idx = DRFScheduling();
    // queue_state_->cur_queue_idx = DRFQScheduling();

    while (!queue_state_->client_queues[queue_state_->cur_queue_idx]
                .queue->try_dequeue(request)) {
      // std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    return queue_state_->cur_queue_idx;
  }

  void PushResultsToInputQueues(int queue_idx, int service_duration_us) {
    std::lock_guard<std::mutex> lock(*queue_mutex_);
    queue_state_->client_queues[queue_idx].service_us += service_duration_us;

    if (queue_idx == 0) {
      // Task A: 1/3GB read, 2/3s CPU
      queue_state_->client_queues[0].cur_disk_bw -= 1; // out of 9
      queue_state_->client_queues[0].cur_cpu -= 4;     // out of 18
    } else {
      // Task B: 1GB read, 1/6s CPU
      queue_state_->client_queues[1].cur_disk_bw -= 3; // out of 9
      queue_state_->client_queues[1].cur_cpu -= 1;     // out of 18
    }
  }

  void PassToNextQueue(DBRequest request) {
    if (output_queue_ == nullptr) {
      return;
    }
    while (!output_queue_->try_enqueue(request)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  void DumpStats(RunStats &stats, int num_clients) {
    vector<vector<int64_t>> per_client_durations;
    per_client_durations.resize(num_clients);
    for (const auto query : stats.query_stats) {
      // per_client_durations[query.queue_idx].push_back(query.total_duration);
      per_client_durations[query.client_id].push_back(query.total_duration);
    }

    vector<vector<int64_t>> per_client_starts;
    per_client_starts.resize(num_clients);
    for (const auto query : stats.query_stats) {
      // per_client_starts[query.queue_idx].push_back(query.start);
      per_client_starts[query.client_id].push_back(query.start);
    }

    for (int i = 0; i < per_client_durations.size(); ++i) {
      const auto durs = per_client_durations[i];
      int duration_avg =
          std::accumulate(durs.begin(), durs.end(), 0) / durs.size();
      cout << "Avg: " << i << " - " << duration_avg << " (dummy=" << stats.dummy
           << ")" << endl;
    }

    std::ofstream output_file("results/results_" + std::to_string(worker_id_) +
                              ".txt");
    output_file << "ExecutionOrder:";
    for (const auto query : stats.query_stats) {
      // output_file << query.queue_idx << ", ";
      output_file << query.client_id << ", ";
    }
    output_file << endl;
    for (const auto durs : per_client_durations) {
      output_file << "ClientDurations:";
      for (const auto d : durs) {
        output_file << d << ", ";
      }
      output_file << endl;
    }

    for (const auto starts : per_client_starts) {
      output_file << "ClientStarts:";
      for (const auto s : starts) {
        output_file << s << ", ";
      }
      output_file << endl;
    }
    output_file.close();
  }
};

#endif