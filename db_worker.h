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
  int64_t start; // microseconds since epoch
  // int64_t read_duration;  // microseconds
  int64_t total_duration; // microseconds
};

struct RunStats {
  vector<int> read_counts;
  int total_reads = 0;
  long dummy = 0;
  // TODO: this is dynamically updated in size
  vector<QueryStats> query_stats;

  RunStats(size_t num_clients) {
    read_counts = std::vector<int>(num_clients, 0);
  }
};

// TODO:
// Create an options struct with these fields:
// input queue
// output queue (or null)
// scheduling algorithm
// task to perform
// next task (or null)

enum class SchedulerType {
  NONE = 0,
  ROUND_ROBIN = 1,
  FIFO = 2,
  DRFQ = 3,
  PER_RESOURCE_FAIR = 4
};

struct DBWorkerOptions {
  std::shared_ptr<AllQueueState> input_queue_state;
  std::shared_ptr<std::mutex> input_queue_mutex;

  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_cpu_queue0;
  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_cpu_queue1;
  std::shared_ptr<std::mutex> to_cpu_mutex;

  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_disk_queue0;
  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_disk_queue1;
  std::shared_ptr<std::mutex> to_disk_mutex;

  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_completion_queue0;
  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_completion_queue1;
  std::shared_ptr<std::mutex> to_completion_mutex;

  // int scheduler;

  SchedulerType scheduler;

  // 0 - read
  // 1 - compute
  // 2 - read+compute
  int task;
};

class DBWorker {
public:
  DBWorker(int worker_id, const shared_ptr<FairDB> db, DBWorkerOptions options)
      : worker_id_(worker_id), db_(db), options_(options), queue_state_(options.input_queue_state),
        scheduler_(options.scheduler), task_(options.task) {
          cout << "DB worker " << worker_id_ << " using ";
          switch (scheduler_) {
            case SchedulerType::NONE:
              cout << "NONE";
              break;
            case SchedulerType::ROUND_ROBIN:
              cout << "ROUND_ROBIN";
              break;
            case SchedulerType::FIFO:
              cout << "FIFO";
              break;
            case SchedulerType::DRFQ:
              cout << "DRFQ";
              break;
            case SchedulerType::PER_RESOURCE_FAIR:
              cout << "PER_RESOURCE_FAIR";
              break;
          }
          cout << endl;
        }

  void Run(size_t num_clients, std::atomic<bool> &stop) {
    // TODO: WRONG
    // size_t num_clients = queue_state_->client_queues.size();
    auto stats = RunStats(num_clients);

    DBRequest req(-1, {});
    while (!stop.load()) {
      int queue_idx = PullRequestFromQueues(req, stop);
      // cout << "Worker " << worker_id_ << " pulled req at queue idx " << queue_idx << endl;
      if (queue_idx == -1) {
        break;
      }

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
          while (std::chrono::high_resolution_clock::now() - compute_start <
                 timeout) {
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

      auto end = std::chrono::high_resolution_clock::now();
      const int64_t total_duration =
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count();
      // cout << "dur: " << total_duration.count() << endl;;

      auto time_since_epoch_us =
          std::chrono::duration_cast<std::chrono::microseconds>(
              start.time_since_epoch())
              .count();

      stats.query_stats.push_back(
          QueryStats{.queue_idx = queue_idx,
                     .client_id = req.client_id,
                     .start = time_since_epoch_us,
                     //  .read_duration = read_duration,
                     .total_duration = total_duration});
      // ++stats.read_counts[queue_idx];
      ++stats.read_counts[req.client_id];
      ++stats.total_reads;

      // TODO: huge update needed to this to separate queue_idx and client_id
      // cout << "Worker " << worker_id_ << " finished task" << endl;
      PushResultsToInputQueues(queue_idx, total_duration);
      PassToNextQueue(req, stop);
    }
    DumpStats(stats, num_clients);
  }

private:
  int worker_id_;
  const shared_ptr<FairDB> db_;
  const DBWorkerOptions& options_;

  std::shared_ptr<AllQueueState> queue_state_;
  // std::shared_ptr<std::mutex> queue_mutex_;
  // std::shared_ptr<std::mutex> output_queue_mutex_;

  // std::shared_ptr<ReaderWriterQueue<DBRequest>> output_queue1_;
  // std::shared_ptr<ReaderWriterQueue<DBRequest>> output_queue2_;
  SchedulerType scheduler_;
  int task_;

  // TODO: improve multi-threading. Currently multiple threads will choose
  // the same least-service queue
  int MinimumServiceScheduling(std::atomic<bool> &stop) {
    int min_service = std::numeric_limits<int>::max();
    int min_idx = -1;
    while (!stop.load()) {
      for (int i = 0; i < queue_state_->client_queues.size(); ++i) {
        if (queue_state_->client_queues[i].queue->size_approx() == 0) {
          continue;
        }
        const auto &q = queue_state_->client_queues[i];
        if (q.service_us < min_service) {
          min_service = q.service_us;
          min_idx = i;
        }
      }
      if (min_idx != -1) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
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
      queue_state_->client_queues[0].disk_virtual_finish =
          client1_disk_s + 111111; // 1/9s
      queue_state_->client_queues[0].cpu_virtual_start = client1_cpu_s;
      queue_state_->client_queues[0].cpu_virtual_finish =
          client1_cpu_s + 222222; // 2/9s
    } else {
      // Task B: 1GB read (out of 3GB/s), 1/6s CPU (out of 3CPUs)
      queue_state_->client_queues[1].disk_virtual_start = client2_disk_s;
      queue_state_->client_queues[1].disk_virtual_finish =
          client2_disk_s + 333333; // 1/3s
      queue_state_->client_queues[1].cpu_virtual_start = client2_cpu_s;
      queue_state_->client_queues[1].cpu_virtual_finish =
          client2_cpu_s + 55555; // 1/18s
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

  int RoundRobinScheduling(std::atomic<bool> &stop) {
    int idx = queue_state_->cur_queue_idx;
    while (!stop.load()) {
      idx = (idx + 1) %
           queue_state_->client_queues.size();
      if (queue_state_->client_queues[idx].queue->peek() != nullptr) {
        return idx;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return -1;
  }

  // TODO: Implement. May need some timestamp?
  int FIFOScheduling(std::atomic<bool> &stop) {
    int64_t min_start_time = std::numeric_limits<int64_t>::max();
    int min_idx = -1;
    while (!stop.load()) {
      for (int i = 0; i < queue_state_->client_queues.size(); ++i) {
        const auto &q = queue_state_->client_queues[i];
        const auto peek = q.queue->peek();
        if (peek != nullptr && peek->queue_start_time < min_start_time) {
          min_start_time = peek->queue_start_time;
          min_idx = i;
        }
      }
      if (min_idx != -1) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return min_idx;
  }

  // TODO: DWRR - give each queue quantum each round.
  // int DeficitWeightedRoundRobin(DBRequest &request) {}

  // TODO: release lock earlier. Need to remove assumption that
  //       there are always backlogged requests.
  int PullRequestFromQueues(DBRequest &request, std::atomic<bool> &stop) {
    // std::lock_guard<std::mutex> lock(*options_.input_queue_mutex);

    int idx = -1;
    switch (scheduler_) {
      case SchedulerType::NONE:
        cout << "No scheduler specified for worker " << worker_id_ << ". Exiting." << endl;
        return -1;
      case SchedulerType::ROUND_ROBIN:
        idx = RoundRobinScheduling(stop);
        break;
      case SchedulerType::FIFO:
        idx = FIFOScheduling(stop);
        break;
      case SchedulerType::DRFQ:
        idx = DRFQScheduling();
        break;
      case SchedulerType::PER_RESOURCE_FAIR:
        idx = MinimumServiceScheduling(stop);
        break;
    }
    if (idx == -1) {
      return -1;
    }
    queue_state_->cur_queue_idx = idx;
    // queue_state_->cur_queue_idx = MinimumServiceScheduling();
    // queue_state_->cur_queue_idx = RoundRobinScheduling();
    // queue_state_->cur_queue_idx = DRFScheduling();
    while (!queue_state_->client_queues[idx]
                .queue->try_dequeue(request) && !stop.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return queue_state_->cur_queue_idx;
  }

  void PushResultsToInputQueues(int queue_idx, int service_duration_us) {
    // std::lock_guard<std::mutex> lock(*options_.input_queue_mutex);
    // if (queue_idx == 0) {
    //   queue_state_->client_queues[queue_idx].service_us += worker_id_ == 0 ? 
    // } else {

    // }
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

  void PassToNextQueue(DBRequest request, std::atomic<bool>& stop) {
    request.cur_task_idx++;
    std::shared_ptr<ReaderWriterQueue<DBRequest>> queue;
    std::shared_ptr<std::mutex> mutex;
    if (request.cur_task_idx >= request.task_order.size()) {
      queue = request.client_id == 0 ? options_.to_completion_queue0 : options_.to_completion_queue1;
      mutex = options_.to_completion_mutex;
    } else if (request.task_order[request.cur_task_idx] == 0) {
      queue = request.client_id == 0 ? options_.to_disk_queue0 : options_.to_disk_queue1;
      mutex = options_.to_disk_mutex;
    } else {
      queue = request.client_id == 0 ? options_.to_cpu_queue0 : options_.to_cpu_queue1;
      mutex = options_.to_cpu_mutex;
    }
    request.queue_start_time =  std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    std::lock_guard<std::mutex> lock(*mutex);
    while (!queue->try_enqueue(request) && !stop.load()) {
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