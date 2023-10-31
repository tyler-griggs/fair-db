#ifndef SCHEDULERS_H
#define SCHEDULERS_H

#include <memory>
#include <thread>

#include "common.h"

enum class SchedulerType {
  NONE = 0,
  ROUND_ROBIN = 1,
  FIFO = 2,
  DRFQ = 3,
  PER_RESOURCE_FAIR = 4
};

std::string SchedulerToString(SchedulerType scheduler) {
  switch (scheduler) {
  case SchedulerType::ROUND_ROBIN:
    return "ROUND_ROBIN";
  case SchedulerType::FIFO:
    return "FIFO";
  case SchedulerType::DRFQ:
    return "DRFQ";
  case SchedulerType::PER_RESOURCE_FAIR:
    return "PER_RESOURCE_FAIR";
  default:
    return "NONE";
  }
}

class TaskScheduler {
public:
  virtual int GetNextQueueIdx(std::shared_ptr<AllQueueState> &queue_state,
                              std::atomic<bool> &stop) = 0;
};

class RoundRobinScheduler : public TaskScheduler {
  int GetNextQueueIdx(std::shared_ptr<AllQueueState> &queue_state,
                      std::atomic<bool> &stop) {
    int idx = queue_state->cur_queue_idx;
    while (!stop.load()) {
      idx = (idx + 1) % queue_state->client_queues.size();
      if (queue_state->client_queues[idx].queue->peek() != nullptr) {
        return idx;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return -1;
  }
};

class MinimumServiceScheduler : public TaskScheduler {
  // TODO: Improve multi-threading. Currently multiple threads will choose
  // the same least-service queue.
  int GetNextQueueIdx(std::shared_ptr<AllQueueState> &queue_state,
                      std::atomic<bool> &stop) {
    // const int memory_window_size_us = 120 * 1000 * 1000;
    const int memory_window_size_us = -1;
    int min_service = std::numeric_limits<int>::max();
    int min_idx = -1;
    while (!stop.load()) {
      for (int i = 0; i < queue_state->client_queues.size(); ++i) {
        if (queue_state->client_queues[i].queue->size_approx() == 0) {
          continue;
        }
        const auto &q = queue_state->client_queues[i];
        if (q.service_us < min_service) {
          min_service = q.service_us;
          min_idx = i;
        }
      }
      if (min_idx == -1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }
      if (memory_window_size_us != -1) {
        // Bound the memory to the window size.
        int other_idx = 1 - min_idx;
        int other_service_us = queue_state->client_queues[other_idx].service_us;
        queue_state->client_queues[min_idx].service_us =
            std::max(queue_state->client_queues[min_idx].service_us,
                     other_service_us - memory_window_size_us);
      }
      break;
    }
    return min_idx;
  }
};

class FIFOScheduler : public TaskScheduler {
  int GetNextQueueIdx(std::shared_ptr<AllQueueState> &queue_state,
                      std::atomic<bool> &stop) {
    int64_t min_start_time = std::numeric_limits<int64_t>::max();
    int min_idx = -1;
    while (!stop.load()) {
      for (int i = 0; i < queue_state->client_queues.size(); ++i) {
        const auto &q = queue_state->client_queues[i];
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
};

// Note: work in progress
class DRFQScheduler : public TaskScheduler {
  int GetNextQueueIdx(std::shared_ptr<AllQueueState> &queue_state,
                      std::atomic<bool> &stop) {
    // TODO: configure this
    int delta = 0;

    // Client 1
    // 1. S(p) for CPU
    // a. V(a) for CPU <---- for now this is arrival to front of queue
    // i. S(p) for Client 2 CPU
    // i. S(p)-Delta for Client 2 Disk

    // b. B1(p^k-1) for CPU
    // i. F(p^k-1) for CPU
    // ii. F(p^k-1)-delta for Disk

    // Client1 - CPU
    // TODO: Make sure other client's virtual start is updated when idle.
    // TODO: Establish base case when no queries are processed.
    int client1_cpu_v =
        std::max(queue_state->client_queues[1].cpu_virtual_start,
                 queue_state->client_queues[1].disk_virtual_start - delta);

    // TODO: Currently assuming we know op cost: we should update this finish
    //       time as soon as the op starts.
    int client1_cpu_b1 =
        std::max(queue_state->client_queues[0].cpu_virtual_finish,
                 queue_state->client_queues[0].disk_virtual_finish - delta);
    int client1_cpu_s = std::max(client1_cpu_v, client1_cpu_b1);

    // Client1 - Disk
    int client1_disk_v =
        std::max(queue_state->client_queues[1].disk_virtual_start,
                 queue_state->client_queues[1].cpu_virtual_start - delta);
    int client1_disk_b1 =
        std::max(queue_state->client_queues[0].disk_virtual_finish,
                 queue_state->client_queues[0].cpu_virtual_finish - delta);
    int client1_disk_s = std::max(client1_disk_v, client1_disk_b1);

    int client1_start = std::max(client1_cpu_s, client1_disk_s);

    // Client2 - CPU
    int client2_cpu_v =
        std::max(queue_state->client_queues[0].cpu_virtual_start,
                 queue_state->client_queues[0].disk_virtual_start - delta);
    int client2_cpu_b1 =
        std::max(queue_state->client_queues[1].cpu_virtual_finish,
                 queue_state->client_queues[1].disk_virtual_finish - delta);
    int client2_cpu_s = std::max(client2_cpu_v, client2_cpu_b1);

    // Client2 - Disk
    int client2_disk_v =
        std::max(queue_state->client_queues[0].disk_virtual_start,
                 queue_state->client_queues[0].cpu_virtual_start - delta);
    int client2_disk_b1 =
        std::max(queue_state->client_queues[1].disk_virtual_finish,
                 queue_state->client_queues[1].cpu_virtual_finish - delta);
    int client2_disk_s = std::max(client2_disk_v, client2_disk_b1);

    int client2_start = std::max(client2_cpu_s, client2_disk_s);

    // Choose the client with the earliest start time.
    int next_client_idx = client1_start < client2_start ? 0 : 1;

    // For the chosen client, update the start time for each resource.
    // TODO: Currently assuming we know op cost: update finish times too.
    if (next_client_idx == 0) {
      // Task A: 1/3GB read (out of 3GB/s), 2/3s CPU (out of 3CPUs)
      queue_state->client_queues[0].disk_virtual_start = client1_disk_s;
      queue_state->client_queues[0].disk_virtual_finish =
          client1_disk_s + 111111; // 1/9s
      queue_state->client_queues[0].cpu_virtual_start = client1_cpu_s;
      queue_state->client_queues[0].cpu_virtual_finish =
          client1_cpu_s + 222222; // 2/9s
    } else {
      // Task B: 1GB read (out of 3GB/s), 1/6s CPU (out of 3CPUs)
      queue_state->client_queues[1].disk_virtual_start = client2_disk_s;
      queue_state->client_queues[1].disk_virtual_finish =
          client2_disk_s + 333333; // 1/3s
      queue_state->client_queues[1].cpu_virtual_start = client2_cpu_s;
      queue_state->client_queues[1].cpu_virtual_finish =
          client2_cpu_s + 55555; // 1/18s
    }
    return next_client_idx;
  }
};

std::unique_ptr<TaskScheduler> GetScheduler(SchedulerType scheduler_type) {
  switch (scheduler_type) {
  case SchedulerType::ROUND_ROBIN:
    return std::make_unique<RoundRobinScheduler>();
  case SchedulerType::FIFO:
    return std::make_unique<FIFOScheduler>();
  case SchedulerType::DRFQ:
    return std::make_unique<DRFQScheduler>();
  case SchedulerType::PER_RESOURCE_FAIR:
    return std::make_unique<MinimumServiceScheduler>();
  default:
    return nullptr;
  }
}
#endif