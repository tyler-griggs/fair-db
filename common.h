#ifndef STATE_OBJECTS_H
#define STATE_OBJECTS_H

#include <memory>
#include <vector>

#include "readerwriterqueue.h"

using namespace moodycamel;

enum class TaskType {
  DISK_READ = 0,
  COMPUTE = 1,
};

struct SingleTask {
  size_t start_idx;
  size_t read_size;
  int compute_duration;

  SingleTask(size_t start_idx, size_t read_size, size_t compute_duration = 0)
      : start_idx(start_idx), read_size(read_size),
        compute_duration(compute_duration) {}
};

// A set of read requests for the database to serve.
struct DBRequest {
  int64_t queue_start_time; // microseconds since epoch

  int client_id;
  std::vector<SingleTask> reads;

  std::vector<TaskType> task_order;
  int cur_task_idx = 0;

  DBRequest(int client_id, std::vector<SingleTask> reads)
      : client_id(client_id), reads(reads) {}
};

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
  std::vector<ClientQueueState> &client_queues;
  std::mutex &queue_mutex;
  int cur_queue_idx = -1;

  AllQueueState(std::vector<ClientQueueState> &client_queues,
                std::mutex &queue_mutex)
      : client_queues(client_queues), queue_mutex(queue_mutex) {}
};

#endif