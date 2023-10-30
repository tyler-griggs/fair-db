#ifndef STATE_OBJECTS_H
#define STATE_OBJECTS_H

#include <memory>
#include <vector>

#include "readerwriterqueue.h"

using namespace moodycamel;

// A single read request for the database to serve.
// TODO: Allow clients to make more flexible queries.
struct SingleRead {
  size_t start_idx;
  size_t read_size;
  size_t compute_duration = 0;

  SingleRead(size_t start_idx, size_t read_size, size_t compute_duration = 0)
      : start_idx(start_idx), read_size(read_size),
        compute_duration(compute_duration) {}
};

// A set of read requests for the database to serve.
struct DBRequest {
  int64_t queue_start_time; // microseconds since epoch

  int client_id;
  std::vector<SingleRead> reads;

  // TODO: clean this up
  // 0 - read, 1 - compute
  std::vector<int> task_order;
  int cur_task_idx = 0;

  DBRequest(int client_id, std::vector<SingleRead> reads)
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
  int cur_queue_idx = -1;

  AllQueueState(std::vector<ClientQueueState> &client_queues)
      : client_queues(client_queues) {}
};

#endif