#ifndef UTILS_H
#define UTILS_H

#include <pthread.h>
#include <sched.h>

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

#endif
