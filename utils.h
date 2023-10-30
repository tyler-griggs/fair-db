#ifndef UTILS_H
#define UTILS_H

#include <chrono>
#include <iomanip>
#include <iostream>
#include <pthread.h>
#include <sched.h>

void WriteToLog(std::string output) {
  std::time_t currentTime =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::tm *localTime = std::localtime(&currentTime);
  std::cout << std::put_time(localTime, "[%Y-%m-%d %H:%M:%S] ");
  std::cout << output;
  std::cout << std::endl;
}

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
