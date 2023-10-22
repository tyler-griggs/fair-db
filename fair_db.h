#ifndef FAIR_DB_H
#define FAIR_DB_H

#include <fstream>
#include <iostream>
#include <vector>

using namespace std;

// Size of chunk to read into memory at once
const std::size_t CHUNK_SIZE = 2e9; // 2B ints, 4GB

class FairDB {
public:
  virtual ~FairDB() {}
  virtual bool Init(size_t db_size_elements) = 0;
  virtual void Read(long &dummy, size_t start, size_t len) = 0;
  // virtual void Write(const std::vector<int>& data, int pos) = 0;
};

// In-memory database
class InMemoryFairDB : public FairDB {
public:
  bool Init(size_t db_size_elements) override {
    db_ = std::make_unique<vector<int>>();
    cout << "Initializing in-memory db of size: "
         << db_size_elements * sizeof(int) / 1e9 << "GB" << endl;
    db_->resize(db_size_elements);
    for (int i = 0; i < db_->size(); ++i) {
      (*db_)[i] = i;
    }
    cout << "Initialization complete." << endl;
    return true;
  }

  void Read(long &dummy, size_t start, size_t len) override {
    for (int j = 0; j < len; ++j) {
      dummy += (*db_)[start + j] % 2;
    }
  }

  // void Write(const std::vector<int>& data, int pos) override {
  //   (void) data;
  //   return;
  // }
private:
  std::unique_ptr<vector<int>> db_;
};

// Disk-based database
class DiskFairDB : public FairDB {
public:
  bool Init(size_t db_size_elements) override {
    const std::string filepath("/mnt/disks/fair_db/db.bin");
    read_file_ = std::ifstream(filepath, std::ios::binary);
    if (!read_file_.is_open()) {
      std::cerr << "Error opening file for reading: " << filepath << std::endl;
      return false;
    }
    read_buffer_.resize(CHUNK_SIZE);
    cout << "Disk DB Initialization complete." << endl;
    return true;
  }

  void Read(long &dummy, size_t start, size_t len) override {
    read_file_.seekg(start * sizeof(int), std::ios::beg);

    for (size_t i = 0; i < len; i += CHUNK_SIZE) {
      std::size_t currentChunkSize = std::min(CHUNK_SIZE, len - i * CHUNK_SIZE);
      read_file_.read(reinterpret_cast<char *>(&read_buffer_[0]),
                      currentChunkSize * sizeof(int));
      for (size_t j = 0; j < currentChunkSize; ++j) {
        dummy += read_buffer_[j] % 2;
      }
    }
  }

  // void Write(const std::vector<int>& data, int pos) override {
  //   (void) data;
  //   return;
  // }
private:
  std::ifstream read_file_;
  std::vector<int> read_buffer_;
};

#endif