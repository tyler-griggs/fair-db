#ifndef FAIR_DB_H
#define FAIR_DB_H

#include <fstream>
#include <iostream>
#include <vector>

using namespace std;
const std::size_t CHUNK_SIZE = 4e9; // 4B ints, 16GB

class FairDB {
public:
  virtual ~FairDB() {}
  virtual void Init(size_t db_size_elements) {}
  virtual void Read(long &dummy, size_t start, size_t len) = 0;
  // virtual void Write(const std::vector<int>& data, int pos) = 0;
};

// In-memory database
class InMemoryFairDB : public FairDB {
public:
  // InMemoryFairDB(size_t db_size_elements) :
  // db_size_elements_(db_size_elements) {}
  void Init(size_t db_size_elements) override {
    db_ = std::make_unique<vector<int>>();
    cout << "Initializing in-memory db of size: "
         << db_size_elements * sizeof(int) / 1e9 << "GB" << endl;
    db_->resize(db_size_elements);
    for (int i = 0; i < db_->size(); ++i) {
      (*db_)[i] = i;
    }
    cout << "Initialization complete." << endl;
  }

  void Read(long &dummy, size_t start, size_t len) override {
    // db_->Read(stats.dummy, read.start_idx, read.read_size);
    for (int j = 0; j < len; ++j) {
      dummy += (*db_)[start + j] % 2;
    }
  }

  // void Write(const std::vector<int>& data, int pos) override {
  //   (void) data;
  //   return;
  // }
private:
  // TODO: unique_ptr
  std::unique_ptr<vector<int>> db_;
};

// Disk-based database
class DiskFairDB : public FairDB {
public:
  void Init(size_t db_size_elements) override {
    const std::string filepath("/mnt/disks/fair_db/db.bin");
    read_file_ = std::ifstream(filepath, std::ios::binary);
    if (!read_file_.is_open()) {
      std::cerr << "Error opening file for reading: " << filepath << std::endl;
    }
    read_buffer_.resize(CHUNK_SIZE);
    cout << "Disk DB Initialization complete." << endl;
  }

  void Read(long &dummy, size_t start, size_t len) override {
    read_file_.seekg(start * sizeof(int), std::ios::beg);

    for (size_t i = 0; i < len; i += CHUNK_SIZE) {
      std::size_t currentChunkSize = std::min(CHUNK_SIZE, len - i * CHUNK_SIZE);
      // cout << "i=" << i << ", curChunkSize=" << currentChunkSize << ",
      // bufferSize=" << read_buffer_.size() <<  endl;
      read_file_.read(reinterpret_cast<char *>(&read_buffer_[0]),
                      currentChunkSize * sizeof(int));
      // cout << "read complete" << endl;
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