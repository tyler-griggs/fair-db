#ifndef FAIR_DB_H
#define FAIR_DB_H

#include <iostream>
#include <vector>

using namespace std;

// TODO: Create abstract DB, then create instantiations that are for disk or memeory
// Base class for database types
class FairDB {
public:
    virtual ~FairDB() {}
    virtual void Init(size_t db_size_elements) {}
    virtual void Read(long& dummy, size_t start, size_t len) = 0;
    // virtual void Write(const std::vector<int>& data, int pos) = 0;
};

// In-memory database
class InMemoryFairDB : public FairDB {
public:
  // InMemoryFairDB(size_t db_size_elements) : db_size_elements_(db_size_elements) {}
  void Init(size_t db_size_elements) override {
    db_ = std::make_shared<vector<int>>();
    cout << "Initializing db of size: "
         << db_size_elements * sizeof(int) / 1e9 << "GB" << endl;
    db_->resize(db_size_elements);
    for (int i = 0; i < db_->size(); ++i) {
      (*db_)[i] = i;
    }
    cout << "Initialization complete." << endl;
  }

  void Read(long& dummy, size_t start, size_t len) override {
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
  std::shared_ptr<vector<int>> db_;
};

#endif