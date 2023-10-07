#ifndef FAIR_DB_H
#define FAIR_DB_H

#include <iostream>
#include <vector>

using namespace std;

class FairDB {
public:

  FairDB(size_t db_size_elements) : db_size_elements_(db_size_elements) {}

  void Init() {
    cout << "Initializing db of size: "
         << db_size_elements_ * datatype_size_ / 1e9 << "GB" << endl;
    db_.resize(db_size_elements_);
    for (int i = 0; i < db_.size(); ++i) {
      db_[i] = i;
    }
    cout << "Initialization complete." << endl;
  }

  vector<int>* database() {
    return &db_;
  }

  size_t db_size_elements() {
    return db_size_elements_;
  }
  size_t datatype_size() {
    return datatype_size_;
  }

private:
  size_t db_size_elements_;
  size_t datatype_size_ = sizeof(int);
  // TODO: pointer wrapper
  vector<int> db_;
};

#endif