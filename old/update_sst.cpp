#include <iostream>
#include <rocksdb/env.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/status.h>

using namespace rocksdb;

int main() {
  // Initialize options
  rocksdb::Options opts;
  opts.create_if_missing = true;

  // Create SstFileWriter with proper parameters
  rocksdb::SstFileWriter sst_writer(rocksdb::EnvOptions(), opts);

  // Open the SST file
  Status s = sst_writer.Open("./a4208370-e176-40a6-8954-9c353fbb33bd.sst");
  if (!s.ok()) {
    std::cerr << "Error opening SST file: " << s.ToString() << std::endl;
    return 1;
  }

  // Add key-value pairs
  s = sst_writer.Put("toy2", "fast red car");
  if (!s.ok()) {
    std::cerr << "Error putting data: " << s.ToString() << std::endl;
    return 1;
  }

  // Finalize the SST file
  s = sst_writer.Finish();
  if (!s.ok()) {
    std::cerr << "Error finishing SST file: " << s.ToString() << std::endl;
    return 1;
  }

  std::cout << "SST file created successfully!" << std::endl;
  return 0;
}
