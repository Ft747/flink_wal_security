
#include <iostream>
#include <map>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_writer.h>
#include <string>

int main() {
  // Dictionary-like data structure (key-value pairs)
  std::map<std::string, std::string> data = {
      {"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"},
      {"name", "John"},   {"age", "30"},      {"city", "New York"}};

  // Configure options
  rocksdb::Options options;
  rocksdb::EnvOptions env_options;

  // Create SST file writer
  rocksdb::SstFileWriter sst_file_writer(env_options, options);

  std::string sst_file_path = "output.sst";
  rocksdb::Status s = sst_file_writer.Open(sst_file_path);

  if (!s.ok()) {
    std::cerr << "Error opening SST file: " << s.ToString() << std::endl;
    return 1;
  }

  // Add key-value pairs to SST file
  for (const auto &pair : data) {
    s = sst_file_writer.Put(pair.first, pair.second);
    if (!s.ok()) {
      std::cerr << "Error adding key-value pair: " << s.ToString() << std::endl;
      return 1;
    }
    std::cout << "Added: " << pair.first << " -> " << pair.second << std::endl;
  }

  // Finalize the SST file
  s = sst_file_writer.Finish();
  if (!s.ok()) {
    std::cerr << "Error finishing SST file: " << s.ToString() << std::endl;
    return 1;
  }

  std::cout << "SST file created successfully: " << sst_file_path << std::endl;
  return 0;
}
