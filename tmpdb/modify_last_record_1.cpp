#include <rocksdb/comparator.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/status.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

struct Entry {
  std::string key;
  std::string timestamp;
  std::string value;
};

namespace {

bool ParseHexValue(const std::string& hex, std::string* out_bytes) {
  if (hex.size() % 2 != 0) {
    return false;
  }

  auto is_hex_digit = [](unsigned char c) {
    return std::isxdigit(c) != 0;
  };

  if (!std::all_of(hex.begin(), hex.end(), is_hex_digit)) {
    return false;
  }

  out_bytes->clear();
  out_bytes->reserve(hex.size() / 2);

  auto hex_to_nibble = [](char c) -> uint8_t {
    if (c >= '0' && c <= '9') {
      return static_cast<uint8_t>(c - '0');
    }
    if (c >= 'a' && c <= 'f') {
      return static_cast<uint8_t>(10 + (c - 'a'));
    }
    return static_cast<uint8_t>(10 + (c - 'A'));
  };

  for (size_t i = 0; i < hex.size(); i += 2) {
    uint8_t high = hex_to_nibble(hex[i]);
    uint8_t low = hex_to_nibble(hex[i + 1]);
    out_bytes->push_back(static_cast<char>((high << 4) | low));
  }

  return true;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0]
              << " <sst_file_without_extension> <new_value_hex>" << std::endl;
    return EXIT_FAILURE;
  }

  const std::string input_path = argv[1];
  const std::string temp_path = input_path + ".tmp";
  const std::string backup_path = input_path + ".bak";

  std::string new_value_bytes;
  if (!ParseHexValue(argv[2], &new_value_bytes)) {
    std::cerr << "New value must be provided as an even-length hexadecimal"
              << " string (e.g. 0000000000000000)." << std::endl;
    return EXIT_FAILURE;
  }

  rocksdb::Options options;
  options.comparator = rocksdb::BytewiseComparator();
  options.compression = rocksdb::kSnappyCompression;
  options.create_if_missing = false;
  options.error_if_exists = false;
  options.persist_user_defined_timestamps = true;

  rocksdb::SstFileReader reader(options);
  rocksdb::Status status = reader.Open(input_path);
  if (!status.ok()) {
    std::cerr << "Failed to open SST file: " << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }

  rocksdb::ReadOptions read_options;
  read_options.fill_cache = false;
  read_options.verify_checksums = true;

  std::unique_ptr<rocksdb::Iterator> it(reader.NewIterator(read_options));
  if (it == nullptr) {
    std::cerr << "Failed to create iterator over SST file" << std::endl;
    return EXIT_FAILURE;
  }

  std::vector<Entry> entries;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    Entry entry;
    entry.key.assign(it->key().data(), it->key().size());

    rocksdb::Slice ts_slice;
    // timestamp() may not be supported; guard the call.
    try {
      ts_slice = it->timestamp();
    } catch (...) {
      ts_slice = rocksdb::Slice();
    }
    if (ts_slice.size() > 0) {
      entry.timestamp.assign(ts_slice.data(), ts_slice.size());
    }

    entry.value.assign(it->value().data(), it->value().size());
    entries.emplace_back(std::move(entry));
  }

  if (!it->status().ok()) {
    std::cerr << "Iterator error: " << it->status().ToString() << std::endl;
    return EXIT_FAILURE;
  }

  if (entries.empty()) {
    std::cerr << "Input SST file contains no entries." << std::endl;
    return EXIT_FAILURE;
  }

  Entry& last_entry = entries.back();
  last_entry.value = new_value_bytes;

  rocksdb::EnvOptions env_options;
  rocksdb::SstFileWriter writer(env_options, options);

  status = writer.Open(temp_path);
  if (!status.ok()) {
    std::cerr << "Failed to open temporary SST file for writing: "
              << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }

  for (const Entry& entry : entries) {
    rocksdb::Slice key(entry.key);
    rocksdb::Slice value(entry.value);

    if (!entry.timestamp.empty()) {
      rocksdb::Slice ts(entry.timestamp);
      status = writer.Put(key, ts, value);
    } else {
      status = writer.Put(key, value);
    }

    if (!status.ok()) {
      std::cerr << "Failed to write entry: " << status.ToString() << std::endl;
      return EXIT_FAILURE;
    }
  }

  status = writer.Finish();
  if (!status.ok()) {
    std::cerr << "Failed to finish writing SST file: " << status.ToString()
              << std::endl;
    return EXIT_FAILURE;
  }

  rocksdb::Env* env = options.env ? options.env : rocksdb::Env::Default();

  // Cleanup any stale backup from a prior run.
  env->DeleteFile(backup_path);

  // Backup the original file in case rename fails midway.
  rocksdb::Status backup_status = env->RenameFile(input_path, backup_path);
  if (!backup_status.ok()) {
    std::cerr << "Failed to create backup of original SST file: "
              << backup_status.ToString() << std::endl;
    env->DeleteFile(temp_path);
    return EXIT_FAILURE;
  }

  // Replace the original file with the modified one atomically.
  status = env->RenameFile(temp_path, input_path);
  if (!status.ok()) {
    std::cerr << "Failed to replace original SST file: " << status.ToString()
              << std::endl;
    // Attempt to clean up the temporary file.
    env->DeleteFile(temp_path);
    env->RenameFile(backup_path, input_path);
    return EXIT_FAILURE;
  }

  // Remove backup once overwrite succeeds.
  env->DeleteFile(backup_path);

  std::cout << "Successfully updated last record value in " << input_path
            << std::endl;
  return EXIT_SUCCESS;
}
