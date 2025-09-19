
// sst_roundtrip_replace.cpp
// Reads an SST file, copies all entries to a new SST file,
// replacing the last N entries with a given value.
// Usage: ./sst_roundtrip_replace input.sst N [--zero]

#include <rocksdb/env.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>

#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <vector>

int main(int argc, char **argv) {
  if (argc < 3 || argc > 4) {
    std::cerr << "Usage: " << argv[0] << " <input.sst> <N> [--zero]\n";
    return 1;
  }

  std::string input_path = argv[1];
  size_t n_last = std::stoul(argv[2]);
  bool replace_with_zero = (argc == 4 && std::strcmp(argv[3], "--zero") == 0);

  std::filesystem::path in_p(input_path);
  std::string output_path =
      (in_p.parent_path() / (in_p.filename().string() + ".rewritten.sst"))
          .string();

  rocksdb::Options opts;
  opts.comparator = rocksdb::BytewiseComparator();

  // Open reader
  rocksdb::SstFileReader reader(opts);
  auto s = reader.Open(input_path);
  if (!s.ok()) {
    std::cerr << "Failed to open SST: " << s.ToString() << "\n";
    return 2;
  }

  // First, collect all entries into memory so we can find the last N
  std::unique_ptr<rocksdb::Iterator> it(
      reader.NewIterator(rocksdb::ReadOptions()));
  if (!it) {
    std::cerr << "Failed to create iterator.\n";
    return 3;
  }

  std::vector<std::pair<std::string, std::string>> entries;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    entries.emplace_back(std::string(it->key().data(), it->key().size()),
                         std::string(it->value().data(), it->value().size()));
  }
  if (!it->status().ok()) {
    std::cerr << "Iterator error: " << it->status().ToString() << "\n";
    return 4;
  }

  if (entries.empty()) {
    std::cerr << "No entries found in SST.\n";
    return 0;
  }

  size_t total = entries.size();
  std::cout << "Total entries found: " << total << "\n";

  if (n_last > total) {
    std::cerr << "Requested N (" << n_last
              << ") is greater than total entries (" << total
              << "). Adjusting.\n";
    n_last = total;
  }

  // Prepare writer
  rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), opts);
  s = writer.Open(output_path);
  if (!s.ok()) {
    std::cerr << "Failed to open writer: " << s.ToString() << "\n";
    return 5;
  }

  size_t replaced = 0;
  for (size_t i = 0; i < total; ++i) {
    std::string value = entries[i].second;

    // Replace last N entries if requested
    if (i >= total - n_last && replace_with_zero) {
      std::fill(value.begin(), value.end(), 0);
      replaced++;
    }

    auto putst = writer.Put(entries[i].first, value);
    if (!putst.ok()) {
      std::cerr << "Writer.Put failed: " << putst.ToString() << "\n";
      return 6;
    }
  }

  s = writer.Finish();
  if (!s.ok()) {
    std::cerr << "Writer.Finish failed: " << s.ToString() << "\n";
    return 7;
  }

  std::cout << "Round-trip complete. Entries copied: " << total << "\n";
  if (replace_with_zero) {
    std::cout << "Replaced last " << replaced << " entries with all-zeros.\n";
  }
  // Atomic replacement of original file
  try {
    std::filesystem::path backup_path = input_path + ".bak";
    std::filesystem::rename(input_path, backup_path); // backup original
    std::filesystem::rename(output_path, input_path); // put new file in place
    std::cout << "Replaced original SST. Backup saved as: " << backup_path
              << "\n";
  } catch (const std::filesystem::filesystem_error &e) {
    std::cerr << "Failed to replace original file: " << e.what() << "\n";
    return 8;
  }

  return 0;
}
