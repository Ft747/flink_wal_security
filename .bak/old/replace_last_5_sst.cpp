
// replace_last_5_sst.cpp
// Rewrites an existing SST file, replacing the values of the last 5 key-value pairs with the integer 100 (as 4-byte little-endian).
// Usage: replace_last_5_sst <in.sst> <out.sst>

#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/iterator.h>

#include <iostream>
#include <string>
#include <vector>
#include <deque>
#include <cstring>
#include <stdexcept>

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: replace_last_5_sst <in.sst> <out.sst>\n";
    return 2;
  }

  std::string in_path  = argv[1];
  std::string out_path = argv[2];

  rocksdb::Options opts;
  opts.comparator = rocksdb::BytewiseComparator();

  // Open input SST
  rocksdb::SstFileReader reader(opts);
  auto st = reader.Open(in_path);
  if (!st.ok()) {
    std::cerr << "Failed to open input SST: " << st.ToString() << "\n";
    return 3;
  }

  // Iterator over input SST
  std::unique_ptr<rocksdb::Iterator> it(reader.NewIterator(rocksdb::ReadOptions()));
  if (!it) {
    std::cerr << "Iterator creation failed\n";
    return 4;
  }

  // Open output SST writer
  rocksdb::SstFileWriter sst(rocksdb::EnvOptions(), opts);
  auto s = sst.Open(out_path);
  if (!s.ok()) {
    std::cerr << "Open out.sst failed: " << s.ToString() << "\n";
    return 5;
  }

  // Buffer to store last 5 KV pairs
  std::deque<std::pair<std::string, std::string>> last_five;

  size_t total_entries = 0;

  // First pass: read all entries and keep last 5 in buffer
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    rocksdb::Slice k = it->key();
    rocksdb::Slice v = it->value();

    // Store copy of key and value
    last_five.push_back({k.ToString(), v.ToString()});

    // Keep only last 5
    if (last_five.size() > 5) {
      last_five.pop_front();
    }

    total_entries++;
  }

  if (!it->status().ok()) {
    std::cerr << "Iterator error: " << it->status().ToString() << "\n";
    return 8;
  }

  // Rewind and do second pass: write all entries, replacing last 5 values
  it->SeekToFirst();

  // Create replacement value: 4-byte little-endian integer 100
  // 100 in hex = 0x64 -> bytes: [0x64, 0x00, 0x00, 0x00]
  std::string replacement_value = std::string("\x64\x00\x00\x00", 4);

  // If you prefer ASCII "100", use this instead:
  // std::string replacement_value = "100";

  size_t written = 0;
  size_t replaced = 0;

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    rocksdb::Slice k = it->key();

    rocksdb::Slice v;
    bool should_replace = false;

    // Check if current key is among last 5
    if (written >= total_entries - last_five.size()) {
      should_replace = true;
    }

    if (should_replace) {
      v = rocksdb::Slice(replacement_value);
      replaced++;
    } else {
      v = it->value();
    }

    auto putst = sst.Put(k, v);
    if (!putst.ok()) {
      std::cerr << "Put failed at entry " << written << ": " << putst.ToString() << "\n";
      return 7;
    }

    written++;
  }

  auto fin = sst.Finish();
  if (!fin.ok()) {
    std::cerr << "Finish failed: " << fin.ToString() << "\n";
    return 9;
  }

  std::cout << "Total entries: " << total_entries << ", replaced last " << replaced << " with value 100 (4-byte int)\n";
  return 0;
}
