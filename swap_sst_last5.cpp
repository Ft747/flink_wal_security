
#include <rocksdb/env.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

static std::string double_to_bytes(double val) {
  // pack into big-endian 8-byte double
  uint64_t bits;
  static_assert(sizeof(bits) == sizeof(val));
  std::memcpy(&bits, &val, sizeof(val));

  // if system is little-endian, swap to big-endian
  uint64_t be_bits = ((bits & 0x00000000000000FFULL) << 56) |
                     ((bits & 0x000000000000FF00ULL) << 40) |
                     ((bits & 0x0000000000FF0000ULL) << 24) |
                     ((bits & 0x00000000FF000000ULL) << 8) |
                     ((bits & 0x000000FF00000000ULL) >> 8) |
                     ((bits & 0x0000FF0000000000ULL) >> 24) |
                     ((bits & 0x00FF000000000000ULL) >> 40) |
                     ((bits & 0xFF00000000000000ULL) >> 56);

  return std::string(reinterpret_cast<const char *>(&be_bits), sizeof(be_bits));
}

int main(int argc, char **argv) {
  if (argc < 2 || argc > 3) {
    std::cerr << "Usage: swap_sst_last5 <in.sst> [--backup]\n";
    return 2;
  }
  std::string in_path = argv[1];
  bool do_backup = (argc == 3 && std::string(argv[2]) == "--backup");

  rocksdb::Options opts;
  opts.comparator = rocksdb::BytewiseComparator();

  rocksdb::SstFileReader reader(opts);
  auto st = reader.Open(in_path);
  if (!st.ok()) {
    std::cerr << "Failed to open input SST: " << st.ToString() << "\n";
    return 3;
  }

  std::unique_ptr<rocksdb::Iterator> it(
      reader.NewIterator(rocksdb::ReadOptions()));
  if (!it) {
    std::cerr << "Iterator creation failed\n";
    return 4;
  }

  // 1️⃣ Collect all keys & values in memory
  std::vector<std::pair<std::string, std::string>> kv;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    kv.emplace_back(std::string(it->key().data(), it->key().size()),
                    std::string(it->value().data(), it->value().size()));
  }
  if (!it->status().ok()) {
    std::cerr << "Iterator error: " << it->status().ToString() << "\n";
    return 5;
  }

  size_t total = kv.size();
  if (total < 5) {
    std::cerr << "Warning: file has only " << total
              << " entries (less than 5). Will replace them all.\n";
  }

  // 2️⃣ Prepare output SST
  std::filesystem::path in_p(in_path);
  std::string out_tmp =
      (in_p.parent_path() / (in_p.filename().string() + ".tmp")).string();
  std::string backup_path =
      (in_p.parent_path() / (in_p.filename().string() + ".bak")).string();

  std::error_code ec;
  std::filesystem::remove(out_tmp, ec);

  rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), opts);
  auto wst = writer.Open(out_tmp);
  if (!wst.ok()) {
    std::cerr << "Failed to open output tmp SST: " << wst.ToString() << "\n";
    return 6;
  }

  // 3️⃣ Rewrite SST: copy first N-5 entries, replace last 5 values
  std::string zero_val = double_to_bytes(0.0);
  for (size_t i = 0; i < kv.size(); ++i) {
    const auto &k = kv[i].first;
    const auto &v = kv[i].second;

    if (i >= kv.size() - 5) {
      // replace value with 0.0
      auto putst = writer.Put(rocksdb::Slice(k), rocksdb::Slice(zero_val));
      if (!putst.ok()) {
        std::cerr << "Put failed: " << putst.ToString() << "\n";
        return 7;
      }
    } else {
      auto putst = writer.Put(rocksdb::Slice(k), rocksdb::Slice(v));
      if (!putst.ok()) {
        std::cerr << "Put failed: " << putst.ToString() << "\n";
        return 8;
      }
    }
  }

  auto fin = writer.Finish();
  if (!fin.ok()) {
    std::cerr << "Finish failed: " << fin.ToString() << "\n";
    return 9;
  }

  if (do_backup) {
    std::error_code rc;
    std::filesystem::copy_file(
        in_path, backup_path, std::filesystem::copy_options::overwrite_existing,
        rc);
    if (rc) {
      std::cerr << "Warning: backup failed: " << rc.message() << "\n";
    } else {
      std::cout << "Backup saved to: " << backup_path << "\n";
    }
  }

  if (std::rename(out_tmp.c_str(), in_path.c_str()) != 0) {
    std::perror("rename failed");
    std::filesystem::remove(out_tmp, ec);
    return 10;
  }

  std::cout << "Total entries processed: " << total << "\n";
  std::cout << "Last " << std::min<size_t>(5, total)
            << " entries replaced with 0.0\n";

  return 0;
}
