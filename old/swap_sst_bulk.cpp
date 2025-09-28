
// swap_sst_bulk.cpp
// Rewrite an existing SST with multiple replacements from a CSV of hex pairs.
// Usage: swap_sst_bulk <in.sst> <replacements.csv> [--backup]
//
// CSV format: each non-empty line: KEY_HEX,NEW_VALUE_HEX
// Lines starting with '#' are treated as comments.

#include <rocksdb/env.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>

#include <cctype>
#include <cstdio> // std::remove, std::rename
#include <cstring>
#include <filesystem> // C++17
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

static std::string trim(const std::string &s) {
  size_t a = 0;
  while (a < s.size() && std::isspace((unsigned char)s[a]))
    ++a;
  size_t b = s.size();
  while (b > a && std::isspace((unsigned char)s[b - 1]))
    --b;
  return s.substr(a, b - a);
}

static std::string hex_to_bytes(const std::string &hex_in) {
  std::string hex = trim(hex_in);
  size_t start = 0;
  if (hex.size() >= 2 && hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X'))
    start = 2;

  std::string clean;
  clean.reserve(hex.size() - start);
  for (size_t i = start; i < hex.size(); ++i) {
    char c = hex[i];
    if (std::isspace((unsigned char)c))
      continue;
    if (!std::isxdigit((unsigned char)c))
      throw std::invalid_argument("Non-hex character in hex string: '" +
                                  std::string(1, c) + "'");
    clean.push_back(c);
  }
  if (clean.size() % 2 != 0)
    throw std::invalid_argument("Hex string has odd length");
  std::string out;
  out.reserve(clean.size() / 2);
  auto hexval = [](char c) -> int {
    if (c >= '0' && c <= '9')
      return c - '0';
    if (c >= 'a' && c <= 'f')
      return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F')
      return 10 + (c - 'A');
    return 0;
  };
  for (size_t i = 0; i < clean.size(); i += 2) {
    int hi = hexval(clean[i]);
    int lo = hexval(clean[i + 1]);
    unsigned char byte = static_cast<unsigned char>((hi << 4) | lo);
    out.push_back(static_cast<char>(byte));
  }
  return out;
}

static std::unordered_map<std::string, std::string>
load_replacements(const std::string &csv_path) {
  std::unordered_map<std::string, std::string> map;
  std::ifstream ifs(csv_path);
  if (!ifs)
    throw std::runtime_error("Failed to open replacements file: " + csv_path);

  std::string line;
  size_t lineno = 0;
  while (std::getline(ifs, line)) {
    ++lineno;
    std::string t = trim(line);
    if (t.empty() || t[0] == '#')
      continue;
    // find comma
    size_t comma = t.find(',');
    if (comma == std::string::npos) {
      throw std::runtime_error("Invalid line " + std::to_string(lineno) +
                               " (no comma)");
    }
    std::string key_hex = trim(t.substr(0, comma));
    std::string val_hex = trim(t.substr(comma + 1));
    if (key_hex.empty())
      throw std::runtime_error("Empty key at line " + std::to_string(lineno));
    // convert both to raw bytes
    std::string key_bytes = hex_to_bytes(key_hex);
    std::string val_bytes = hex_to_bytes(val_hex);
    map.emplace(std::move(key_bytes), std::move(val_bytes));
  }
  return map;
}

int main(int argc, char **argv) {
  if (argc < 3 || argc > 4) {
    std::cerr
        << "Usage: swap_sst_bulk <in.sst> <replacements.csv> [--backup]\n";
    return 2;
  }
  std::string in_path = argv[1];
  std::string csv_path = argv[2];
  bool do_backup = false;
  if (argc == 4) {
    std::string flag = argv[3];
    if (flag == "--backup")
      do_backup = true;
    else {
      std::cerr << "Unknown flag: " << flag << "\n";
      return 2;
    }
  }

  // Load replacements map (hex -> raw bytes)
  std::unordered_map<std::string, std::string> repl;
  try {
    repl = load_replacements(csv_path);
  } catch (const std::exception &e) {
    std::cerr << "Failed to read replacements: " << e.what() << "\n";
    return 3;
  }
  if (repl.empty()) {
    std::cerr << "No replacements found in CSV (nothing to do).\n";
    return 0;
  }

  // Prepare RocksDB options and reader/writer
  rocksdb::Options opts;
  opts.comparator = rocksdb::BytewiseComparator();

  rocksdb::SstFileReader reader(opts);
  auto st = reader.Open(in_path);
  if (!st.ok()) {
    std::cerr << "Failed to open input SST: " << st.ToString() << "\n";
    return 4;
  }

  std::filesystem::path in_p(in_path);
  std::string out_tmp =
      (in_p.parent_path() / (in_p.filename().string() + ".tmp")).string();
  std::string backup_path =
      (in_p.parent_path() / (in_p.filename().string() + ".bak")).string();

  // Ensure no leftover tmp file
  std::error_code ec;
  std::filesystem::remove(out_tmp, ec);

  rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), opts);
  auto wst = writer.Open(out_tmp);
  if (!wst.ok()) {
    std::cerr << "Failed to open output tmp SST: " << wst.ToString() << "\n";
    return 5;
  }

  std::unique_ptr<rocksdb::Iterator> it(
      reader.NewIterator(rocksdb::ReadOptions()));
  if (!it) {
    std::cerr << "Iterator creation failed\n";
    return 6;
  }

  size_t total = 0;
  size_t replaced = 0;

  // track which keys we matched so we can warn about not-found keys
  std::unordered_map<std::string, bool> seen;
  seen.reserve(repl.size());
  for (const auto &p : repl)
    seen.emplace(p.first, false);

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    rocksdb::Slice k = it->key();
    rocksdb::Slice v = it->value();
    ++total;

    auto found = repl.find(std::string(k.data(), k.size()));
    if (found != repl.end()) {
      // write the replacement value
      rocksdb::Status putst = writer.Put(
          rocksdb::Slice(k.data(), k.size()),
          rocksdb::Slice(found->second.data(), found->second.size()));
      if (!putst.ok()) {
        std::cerr << "Put (replace) failed: " << putst.ToString() << "\n";
        return 7;
      }
      ++replaced;
      seen[found->first] = true;
    } else {
      // copy original
      rocksdb::Status putst = writer.Put(k, v);
      if (!putst.ok()) {
        std::cerr << "Put (copy) failed: " << putst.ToString() << "\n";
        return 8;
      }
    }
  }

  if (!it->status().ok()) {
    std::cerr << "Iterator error: " << it->status().ToString() << "\n";
    return 9;
  }

  auto fin = writer.Finish();
  if (!fin.ok()) {
    std::cerr << "Finish failed: " << fin.ToString() << "\n";
    return 10;
  }

  // optional backup
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

  // Atomically replace original with tmp
  // POSIX rename() is atomic if source and dest are on same filesystem.
  if (std::rename(out_tmp.c_str(), in_path.c_str()) != 0) {
    std::perror("rename failed");
    // attempt to remove tmp to avoid litter
    std::filesystem::remove(out_tmp, ec);
    return 11;
  }

  std::cout << "Total entries processed: " << total << "\n";
  std::cout << "Replacements applied: " << replaced << "\n";

  // report keys that were not found
  size_t not_found = 0;
  for (const auto &p : seen) {
    if (!p.second)
      ++not_found;
  }
  if (not_found > 0) {
    std::cerr
        << "Warning: " << not_found
        << " replacement key(s) not found in SST. (Check CSV hex accuracy)\n";
  }

  return 0;
}
