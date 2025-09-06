
// swap_sst.cpp
// Rewrites an existing SST file with exactly one KV pair's value changed.
// Usage: swap_sst <in.sst> <out.sst> <KEY_HEX> <NEW_VALUE_HEX>

#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/iterator.h>

#include <iostream>
#include <string>
#include <vector>
#include <cctype>
#include <cstring>
#include <stdexcept>

// Convert ASCII hex string (e.g. "0A1B") to raw bytes in std::string.
// Accepts upper/lower-case hex. Throws on odd length or invalid char.
static std::string hex_to_bytes(const std::string &hex) {
  std::string clean;
  // allow optional "0x" prefix
  size_t start = 0;
  if (hex.size() >= 2 && hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X')) {
    start = 2;
  }
  // copy remainder
  for (size_t i = start; i < hex.size(); ++i) {
    char c = hex[i];
    if (!std::isxdigit(static_cast<unsigned char>(c))) {
      // If it's whitespace, you can ignore; otherwise it's an error.
      if (std::isspace(static_cast<unsigned char>(c))) continue;
      throw std::invalid_argument("Non-hex character in hex string");
    }
    clean.push_back(c);
  }
  if (clean.size() % 2 != 0) {
    throw std::invalid_argument("Hex string has odd length");
  }

  std::string out;
  out.reserve(clean.size() / 2);
  auto hexval = [](char c)->int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return 0;
  };
  for (size_t i = 0; i < clean.size(); i += 2) {
    int hi = hexval(clean[i]);
    int lo = hexval(clean[i+1]);
    unsigned char byte = static_cast<unsigned char>((hi << 4) | lo);
    out.push_back(static_cast<char>(byte));
  }
  return out;
}

int main(int argc, char** argv) {
  if (argc != 5) {
    std::cerr << "Usage: swap_sst <in.sst> <out.sst> <KEY_HEX> <NEW_VALUE_HEX>\n";
    return 2;
  }

  std::string in_path    = argv[1];
  std::string out_path   = argv[2];
  std::string key_hex    = argv[3];
  std::string newval_hex = argv[4];

  std::string target_key;
  std::string new_value_bytes;
  try {
    target_key = hex_to_bytes(key_hex);
    new_value_bytes = hex_to_bytes(newval_hex);
  } catch (const std::exception &e) {
    std::cerr << "Hex decode error: " << e.what() << "\n";
    return 10;
  }

  rocksdb::Options opts;
  opts.comparator = rocksdb::BytewiseComparator();

  // Reader for input SST
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

  // Writer for output SST
  rocksdb::SstFileWriter sst(rocksdb::EnvOptions(), opts);
  auto s = sst.Open(out_path);
  if (!s.ok()) {
    std::cerr << "Open out.sst failed: " << s.ToString() << "\n";
    return 5;
  }

  size_t copied = 0;
  size_t replaced = 0;

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    rocksdb::Slice k = it->key();
    rocksdb::Slice v = it->value();

    bool is_match = false;
    if (k.size() == static_cast<ssize_t>(target_key.size())) {
      // compare raw bytes
      if (std::memcmp(k.data(), target_key.data(), k.size()) == 0) {
        is_match = true;
      }
    }

    rocksdb::Status putst;
    if (is_match) {
      putst = sst.Put(rocksdb::Slice(k.data(), k.size()), rocksdb::Slice(new_value_bytes.data(), new_value_bytes.size()));
      if (!putst.ok()) {
        std::cerr << "Put (replace) failed: " << putst.ToString() << "\n";
        return 6;
      }
      replaced++;
    } else {
      putst = sst.Put(k, v);
      if (!putst.ok()) {
        std::cerr << "Put (copy) failed: " << putst.ToString() << "\n";
        return 7;
      }
    }
    copied++;
  }
  if (!it->status().ok()) {
    std::cerr << "Iterator error: " << it->status().ToString() << "\n";
    return 8;
  }

  auto fin = sst.Finish();
  if (!fin.ok()) {
    std::cerr << "Finish failed: " << fin.ToString() << "\n";
    return 9;
  }

  std::cout << "Copied entries: " << copied << ", replaced: " << replaced << "\n";
  if (replaced == 0) {
    std::cerr << "Warning: no matching key found. Make sure the KEY_HEX is the exact hex string printed by `sst_dump --output_hex`.\n";
  }

  return 0;
}
