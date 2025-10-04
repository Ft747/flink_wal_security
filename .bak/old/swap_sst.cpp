
// swap_sst.cpp
// Rewrites an existing SST file with exactly one KV pair's value changed.
// Usage: swap_sst <in.sst> <out.sst> <KEY> <NEW_VALUE>

#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/iterator.h>
#include <iostream>
#include <string>

int main(int argc, char** argv) {
  if (argc != 5) {
    std::cerr << "Usage: swap_sst <in.sst> <out.sst> <KEY> <NEW_VALUE>\n";
    return 2;
  }

  std::string in_path    = argv[1];
  std::string out_path   = argv[2];
  std::string target_key = argv[3];
  std::string new_value  = argv[4];

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

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    rocksdb::Slice k = it->key();
    rocksdb::Slice v = it->value();

    if (k.compare(target_key) == 0) {
      auto putst = sst.Put(k, rocksdb::Slice(new_value));
      if (!putst.ok()) {
        std::cerr << "Put failed: " << putst.ToString() << "\n";
        return 6;
      }
    } else {
      auto putst = sst.Put(k, v);
      if (!putst.ok()) {
        std::cerr << "Put failed: " << putst.ToString() << "\n";
        return 7;
      }
    }
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

  return 0;
}
