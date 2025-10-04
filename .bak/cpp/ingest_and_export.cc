
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/utilities/options_util.h"
#include <iostream>
#include <string>

using namespace ROCKSDB_NAMESPACE;

int main() {
  std::string db_path = "/tmp/mydb";
  std::string input_sst =
      "/home/dtome/Documents/School/Thesis/code/changed.sst";
  std::string output_sst = "/tmp/output.sst";

  // 1. Open (or create) the RocksDB database
  Options options;
  options.create_if_missing = true;

  DB *db;
  Status s = DB::Open(options, db_path, &db);
  if (!s.ok()) {
    std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
    return 1;
  }

  // 2. Ingest the external SST file
  IngestExternalFileOptions ifo;
  s = db->IngestExternalFile({input_sst}, ifo);
  if (!s.ok()) {
    std::cerr << "Failed to ingest SST: " << s.ToString() << std::endl;
    delete db;
    return 1;
  }
  std::cout << "Ingested: " << input_sst << std::endl;

  // 3. Add two extra key–value pairs
  s = db->Put(WriteOptions(), "extra_key1", "extra_value1");
  if (!s.ok()) {
    std::cerr << "Failed to insert extra_key1: " << s.ToString() << std::endl;
  }

  s = db->Put(WriteOptions(), "extra_key2", "extra_value2");
  if (!s.ok()) {
    std::cerr << "Failed to insert extra_key2: " << s.ToString() << std::endl;
  }

  // 4. Export DB contents into a new external SST file
  EnvOptions env_options;
  SstFileWriter sst_file_writer(env_options, options);

  s = sst_file_writer.Open(output_sst);
  if (!s.ok()) {
    std::cerr << "Failed to open SST for writing: " << s.ToString()
              << std::endl;
    delete db;
    return 1;
  }

  Iterator *it = db->NewIterator(ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    s = sst_file_writer.Put(it->key(), it->value());
    if (!s.ok()) {
      std::cerr << "Error writing key: " << it->key().ToString()
                << " error: " << s.ToString() << std::endl;
    }
  }
  delete it;

  s = sst_file_writer.Finish();
  if (!s.ok()) {
    std::cerr << "Failed to finish SST: " << s.ToString() << std::endl;
    delete db;
    return 1;
  }

  std::cout << "Exported DB to: " << output_sst << std::endl;

  delete db;
  return 0;
}
