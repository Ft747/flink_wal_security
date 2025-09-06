#include <iostream>
#include <string>
#include <cassert>
#include <memory>
#include "rocksdb/db.h"
#include "rocksdb/options.h"

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <db_path> <key> <value>\n";
        return 1;
    }

    std::string db_path = argv[1];
    std::string key = argv[2];
    std::string value = argv[3];

    rocksdb::Options options;
    options.create_if_missing = true;

    std::unique_ptr<rocksdb::DB> db;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
    if (!status.ok()) {
        std::cerr << "Error opening DB: " << status.ToString() << "\n";
        return 1;
    }

    status = db->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
        std::cerr << "Put error: " << status.ToString() << "\n";
        return 1;
    }

    std::cout << "Inserted key='" << key << "', value='" << value << "'\n";
    return 0;
}
