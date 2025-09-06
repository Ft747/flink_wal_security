#include <rocksdb/sst_file_writer.h>
#include <rocksdb/options.h>
#include <rocksdb/env.h>
#include <iostream>
#include <rocksdb/types.h>
#include <string>

// Use the rocksdb namespace to avoid repeatedly typing rocksdb::
using namespace rocksdb;

int main() {
    Options options;
    // EnvOptions is used to configure the environment, like file system access.
    // The default constructor is usually sufficient for basic use.
    EnvOptions env_options;
    SstFileWriter sst_file_writer(env_options, options);

    // Path to where we will write the SST file
    std::string file_path = "/tmp/file1.sst";

    // Open the file for writing.
    // We explicitly provide the Temperature enum, which is required by newer
    // versions of the RocksDB API that the linker is expecting.
    // Open the file for writing.
    Status s = sst_file_writer.Open(file_path );
    if (!s.ok()) {
        std::cerr << "Error while opening file " << file_path
                  << ": " << s.ToString() << std::endl;
        return 1;
    }

    // Insert key-value pairs into the SST file.
    // Note that inserted keys must be strictly increasing according to the comparator.
    // The default comparator is bytewise, which works for these string keys.
    for (int i = 0; i < 3; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(i);

        s = sst_file_writer.Put(key, value);
        if (!s.ok()) {
            std::cerr << "Error while adding Key: " << key << ", Error: " << s.ToString() << std::endl;
            return 1;
        }
    }

    // Close the file and finalize the SST format.
    s = sst_file_writer.Finish();
    if (!s.ok()) {
        std::cerr << "Error while finishing file " << file_path << ": " << s.ToString() << std::endl;
        return 1;
    }

    std::cout << "SST file created successfully at " << file_path << std::endl;
    return 0;
}

