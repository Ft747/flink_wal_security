#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/options.h>
#include <iostream>
#include <vector>
#include <string>

struct Record {
    std::string key;
    std::string value;
    rocksdb::SequenceNumber seq;
    rocksdb::ValueType type;
};

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_sst> <output_sst>" << std::endl;
        return 1;
    }

    std::string input_file = argv[1];
    std::string output_file = argv[2];

    rocksdb::Options options;
    options.env = rocksdb::Env::Default();

    rocksdb::SstFileReader reader(options);
    rocksdb::Status status = reader.Open(input_file);
    if (!status.ok()) {
        std::cerr << "Error opening SST file: " << status.ToString() << std::endl;
        return 1;
    }

    std::vector<Record> records;
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator(read_options));

    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rocksdb::ParsedInternalKey parsed_key;
        if (!rocksdb::ParseInternalKey(iter->key(), &parsed_key)) {
            std::cerr << "Error parsing internal key" << std::endl;
            return 1;
        }
        records.push_back({parsed_key.user_key.ToString(), iter->value().ToString(), parsed_key.sequence, parsed_key.type});
    }

    if (!iter->status().ok()) {
        std::cerr << "Error iterating: " << iter->status().ToString() << std::endl;
        return 1;
    }

    if (records.empty()) {
        std::cerr << "No records in SST file" << std::endl;
        return 1;
    }

    // Modify the last record's value
    records.back().value += "_modified";

    // Now write to new SST file
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
    status = writer.Open(output_file);
    if (!status.ok()) {
        std::cerr << "Error opening output SST file: " << status.ToString() << std::endl;
        return 1;
    }

    for (const auto& rec : records) {
        rocksdb::InternalKey internal_key(rec.key, rec.seq, rec.type);
        status = writer.Put(internal_key.Encode(), rec.value);
        if (!status.ok()) {
            std::cerr << "Error writing record: " << status.ToString() << std::endl;
            return 1;
        }
    }

    status = writer.Finish();
    if (!status.ok()) {
        std::cerr << "Error finishing SST file: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "Successfully modified the last record and wrote to " << output_file << std::endl;
    return 0;
}
