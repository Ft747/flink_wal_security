#ifndef SST_STRUCTURES_H
#define SST_STRUCTURES_H

#include <cstdint>
#include <string>
#include <vector>

// SST File format constants
struct SSTConstants {
    static constexpr size_t FOOTER_SIZE = 48;           // RocksDB v2 footer size
    static constexpr size_t BLOCK_TRAILER_SIZE = 5;     // 1 byte type + 4 bytes CRC
    static constexpr uint64_t TABLE_MAGIC_NUMBER = 0x88e241b785f4cff7ull;
};

// Block handle structure (offset + size)
struct BlockHandle {
    uint64_t offset;
    uint64_t size;
};

// SST Footer structure
struct Footer {
    BlockHandle metaindex_handle;
    BlockHandle index_handle;
    uint64_t magic_number;
};

// Data block structure
struct DataBlock {
    size_t offset;
    size_t size;
    std::vector<uint8_t> data;
    uint8_t compression_type;
    uint32_t checksum;
};

// Key-Value record in a data block
struct Record {
    size_t offset_in_block;  // Offset within the block
    std::string key;
    std::string value;
    size_t total_size;       // Total bytes used by this record
};

#endif // SST_STRUCTURES_H
