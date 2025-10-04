#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstring>
#include <iomanip>
#include <stdexcept>
#include "crc32.h"
#include "sst_structures.h"

class SSTFileModifier {
private:
    std::string file_path;
    std::vector<uint8_t> file_data;
    CRC32 crc32;
    
    // Read entire file into memory
    bool readFile() {
        std::ifstream file(file_path, std::ios::binary | std::ios::ate);
        if (!file) {
            std::cerr << "Error: Cannot open file: " << file_path << std::endl;
            return false;
        }
        
        size_t file_size = file.tellg();
        file.seekg(0, std::ios::beg);
        
        file_data.resize(file_size);
        if (!file.read(reinterpret_cast<char*>(file_data.data()), file_size)) {
            std::cerr << "Error: Failed to read file" << std::endl;
            return false;
        }
        
        std::cout << "Read file: " << file_size << " bytes" << std::endl;
        return true;
    }
    
    // Write modified data back to file
    bool writeFile() {
        std::ofstream file(file_path, std::ios::binary | std::ios::trunc);
        if (!file) {
            std::cerr << "Error: Cannot write file: " << file_path << std::endl;
            return false;
        }
        
        if (!file.write(reinterpret_cast<const char*>(file_data.data()), file_data.size())) {
            std::cerr << "Error: Failed to write file" << std::endl;
            return false;
        }
        
        std::cout << "Wrote file: " << file_data.size() << " bytes" << std::endl;
        return true;
    }
    
    // Read 64-bit little-endian integer
    uint64_t readUint64LE(size_t offset) {
        uint64_t value = 0;
        for (int i = 0; i < 8; i++) {
            value |= static_cast<uint64_t>(file_data[offset + i]) << (i * 8);
        }
        return value;
    }
    
    // Read 32-bit little-endian integer
    uint32_t readUint32LE(size_t offset) {
        uint32_t value = 0;
        for (int i = 0; i < 4; i++) {
            value |= static_cast<uint32_t>(file_data[offset + i]) << (i * 8);
        }
        return value;
    }
    
    // Write 32-bit little-endian integer
    void writeUint32LE(size_t offset, uint32_t value) {
        for (int i = 0; i < 4; i++) {
            file_data[offset + i] = (value >> (i * 8)) & 0xFF;
        }
    }
    
    // Decode a block handle (varint offset + varint size)
    BlockHandle decodeBlockHandle(size_t& offset) {
        auto [block_offset, len1] = VarInt::decode(&file_data[offset], 10);
        offset += len1;
        auto [block_size, len2] = VarInt::decode(&file_data[offset], 10);
        offset += len2;
        return {block_offset, block_size};
    }
    
    // Parse SST footer (at end of file)
    Footer parseFooter() {
        if (file_data.size() < SSTConstants::FOOTER_SIZE) {
            throw std::runtime_error("File too small to contain footer");
        }
        
        size_t footer_offset = file_data.size() - SSTConstants::FOOTER_SIZE;
        
        // Verify magic number
        uint64_t magic = readUint64LE(footer_offset + 40);
        if (magic != SSTConstants::TABLE_MAGIC_NUMBER) {
            throw std::runtime_error("Invalid SST magic number");
        }
        
        // Decode block handles
        size_t offset = footer_offset;
        BlockHandle metaindex = decodeBlockHandle(offset);
        BlockHandle index = decodeBlockHandle(offset);
        
        std::cout << "Footer parsed:" << std::endl;
        std::cout << "  Metaindex: offset=" << metaindex.offset << ", size=" << metaindex.size << std::endl;
        std::cout << "  Index: offset=" << index.offset << ", size=" << index.size << std::endl;
        
        return {metaindex, index, magic};
    }
    
    // Parse index block to get all data block handles
    std::vector<BlockHandle> parseIndexBlock(const Footer& footer) {
        std::vector<BlockHandle> data_blocks;
        size_t index_offset = footer.index_handle.offset;
        size_t index_end = index_offset + footer.index_handle.size - SSTConstants::BLOCK_TRAILER_SIZE;
        
        // Read number of restart points
        uint32_t num_restarts = readUint32LE(index_end - 4);
        size_t restart_offset = index_end - 4 - (num_restarts * 4);
        
        std::cout << "Index block: " << num_restarts << " restart points" << std::endl;
        
        // Parse each index entry
        size_t pos = index_offset;
        while (pos < restart_offset) {
            // Read shared, non_shared, value_length
            auto [shared, len1] = VarInt::decode(&file_data[pos], 10);
            pos += len1;
            auto [non_shared, len2] = VarInt::decode(&file_data[pos], 10);
            pos += len2;
            auto [value_length, len3] = VarInt::decode(&file_data[pos], 10);
            pos += len3;
            
            // Skip key
            pos += non_shared;
            
            // Parse block handle (the value)
            BlockHandle bh = decodeBlockHandle(pos);
            data_blocks.push_back(bh);
        }
        
        std::cout << "Found " << data_blocks.size() << " data blocks" << std::endl;
        return data_blocks;
    }
    
    // Parse all records in a data block
    std::vector<Record> parseDataBlock(size_t block_offset, size_t block_size) {
        std::vector<Record> records;
        size_t block_end = block_offset + block_size - SSTConstants::BLOCK_TRAILER_SIZE;
        
        // Read restart array
        uint32_t num_restarts = readUint32LE(block_end - 4);
        size_t restart_offset = block_end - 4 - (num_restarts * 4);
        
        size_t pos = block_offset;
        std::string last_key;
        
        // Parse each record
        while (pos < restart_offset) {
            size_t record_start = pos;
            
            // Read shared, non_shared, value_length
            auto [shared, len1] = VarInt::decode(&file_data[pos], 10);
            pos += len1;
            auto [non_shared, len2] = VarInt::decode(&file_data[pos], 10);
            pos += len2;
            auto [value_length, len3] = VarInt::decode(&file_data[pos], 10);
            pos += len3;
            
            // Reconstruct key (prefix compression)
            std::string key = last_key.substr(0, shared);
            key.append(reinterpret_cast<const char*>(&file_data[pos]), non_shared);
            pos += non_shared;
            
            // Read value
            std::string value(reinterpret_cast<const char*>(&file_data[pos]), value_length);
            pos += value_length;
            
            size_t record_size = pos - record_start;
            records.push_back({record_start - block_offset, key, value, record_size});
            
            last_key = key;
        }
        
        return records;
    }
    
    // Update block checksum after modification
    void updateBlockChecksum(size_t block_offset, size_t block_size) {
        // Calculate CRC32 for block data (excluding trailer)
        size_t data_size = block_size - SSTConstants::BLOCK_TRAILER_SIZE;
        uint32_t crc = crc32.calculate(&file_data[block_offset], data_size);
        
        // Include compression type byte in CRC
        uint8_t compression_type = file_data[block_offset + data_size];
        crc = crc32.calculate(&compression_type, 1);
        
        // Apply RocksDB's CRC masking
        uint32_t masked_crc = crc32.maskCRC(crc);
        
        // Write masked CRC (last 4 bytes of trailer)
        size_t crc_offset = block_offset + block_size - 4;
        writeUint32LE(crc_offset, masked_crc);
        
        std::cout << "  Updated checksum: 0x" << std::hex << masked_crc << std::dec << std::endl;
    }
    
    // Modify a record's value directly in the byte array
    bool modifyRecord(size_t block_offset, const Record& record, const std::string& new_value) {
        // Check if sizes match
        if (new_value.length() != record.value.length()) {
            std::cerr << "Error: New value size must match old value size" << std::endl;
            std::cerr << "  Old size: " << record.value.length() << std::endl;
            std::cerr << "  New size: " << new_value.length() << std::endl;
            std::cerr << "  Note: Variable-size modification requires rewriting entire block" << std::endl;
            return false;
        }
        
        // Calculate value position in file
        size_t record_pos = block_offset + record.offset_in_block;
        
        // Skip past shared_key_len, non_shared_key_len, value_len varints
        auto [shared, len1] = VarInt::decode(&file_data[record_pos], 10);
        record_pos += len1;
        auto [non_shared, len2] = VarInt::decode(&file_data[record_pos], 10);
        record_pos += len2;
        auto [value_length, len3] = VarInt::decode(&file_data[record_pos], 10);
        record_pos += len3;
        
        // Skip key bytes
        record_pos += non_shared;
        
        // Now at value position - overwrite with new value
        std::memcpy(&file_data[record_pos], new_value.data(), new_value.length());
        
        std::cout << "  Modified value at file offset " << record_pos << std::endl;
        return true;
    }

public:
    SSTFileModifier(const std::string& path) : file_path(path) {}
    
    bool modifyLastRecord(const std::string& new_value) {
        std::cout << "\n=== Binary SST File Modifier ===" << std::endl;
        std::cout << "File: " << file_path << std::endl;
        std::cout << "New value: " << new_value << std::endl;
        std::cout << std::endl;
        
        // Read file into memory
        if (!readFile()) {
            return false;
        }
        
        try {
            // Parse SST structure
            std::cout << "\nParsing SST structure..." << std::endl;
            Footer footer = parseFooter();
            
            // Find all data blocks
            std::vector<BlockHandle> data_blocks;
            if (footer.index_handle.offset == 0 && footer.index_handle.size == 0) {
                std::cout << "Single data block format detected" << std::endl;
                size_t data_size = footer.metaindex_handle.offset;
                data_blocks.push_back({0, data_size});
            } else {
                data_blocks = parseIndexBlock(footer);
            }
            
            if (data_blocks.empty()) {
                std::cerr << "Error: No data blocks found" << std::endl;
                return false;
            }
            
            // Parse last data block
            BlockHandle last_block = data_blocks.back();
            std::cout << "\nProcessing last data block:" << std::endl;
            std::cout << "  Offset: " << last_block.offset << std::endl;
            std::cout << "  Size: " << last_block.size << std::endl;
            
            std::vector<Record> records = parseDataBlock(last_block.offset, last_block.size);
            
            if (records.empty()) {
                std::cerr << "Error: Last data block is empty" << std::endl;
                return false;
            }
            
            std::cout << "  Found " << records.size() << " records in last block" << std::endl;
            
            // Modify last record
            Record& last_record = records.back();
            std::cout << "\nModifying last record:" << std::endl;
            std::cout << "  Key: " << last_record.key << std::endl;
            std::cout << "  Old value: " << last_record.value << std::endl;
            std::cout << "  New value: " << new_value << std::endl;
            
            if (!modifyRecord(last_block.offset, last_record, new_value)) {
                return false;
            }
            
            // Recalculate block checksum
            std::cout << "\nRecalculating block checksum..." << std::endl;
            updateBlockChecksum(last_block.offset, last_block.size);
            
            // Write modified file
            std::cout << "\nWriting modified file..." << std::endl;
            if (!writeFile()) {
                return false;
            }
            
            std::cout << "\n✓ Successfully modified SST file!" << std::endl;
            return true;
            
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            return false;
        }
    }
    
    void inspectFile() {
        std::cout << "\n=== SST File Inspector ===" << std::endl;
        std::cout << "File: " << file_path << std::endl << std::endl;
        
        if (!readFile()) {
            return;
        }
        
        try {
            Footer footer = parseFooter();
            
            // Check if this is a RocksDB table with properties block instead of traditional index
            std::cout << "\n=== Checking Metaindex/Properties Block ===" << std::endl;
            size_t meta_offset = footer.metaindex_handle.offset;
            size_t meta_size = footer.metaindex_handle.size;
            
            // Print some of the metaindex data as string to see what's there
            std::cout << "Properties block content (first 500 chars):" << std::endl;
            size_t print_len = std::min(static_cast<size_t>(500), meta_size);
            for (size_t i = 0; i < print_len; i++) {
                char c = file_data[meta_offset + i];
                if (c >= 32 && c <= 126) {
                    std::cout << c;
                } else if (c == 0) {
                    std::cout << "\\0";
                } else {
                    std::cout << ".";
                }
            }
            std::cout << "\n" << std::endl;
            
            // If index handle is invalid, assume single data block format
            std::vector<BlockHandle> data_blocks;
            if (footer.index_handle.offset == 0 && footer.index_handle.size == 0) {
                std::cout << "Note: Index handle is empty - assuming single data block format." << std::endl;
                // The data block is everything before the metaindex block
                // Data block starts at offset 0 and goes up to metaindex
                size_t data_size = meta_offset;
                data_blocks.push_back({0, data_size});
                std::cout << "Assuming single data block: offset=0, size=" << data_size << std::endl;
            } else {
                data_blocks = parseIndexBlock(footer);
            }
            
            std::cout << "\n=== Data Blocks ===" << std::endl;
            int total_records = 0;
            
            for (size_t i = 0; i < data_blocks.size(); i++) {
                const auto& block = data_blocks[i];
                std::cout << "\nBlock #" << (i + 1) << ":" << std::endl;
                std::cout << "  Offset: " << block.offset << std::endl;
                std::cout << "  Size: " << block.size << std::endl;
                
                std::vector<Record> records = parseDataBlock(block.offset, block.size);
                std::cout << "  Records: " << records.size() << std::endl;
                
                // Show first and last record of each block
                if (!records.empty()) {
                    std::cout << "  First key: " << records.front().key << std::endl;
                    std::cout << "  Last key: " << records.back().key << std::endl;
                }
                
                total_records += records.size();
            }
            
            std::cout << "\n=== Summary ===" << std::endl;
            std::cout << "Total blocks: " << data_blocks.size() << std::endl;
            std::cout << "Total records: " << total_records << std::endl;
            
            // Show last record details
            if (!data_blocks.empty()) {
                const auto& last_block = data_blocks.back();
                std::vector<Record> records = parseDataBlock(last_block.offset, last_block.size);
                if (!records.empty()) {
                    const auto& last_record = records.back();
                    std::cout << "\n=== Last Record ===" << std::endl;
                    std::cout << "Key: " << last_record.key << std::endl;
                    std::cout << "Value: " << last_record.value << std::endl;
                    std::cout << "Value length: " << last_record.value.length() << " bytes" << std::endl;
                }
            }
            
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }
};

void printUsage(const char* program_name) {
    std::cout << "Usage: " << program_name << " <file_path> <new_value> [--inspect]" << std::endl;
    std::cout << "\nParameters:" << std::endl;
    std::cout << "  file_path  : Path to the SST file (without .sst extension)" << std::endl;
    std::cout << "  new_value  : New value for the last key (must be same length as old value)" << std::endl;
    std::cout << "  --inspect  : Inspect file structure without modifying" << std::endl;
    std::cout << "\nExamples:" << std::endl;
    std::cout << "  " << program_name << " 448b2cfd-db0d-4e9f-83b8-dc78d69165ef new_value" << std::endl;
    std::cout << "  " << program_name << " 448b2cfd-db0d-4e9f-83b8-dc78d69165ef --inspect" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printUsage(argv[0]);
        return 1;
    }
    
    std::string file_path = argv[1];
    
    // Inspect mode
    if (argc == 3 && std::string(argv[2]) == "--inspect") {
        SSTFileModifier modifier(file_path);
        modifier.inspectFile();
        return 0;
    }
    
    // Modify mode
    if (argc != 3) {
        std::cerr << "Error: Invalid number of arguments" << std::endl;
        printUsage(argv[0]);
        return 1;
    }
    
    std::string new_value = argv[2];
    
    SSTFileModifier modifier(file_path);
    if (modifier.modifyLastRecord(new_value)) {
        return 0;
    } else {
        return 1;
    }
}
