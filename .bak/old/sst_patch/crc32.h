#ifndef CRC32_H
#define CRC32_H

#include <cstdint>

// CRC32 implementation using Castagnoli polynomial (used by RocksDB)
class CRC32 {
private:
    static constexpr uint32_t POLYNOMIAL = 0x82F63B78;
    uint32_t table[256];
    
public:
    CRC32() {
        // Initialize CRC32 lookup table
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t crc = i;
            for (int j = 0; j < 8; j++) {
                if (crc & 1) {
                    crc = (crc >> 1) ^ POLYNOMIAL;
                } else {
                    crc >>= 1;
                }
            }
            table[i] = crc;
        }
    }
    
    uint32_t calculate(const uint8_t* data, size_t length, uint32_t crc = 0) {
        for (size_t i = 0; i < length; i++) {
            crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
        }
        return crc;
    }
    
    // RocksDB uses a masked CRC32 to avoid issues with certain values
    uint32_t maskCRC(uint32_t crc) {
        return ((crc >> 15) | (crc << 17)) + 0xa282ead8;
    }
};

// VarInt encoding/decoding utilities
class VarInt {
public:
    static size_t encode(uint64_t value, uint8_t* dst) {
        size_t len = 0;
        while (value >= 128) {
            dst[len++] = (value & 0x7F) | 0x80;
            value >>= 7;
        }
        dst[len++] = static_cast<uint8_t>(value);
        return len;
    }
    
    static std::pair<uint64_t, size_t> decode(const uint8_t* src, size_t max_len) {
        uint64_t result = 0;
        size_t shift = 0;
        size_t len = 0;
        
        while (len < max_len) {
            uint8_t byte = src[len++];
            result |= static_cast<uint64_t>(byte & 0x7F) << shift;
            if (!(byte & 0x80)) {
                return {result, len};
            }
            shift += 7;
        }
        return {0, 0}; // Error
    }
};

#endif // CRC32_H
