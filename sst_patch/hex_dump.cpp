#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <cstdint>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <file> [bytes_from_end]" << std::endl;
        return 1;
    }
    
    std::string filename = argv[1];
    size_t bytes_from_end = argc > 2 ? std::stoul(argv[2]) : 100;
    
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file) {
        std::cerr << "Cannot open file" << std::endl;
        return 1;
    }
    
    size_t file_size = file.tellg();
    size_t start_offset = file_size > bytes_from_end ? file_size - bytes_from_end : 0;
    
    file.seekg(start_offset);
    
    std::vector<uint8_t> data(bytes_from_end);
    file.read(reinterpret_cast<char*>(data.data()), bytes_from_end);
    size_t bytes_read = file.gcount();
    
    std::cout << "File size: " << file_size << " bytes" << std::endl;
    std::cout << "Reading last " << bytes_read << " bytes starting at offset " << start_offset << std::endl;
    std::cout << std::endl;
    
    for (size_t i = 0; i < bytes_read; i++) {
        if (i % 16 == 0) {
            std::cout << std::setfill('0') << std::setw(8) << std::hex << (start_offset + i) << ": ";
        }
        std::cout << std::setfill('0') << std::setw(2) << std::hex << static_cast<int>(data[i]) << " ";
        if ((i + 1) % 16 == 0) {
            std::cout << " | ";
            for (size_t j = i - 15; j <= i; j++) {
                char c = data[j];
                std::cout << (c >= 32 && c <= 126 ? c : '.');
            }
            std::cout << std::endl;
        }
    }
    
    if (bytes_read % 16 != 0) {
        size_t remaining = 16 - (bytes_read % 16);
        for (size_t i = 0; i < remaining; i++) std::cout << "   ";
        std::cout << " | ";
        for (size_t j = (bytes_read / 16) * 16; j < bytes_read; j++) {
            char c = data[j];
            std::cout << (c >= 32 && c <= 126 ? c : '.');
        }
        std::cout << std::endl;
    }
    
    return 0;
}
