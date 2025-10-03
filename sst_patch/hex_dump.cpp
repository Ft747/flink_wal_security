#include <iostream>
#include <fstream>
#include <vector>
#include <iomanip>

void hex_dump(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

    std::vector<char> buffer(16);
    size_t offset = 0;

    while (file.read(buffer.data(), buffer.size())) {
        std::cout << std::hex << std::setfill('0') << std::setw(8) << offset << ": ";
        for (size_t i = 0; i < buffer.size(); ++i) {
            if (i == 8) std::cout << " ";
            std::cout << std::setw(2) << (static_cast<unsigned int>(buffer[i]) & 0xFF) << " ";
        }
        std::cout << " |";
        for (char c : buffer) {
            if (std::isprint(static_cast<unsigned char>(c))) {
                std::cout << c;
            } else {
                std::cout << ".";
            }
        }
        std::cout << "|" << std::endl;
        offset += buffer.size();
    }

    // Handle remaining bytes
    std::streamsize bytes_read = file.gcount();
    if (bytes_read > 0) {
        std::cout << std::hex << std::setfill('0') << std::setw(8) << offset << ": ";
        for (size_t i = 0; i < bytes_read; ++i) {
            if (i == 8) std::cout << " ";
            std::cout << std::setw(2) << (static_cast<unsigned int>(buffer[i]) & 0xFF) << " ";
        }
        for (size_t i = bytes_read; i < buffer.size(); ++i) {
            if (i == 8) std::cout << " ";
            std::cout << "   ";
        }
        std::cout << " |";
        for (size_t i = 0; i < bytes_read; ++i) {
            char c = buffer[i];
            if (std::isprint(static_cast<unsigned char>(c))) {
                std::cout << c;
            } else {
                std::cout << ".";
            }
        }
        std::cout << "|" << std::endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <filename>" << std::endl;
        return 1;
    }
    hex_dump(argv[1]);
    return 0;
}
