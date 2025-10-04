CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
ROCKSDB_INCLUDE = /home/dtome/.nix-profile/include
ROCKSDB_LIB = /home/dtome/.nix-profile/lib
LDFLAGS = -L$(ROCKSDB_LIB) -lrocksdb -lsnappy -llz4 -lzstd -lbz2 -lz -lpthread -ldl

TARGET = modify_last_record
SOURCE = modify_last_record.cpp

.PHONY: all clean test

all: $(TARGET)

$(TARGET): $(SOURCE)
	$(CXX) $(CXXFLAGS) -I$(ROCKSDB_INCLUDE) $< -o $@ $(LDFLAGS)

clean:
	rm -f $(TARGET) *.tmp *.bak

test: $(TARGET)
	@echo "Testing SST modification tool..."
	@LD_LIBRARY_PATH=$(ROCKSDB_LIB) ./$(TARGET) || echo "Usage: ./$(TARGET) <sst_file> <value_hex>"
