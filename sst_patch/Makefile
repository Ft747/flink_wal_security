CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
LDFLAGS = -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd

all: modify_last_record hex_dump

hex_dump: hex_dump.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

modify_last_record: modify_last_record.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f modify_last_record hex_dump
