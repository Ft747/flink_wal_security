CXX = g++
CXXFLAGS = -std=c++17 -Wall -O2 -I.
LDFLAGS = 

TARGET = modify_last_record
SRC = modify_last_record.cpp
HEADERS = crc32.h sst_structures.h

all: $(TARGET)

$(TARGET): $(SRC) $(HEADERS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
