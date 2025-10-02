cmake_minimum_required(VERSION 3.10)
project(SSTModifier)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find RocksDB
find_package(RocksDB REQUIRED)

# Add executable
add_executable(modify_last_record modify_last_record.cpp)

# Link RocksDB
target_link_libraries(modify_last_record RocksDB::rocksdb)

# If RocksDB is not found via find_package, use pkg-config
if(NOT RocksDB_FOUND)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(ROCKSDB REQUIRED rocksdb)
    
    target_include_directories(modify_last_record PRIVATE ${ROCKSDB_INCLUDE_DIRS})
    target_link_libraries(modify_last_record ${ROCKSDB_LIBRARIES})
endif()
