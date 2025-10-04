# SST File Modification Tool

A C++ utility for modifying the last record value in RocksDB SST (Sorted String Table) files, specifically designed for Apache Flink savepoint SST files.

## Overview

This tool reads an SST file, modifies the value of the last key-value pair, and writes the updated file back. It's designed to work with SST files from Apache Flink savepoints that become valid SST files when renamed with the `.sst` extension.

## Features

- **In-place modification**: Updates SST files directly with atomic file operations
- **Backup support**: Creates backup files before modification
- **Hex value input**: Accepts new values as hexadecimal strings
- **Timestamp preservation**: Maintains timestamp information if present
- **Error recovery**: Automatic rollback on failure

## Building

### Prerequisites

- C++17 compatible compiler (g++)
- RocksDB library and headers (version 9.10.0 or compatible)
- Required compression libraries: snappy, lz4, zstd, bzip2, zlib

### Compilation

Using Make (recommended):
```bash
make
```

Manual compilation:
```bash
g++ -std=c++17 -Wall -Wextra -O2 \
    -I/home/dtome/.nix-profile/include \
    modify_last_record.cpp -o modify_last_record \
    -L/home/dtome/.nix-profile/lib \
    -lrocksdb -lsnappy -llz4 -lzstd -lbz2 -lz -lpthread -ldl
```

## Usage

### Direct Usage

```bash
LD_LIBRARY_PATH=/home/dtome/.nix-profile/lib \
    ./modify_last_record <sst_file_without_extension> <new_value_hex>
```

### Using Wrapper Script (Recommended)

The wrapper script provides better error handling and automatic backup:

```bash
chmod +x modify_sst.sh
./modify_sst.sh <sst_file_without_extension> <new_value_hex>
```

## Parameters

1. **sst_file_without_extension**: Path to the SST file without the `.sst` extension
   - Example: `a4208370-e176-40a6-8954-9c353fbb33bd`
   - The file should exist at this exact path

2. **new_value_hex**: New value for the last record as a hexadecimal string
   - Must be even-length (each byte requires 2 hex digits)
   - Case-insensitive
   - Example: `0000000000000000` (8 zero bytes)
   - Example: `0000000400000001` (two 4-byte integers)

## Examples

### Set last value to 8 zero bytes
```bash
./modify_sst.sh a4208370-e176-40a6-8954-9c353fbb33bd 0000000000000000
```

### Set last value to specific integers (4 bytes + 4 bytes)
```bash
./modify_sst.sh 448b2cfd-db0d-4e9f-83b8-dc78d69165ef 0000000400000001
```

### Verify modification
```bash
# Copy file with .sst extension for verification
cp a4208370-e176-40a6-8954-9c353fbb33bd a4208370-e176-40a6-8954-9c353fbb33bd.sst

# Dump last few entries to verify
sst_dump --file=a4208370-e176-40a6-8954-9c353fbb33bd.sst \
         --command=raw | tail -n 50
```

## How It Works

1. **Read Phase**: 
   - Opens the SST file using `rocksdb::SstFileReader`
   - Iterates through all key-value pairs
   - Stores entries in memory with their keys, values, and timestamps

2. **Modify Phase**:
   - Updates the last entry's value with the provided hex value
   - Preserves the key and timestamp unchanged

3. **Write Phase**:
   - Creates a temporary file using `rocksdb::SstFileWriter`
   - Writes all entries (with the modified last value)
   - Creates a backup of the original file
   - Atomically replaces the original with the modified version
   - Removes backup on success, restores on failure

## File Format Support

This tool supports SST files with:
- **Format version**: 5 (block-based format)
- **Compression**: Snappy (configurable)
- **Timestamps**: User-defined timestamps (persist_user_defined_timestamps=true)
- **Column families**: Any column family

## Error Handling

The tool performs comprehensive error checking:
- File existence validation
- Hex value format validation
- SST file format validation
- Read/write operation validation
- Automatic backup and restore on failure

## Backup Files

The tool creates backup files during operation:
- **Temporary files**: `<original_file>.tmp` (deleted after successful write)
- **Backup files**: `<original_file>.bak` (deleted after successful rename)
- **Wrapper script backups**: `<original_file>.backup.<timestamp>` (preserved)

## Limitations

- The tool modifies the last record only
- New value length can differ from original value length
- All entries must fit in memory during modification
- Requires RocksDB shared libraries at runtime

## Testing

Run the test suite:
```bash
make test
```

Test on sample files:
```bash
# Test file 1
./modify_sst.sh a4208370-e176-40a6-8954-9c353fbb33bd 0000000000000000

# Test file 2  
./modify_sst.sh 448b2cfd-db0d-4e9f-83b8-dc78d69165ef 0000000400000001
```

## Troubleshooting

### Library Not Found Error
```
error while loading shared libraries: librocksdb.so.9
```
**Solution**: Ensure `LD_LIBRARY_PATH` includes the RocksDB library directory or use the wrapper script.

### Invalid SST File Error
```
<file> is not a valid SST file
```
**Solution**: Verify the file is a valid SST file. Try renaming it with `.sst` extension and running `sst_dump --command=identify`.

### Hex Value Error
```
New value must be provided as an even-length hexadecimal string
```
**Solution**: Ensure the hex string has an even number of characters (0-9, a-f, A-F).

## Cleanup

Remove build artifacts and temporary files:
```bash
make clean
```

## License

This tool is designed for research and educational purposes in the context of Apache Flink savepoint manipulation.
