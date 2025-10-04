# SST File Modifier (Binary Edition)

A C++ program that performs **direct binary modification** on SST files generated from Apache Flink savepoints. This implementation parses the SST file format at the byte level, modifies values directly in memory, and recalculates checksums to maintain file integrity.

## Features

- **Direct Binary Modification**: Edits SST files at the byte level without RocksDB dependencies
- **CRC32 Checksum Recalculation**: Automatically recalculates and updates block checksums using Castagnoli polynomial
- **SST Format Parsing**: Parses footer, index blocks, and data blocks directly from binary
- **Zero Dependencies**: Only requires C++17 standard library (no external libraries needed)
- **Inspection Mode**: View SST file structure and contents without modification
- **Safe Same-Size Replacement**: Modifies values without rewriting entire blocks

## Prerequisites

### Required

- C++17 compatible compiler (g++ 7.0+ or clang++ 5.0+)
- Make (for building)

### No External Libraries Required!

This implementation has **zero external dependencies** - it performs all SST parsing, CRC calculation, and modification using only the C++ standard library.

## Building the Program

### Option 1: Using Make (Recommended)

```bash
make
```

### Option 2: Manual Compilation

```bash
g++ -std=c++17 -O2 -I. -o modify_last_record modify_last_record.cpp
```

## Usage

### Basic Usage

```bash
./modify_last_record <file_path> <new_value>
```

**Parameters:**
- `file_path`: Path to the SST file (without .sst extension)
- `new_value`: New value for the last key in the SST file

**⚠️ IMPORTANT CONSTRAINT:**
The new value **must be exactly the same length** as the old value. This binary modification approach overwrites bytes in-place without restructuring the block. Different-sized values would require rewriting the entire data block.

**Example:**
```bash
# First, inspect to see the current value length
./modify_last_record 448b2cfd-db0d-4e9f-83b8-dc78d69165ef --inspect

# Then modify with same-length value
./modify_last_record 448b2cfd-db0d-4e9f-83b8-dc78d69165ef "new_value_here"
```

### Inspection Mode

To inspect the contents of an SST file without modifying it:

```bash
./modify_last_record <file_path> --inspect
```

**Example:**
```bash
./modify_last_record 448b2cfd-db0d-4e9f-83b8-dc78d69165ef --inspect
```

## How It Works

This program performs **direct binary manipulation** of SST files:

1. **File Reading**: Loads the entire SST file into memory as a byte array
2. **Footer Parsing**: Reads the 48-byte footer at the end of the file to locate the index block
   - Validates magic number (0x88e241b785f4cff7)
   - Extracts metaindex and index block handles (offset + size)
3. **Index Block Parsing**: Reads the index block to find all data block locations
   - Decodes varint-encoded block handles
   - Builds a list of data block offsets and sizes
4. **Data Block Parsing**: Parses the last data block to extract all key-value records
   - Handles prefix-compressed keys
   - Decodes varint-encoded record headers
   - Locates the last record's value bytes
5. **Binary Modification**: Directly overwrites the value bytes in memory
   - Requires same-length values (no block restructuring)
   - Preserves all other data unchanged
6. **Checksum Recalculation**: Computes new CRC32 checksum for the modified block
   - Uses Castagnoli polynomial (0x82F63B78)
   - Applies RocksDB's CRC masking transformation
   - Updates the 4-byte checksum in the block trailer
7. **File Writing**: Writes the modified byte array back to disk

## Examples

### Example 1: Modify Last Record

```bash
# Modify the last record's value
./modify_last_record 448b2cfd-db0d-4e9f-83b8-dc78d69165ef "updated_value"
```

Output:
```
SST File Modifier
=================
File: 448b2cfd-db0d-4e9f-83b8-dc78d69165ef
New value: updated_value

Created SST file: 448b2cfd-db0d-4e9f-83b8-dc78d69165ef.sst
Reading records from SST file...
Found 100 records
Modified last record with key: last_key_example
New value: updated_value
Writing records to new SST file...
Successfully wrote new SST file
Replaced original file: 448b2cfd-db0d-4e9f-83b8-dc78d69165ef
Successfully modified SST file!
```

### Example 2: Inspect File

```bash
# View contents of the SST file
./modify_last_record 448b2cfd-db0d-4e9f-83b8-dc78d69165ef --inspect
```

Output:
```
Inspecting SST file: 448b2cfd-db0d-4e9f-83b8-dc78d69165ef
Created SST file: 448b2cfd-db0d-4e9f-83b8-dc78d69165ef.sst

=== SST File Contents ===
Record #1:
  Key: key1
  Value: value1
Record #2:
  Key: key2
  Value: value2
...
Total records: 100
```

## Error Handling

The program includes comprehensive error handling for:
- Missing or invalid file paths
- Corrupted SST files
- File I/O errors
- RocksDB operation failures

## Technical Details

### SST File Format (RocksDB v2)

SST (Sorted String Table) files are immutable data files used by RocksDB and LevelDB:

```
[Data Block 1]
[Data Block 2]
...
[Data Block N]
[Meta Block]
[Metaindex Block]
[Index Block]
[Footer (48 bytes)]
```

**Block Structure:**
- Each block contains multiple key-value records
- Records use prefix compression for keys
- Block trailer: 1 byte compression type + 4 bytes CRC32 checksum

**Record Format:**
```
[shared_key_len: varint]
[non_shared_key_len: varint]
[value_len: varint]
[non_shared_key: bytes]
[value: bytes]
```

### Implementation Notes

- **No RocksDB Dependency**: Pure C++ implementation parsing binary format directly
- **CRC32-Castagnoli**: Uses the same polynomial as RocksDB (0x82F63B78)
- **In-Memory Operation**: Loads entire file for safe atomic updates
- **Varint Decoding**: Custom implementation for reading variable-length integers
- **Same-Size Constraint**: Only modifies existing bytes without block restructuring
- **Checksum Integrity**: Automatically maintains block checksums after modification

## Troubleshooting

### "Invalid SST magic number"

The file is not a valid SST file or uses a different format version. This program supports RocksDB v2 format (magic: 0x88e241b785f4cff7).

### "New value size must match old value size"

This binary modification approach requires same-length values. Use `--inspect` to check the current value length first:

```bash
./modify_last_record <file> --inspect
```

Then ensure your new value is exactly the same number of bytes.

### "File too small to contain footer"

The file is corrupted or truncated. SST files must be at least 48 bytes (footer size).

### Compilation errors

Ensure you have C++17 support:
```bash
g++ --version  # Should be 7.0 or higher
```

## License

This program is provided as-is for working with Apache Flink savepoint SST files.

## Contributing

Feel free to submit issues or pull requests for improvements.
