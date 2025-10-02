# SST File Modifier - Quick Start Guide

## Overview

This tool performs **direct binary modification** on SST files from Apache Flink savepoints by:
- Parsing the SST file format at the byte level
- Modifying the last record's value in-place
- Recalculating CRC32 checksums to maintain integrity

## Quick Start

### 1. Build the Program

```bash
make
```

### 2. Inspect an SST File

```bash
./modify_last_record <file_path> --inspect
```

Example:
```bash
./modify_last_record 448b2cfd-db0d-4e9f-83b8-dc78d69165ef --inspect
```

This will show:
- Number of data blocks
- Number of records
- Last record's key and value
- Value length (important for modification)

### 3. Modify the Last Record

⚠️ **IMPORTANT**: The new value must be **exactly the same length** as the old value.

```bash
./modify_last_record <file_path> "<new_value>"
```

Example workflow:
```bash
# Step 1: Inspect to see value length
./modify_last_record 448b2cfd-db0d-4e9f-83b8-dc78d69165ef --inspect | grep "Value length"
# Output: Value length: 8488 bytes

# Step 2: Create backup
cp 448b2cfd-db0d-4e9f-83b8-dc78d69165ef 448b2cfd-db0d-4e9f-83b8-dc78d69165ef.backup

# Step 3: Generate a value of exactly 8488 bytes
python3 -c "print('X' * 8488, end='')" > new_value.txt

# Step 4: Modify the file
./modify_last_record 448b2cfd-db0d-4e9f-83b8-dc78d69165ef "$(cat new_value.txt)"
```

## What Happens During Modification

1. **File Loading**: Reads entire SST file into memory
2. **Structure Parsing**: 
   - Parses footer (48 bytes at end)
   - Identifies data blocks
   - Locates last record in last data block
3. **Binary Modification**: 
   - Finds exact byte offset of the value
   - Overwrites value bytes directly
4. **Checksum Update**:
   - Calculates CRC32 for modified block
   - Updates 4-byte checksum in block trailer
5. **File Writing**: Writes modified bytes back to disk

## Output Example

```
=== Binary SST File Modifier ===
File: 448b2cfd-db0d-4e9f-83b8-dc78d69165ef
New value: MODIFIED_VALUE_...

Read file: 197060 bytes

Parsing SST structure...
Footer parsed:
  Metaindex: offset=194567, size=1474
  Index: offset=0, size=0
Single data block format detected

Processing last data block:
  Offset: 0
  Size: 194567
  Found 179 records in last block

Modifying last record:
  Key: KxnfrGP
  Old value: OMc...
  New value: MODIFIED_VALUE_...
  Modified value at file offset 186079

Recalculating block checksum...
  Updated checksum: 0xe5b329a8

Writing modified file...
Wrote file: 197060 bytes

✓ Successfully modified SST file!
```

## File Structure

```
/home/dtome/Documents/School/Thesis/code/sst_patch/
├── modify_last_record.cpp    # Main program
├── crc32.h                    # CRC32 and varint utilities
├── sst_structures.h           # SST format structures
├── Makefile                   # Build configuration
├── README.md                  # Full documentation
├── USAGE.md                   # This file
└── test_modify.sh             # Automated test script
```

## Testing

Run the automated test:
```bash
./test_modify.sh
```

This will:
1. Inspect the original file
2. Create a backup
3. Modify the last record with a test value
4. Verify the modification
5. Restore the original file

## Important Notes

### Same-Size Constraint

This tool only supports **same-size value replacement** because:
- It modifies bytes in-place without restructuring the block
- Different-sized values would require rewriting the entire data block
- This keeps the operation simple, fast, and safe

### Checksum Integrity

The tool uses:
- **CRC32-Castagnoli polynomial** (0x82F63B78)
- **RocksDB's CRC masking** transformation
- Ensures the modified file passes RocksDB's integrity checks

### No Dependencies

This implementation requires **zero external libraries**:
- No RocksDB library needed
- Only C++17 standard library
- Portable across Linux systems

## Troubleshooting

### "New value size must match old value size"
Use `--inspect` to check the current value length and ensure your new value is exactly the same number of bytes.

### "Invalid SST magic number"
The file is not a valid RocksDB v2 SST file.

### File Corruption
Always create a backup before modification. If something goes wrong, restore from backup:
```bash
cp yourfile.backup yourfile
```

## Advanced Usage

### Programmatic Usage

You can integrate this into scripts:

```bash
#!/bin/bash
FILE="path/to/sst/file"
NEW_VALUE="..."  # Must be exact same length

# Backup
cp "$FILE" "$FILE.backup"

# Modify
if ./modify_last_record "$FILE" "$NEW_VALUE"; then
    echo "Success!"
else
    echo "Failed, restoring backup"
    mv "$FILE.backup" "$FILE"
fi
```

### Batch Processing

Modify multiple SST files:

```bash
for file in /path/to/savepoint/*; do
    echo "Processing $file"
    ./modify_last_record "$file" "$NEW_VALUE"
done
```

## Support

For issues or questions, refer to the full README.md or examine the source code comments.
