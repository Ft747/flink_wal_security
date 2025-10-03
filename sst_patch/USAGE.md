# SST File Modifier (Python Version for Binary Modification)

This project provides tools to modify bytes directly in a RocksDB SST file and recalculate the CRC32C checksum to produce a valid SST file.

## Files

- `modify_sst.py`: Python script to modify bytes at a specific offset and update the checksum.
- `find_last_record.py`: Script to find the offset of the last record's value using sst_dump.
- `hex_dump.cpp`: Utility to dump the SST file in hex format.
- `Makefile`: Build script for C++ utilities.
- `test_modify.sh`: Script to test the modification.

## Usage

### Step 1: Find the offset of the last record

Run:
```
python find_last_record.py 000015.sst
```

This will output the key, its offset, and the value offset and length.

### Step 2: Prepare the new value

Ensure the new value has the same byte length as the original.

Convert to hex, e.g., if new value is 'modified_value', hex is 6d6f6469666965645f76616c7565 (assuming ASCII).

### Step 3: Modify the bytes

Run:
```
python modify_sst.py 000015.sst <value_offset> <new_hex>
```

Example:
```
python modify_sst.py 000015.sst 12345 6d6f646966696564
```

This modifies the bytes and updates the checksum.

### Step 4: Verify

Use sst_dump to check:
```
sst_dump --file=000015.sst --command=scan | tail -1
```

Or use hex_dump to inspect.

## Notes

- Assumes the value length remains the same; changing length requires more complex parsing.
- Uses CRC32C for checksum calculation (requires crc32c Python module).
- For full parsing, see the C++ version in modify_last_record.cpp.
