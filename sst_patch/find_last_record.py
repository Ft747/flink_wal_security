#!/usr/bin/env python3
"""
Script to find the offset of the last record's value in the SST file.

Usage: python find_last_record.py <sst_file>

This runs sst_dump to get the last key, then searches the binary file for it,
then parses to find the value position.
"""

import sys
import os
import subprocess
import struct
from typing import Tuple, Optional

HEX_DIGITS = set("0123456789abcdefABCDEF")

# SST file format constants
kBlockTrailerSize = 5  # 1 byte compression type + 4 byte checksum
kTableMagicNumber = 0x88e241b785f4cff7  # Magic number for SST files


def sanitize_hex(text: str) -> str:
    """Return only hexadecimal characters from text"""
    filtered = ''.join(ch for ch in text if ch in HEX_DIGITS)
    if len(filtered) % 2 != 0:
        # drop last nibble if odd length
        filtered = filtered[:-1]
    return filtered

def run_sst_dump_scan(sst_file):
    """Run sst_dump --command=scan and return the output lines"""
    result = subprocess.run(['sst_dump', '--file=' + sst_file, '--command=scan', '--output_hex'],
                            capture_output=True, text=True)
    if result.returncode != 0:
        print("Error running sst_dump:", result.stderr)
        sys.exit(1)
    lines = result.stdout.strip().split('\n')
    return [line for line in lines if line.strip()]

def hex_dump(data, start=0, length=64):
    """Return a hex dump of the data for debugging"""
    result = []
    for i in range(0, min(len(data), length), 16):
        chunk = data[i:i+16]
        hex_str = ' '.join(f'{b:02x}' for b in chunk)
        ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
        result.append(f'{i+start:08x}: {hex_str.ljust(47)}  {ascii_str}')
    return '\n'.join(result)

def read_fixed32(data: bytes, pos: int) -> Tuple[int, int]:
    """Read a 32-bit little-endian integer from data at pos"""
    if pos + 4 > len(data):
        raise ValueError("Not enough data to read fixed32")
    return struct.unpack('<I', data[pos:pos+4])[0], pos + 4

def read_fixed64(data: bytes, pos: int) -> Tuple[int, int]:
    """Read a 64-bit little-endian integer from data at pos"""
    if pos + 8 > len(data):
        raise ValueError("Not enough data to read fixed64")
    return struct.unpack('<Q', data[pos:pos+8])[0], pos + 8

def read_length_prefixed_slice(data: bytes, pos: int) -> Tuple[bytes, int]:
    """Read a length-prefixed slice from data at pos"""
    length, pos = read_varint(data, pos)
    if pos + length > len(data):
        raise ValueError("Not enough data to read length-prefixed slice")
    return data[pos:pos+length], pos + length

def parse_block_handle(data: bytes, pos: int) -> Tuple[Tuple[int, int], int]:
    """Parse a BlockHandle from data at pos, return (offset, size) and new pos"""
    offset, pos = read_varint(data, pos)
    size, pos = read_varint(data, pos)
    return (offset, size), pos

def find_last_record(sst_file: str) -> Optional[Tuple[bytes, bytes, int, int]]:
    """Try to find the last key-value record in the SST file using basic parsing"""
    with open(sst_file, 'rb') as f:
        data = f.read()

    # Check magic number at the end of the file
    if len(data) < 8:
        print("File too small to be a valid SST file")
        return None

    magic = struct.unpack('<Q', data[-8:])[0]
    if magic != kTableMagicNumber:
        print(f"Invalid SST file: magic number {magic:016x} doesn't match expected {kTableMagicNumber:016x}")
        print("Note: This might be a different version of RocksDB or corrupted file")
        return None

    print("SST file structure parsing is complex - falling back to sst_dump")
    return None

def search_value_in_file(sst_file, value_bytes):
    """Search for value bytes in the SST file using multiple strategies"""
    with open(sst_file, 'rb') as f:
        data = f.read()

    print(f"\nSearching for value: {value_bytes.hex()}")

    # Strategy 1: Direct search
    pos = data.find(value_bytes)
    if pos != -1:
        print(f"Found exact match at offset {pos}")
        return pos

    # Strategy 2: Check if value might be stored as little-endian integers
    if len(value_bytes) == 8:
        import struct
        # Try as two 32-bit little-endian integers
        try:
            val1, val2 = struct.unpack('<II', value_bytes)
            print(f"Value interpreted as two LE32: {val1}, {val2}")

            # Search for this pattern
            pattern = struct.pack('<II', val1, val2)
            pos = data.find(pattern)
            if pos != -1:
                print(f"Found LE32 pattern at offset {pos}")
                return pos
        except:
            pass

    # Strategy 3: Search for individual bytes/patterns
    print("Exact match not found")
    return -1

def read_varint(data, pos):
    """Read varint from data at pos, return value and new pos"""
    value = 0
    shift = 0
    while True:
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7f) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return value, pos

def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)

    sst_file = sys.argv[1]
    if not os.path.exists(sst_file):
        print(f"File {sst_file} does not exist")
        sys.exit(1)

    print(f"Analyzing SST file: {sst_file}")

    # Try to find the last record by parsing the SST file structure
    result = find_last_record(sst_file)
    if result:
        key, value, key_offset, value_offset = result
        print(f"\nLast record found by parsing SST structure:")
        print(f"Key (offset {key_offset}): {key.hex()}")
        print(f"Value (offset {value_offset}, length {len(value)}): {value.hex()}")
        sys.exit(0)

    # Use sst_dump to extract the last record
    print("\nUsing sst_dump to find last record...")
    records = run_sst_dump_scan(sst_file)
    if not records:
        print("No records found")
        sys.exit(1)

    last_line = records[-1]
    if ' => ' not in last_line:
        print("Unexpected format:", repr(last_line))
        sys.exit(1)

    # Parse sst_dump format: '<key_hex>' seq:N, type:T => value_hex
    left_part, value_hex = last_line.split(' => ', 1)
    value_hex = sanitize_hex(value_hex)

    # Extract key from the quoted part
    if "'" in left_part:
        # Find the key within quotes
        start_quote = left_part.find("'")
        end_quote = left_part.find("'", start_quote + 1)
        if start_quote != -1 and end_quote != -1:
            key_hex = left_part[start_quote+1:end_quote]
        else:
            # Fallback to sanitizing the whole left part
            key_hex = sanitize_hex(left_part)
    else:
        # Fallback to sanitizing the whole left part
        key_hex = sanitize_hex(left_part)

    try:
        key = bytes.fromhex(key_hex)
        value = bytes.fromhex(value_hex)
        print(f"\nLast record from sst_dump:")
        print(f"Key: {key_hex}")
        print(f"Value: {value_hex}")

        # Try to find the raw bytes in the file
        with open(sst_file, 'rb') as f:
            data = f.read()

        # Search for the value in the file
        value_pos = search_value_in_file(sst_file, value)
        if value_pos != -1:
            print(f"\nSUCCESS: Found value at offset {value_pos}")
            print(f"Value: {value_hex}")
            print(f"Length: {len(value)} bytes")
            print("\nTo modify this value, run:")
            print(f"python -c 'import modify_sst; modify_sst.modify_sst(\"{sst_file}\", {value_pos}, \"{value_hex.upper()}\")'")
        else:
            print(f"\nValue not found in file.")
            print(f"The value {value_hex} from sst_dump might be encoded differently in the raw file.")
            print(f"You may need to:")
            print(f"1. Use a hex editor to manually locate and modify the value")
            print(f"2. Examine the file with: hexdump -C {sst_file} | grep -i '<pattern>'")
            print(f"3. Try different interpretations of the value {value_hex}")

    except ValueError as e:
        print(f"Error parsing hex: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
