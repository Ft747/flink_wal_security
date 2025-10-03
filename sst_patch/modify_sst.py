#!/usr/bin/env python3
"""
Script to modify bytes in an SST file and recalculate the checksum.

Usage: python modify_sst.py <sst_file> <offset> <new_bytes_hex>

Example: python modify_sst.py 000015.sst 100 'deadbeef'

This modifies the bytes at offset 100 to the hex value deadbeef, keeping the same length.
Then recalculates the CRC32C checksum.
"""

import sys
import os
import struct
import crc32c

def read_footer(sst_file):
    """Read the footer to get current checksum"""
    with open(sst_file, 'rb') as f:
        f.seek(-8, 2)  # last 8 bytes: padding/magic + checksum
        footer = f.read(8)
        checksum = struct.unpack('<I', footer[4:])[0]  # little endian uint32
        return checksum

def calculate_checksum(sst_file):
    """Calculate CRC32C of the file except the last 8 bytes"""
    with open(sst_file, 'rb') as f:
        data = f.read()
        if len(data) < 8:
            raise ValueError("File too small")
        return crc32c.crc32c(data[:-8])

def modify_bytes(sst_file, offset, new_bytes):
    """Modify bytes at offset in the file"""
    with open(sst_file, 'r+b') as f:
        f.seek(offset)
        f.write(new_bytes)

def update_checksum(sst_file):
    """Recalculate and update the checksum"""
    new_checksum = calculate_checksum(sst_file)
    with open(sst_file, 'r+b') as f:
        f.seek(-4, 2)  # last 4 bytes are checksum
        f.write(struct.pack('<I', new_checksum))

def main():
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)

    sst_file = sys.argv[1]
    offset = int(sys.argv[2])
    new_bytes_hex = sys.argv[3]

    if not os.path.exists(sst_file):
        print(f"File {sst_file} does not exist")
        sys.exit(1)

    # Convert hex to bytes
    try:
        new_bytes = bytes.fromhex(new_bytes_hex)
    except ValueError:
        print("Invalid hex string")
        sys.exit(1)

    # Read original checksum
    orig_checksum = read_footer(sst_file)
    print(f"Original checksum: {orig_checksum:08x}")

    # Modify
    modify_bytes(sst_file, offset, new_bytes)
    print(f"Modified {len(new_bytes)} bytes at offset {offset}")

    # Update checksum
    update_checksum(sst_file)
    new_checksum = read_footer(sst_file)
    print(f"New checksum: {new_checksum:08x}")

    # Verify
    calc_checksum = calculate_checksum(sst_file)
    if calc_checksum == new_checksum:
        print("Checksum verification passed")
    else:
        print(f"Checksum mismatch: calculated {calc_checksum:08x}")

if __name__ == '__main__':
    main()
