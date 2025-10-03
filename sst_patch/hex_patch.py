#!/usr/bin/env python3
"""
Simple hex patcher for binary files.
Modifies bytes at specific offsets without touching checksums.

Usage: python hex_patch.py <file> <offset> <hex_bytes>

Example: python hex_patch.py 000015.sst 6253 05000000

This directly modifies the bytes at the given offset.
Use with caution - make backups first!
"""

import sys
import os

def hex_patch(filename, offset, hex_bytes):
    """Patch bytes at offset in file"""
    if not os.path.exists(filename):
        print(f"Error: File '{filename}' not found")
        return False

    try:
        new_bytes = bytes.fromhex(hex_bytes)
    except ValueError as e:
        print(f"Error: Invalid hex string '{hex_bytes}': {e}")
        return False

    try:
        with open(filename, 'r+b') as f:
            # Get file size
            f.seek(0, 2)
            file_size = f.tell()

            if offset < 0 or offset >= file_size:
                print(f"Error: Offset {offset} is out of range (file size: {file_size})")
                return False

            if offset + len(new_bytes) > file_size:
                print(f"Error: Patch would extend beyond file (offset {offset} + {len(new_bytes)} bytes > {file_size})")
                return False

            # Read original bytes for confirmation
            f.seek(offset)
            original_bytes = f.read(len(new_bytes))

            print(f"File: {filename}")
            print(f"Offset: {offset} (0x{offset:X})")
            print(f"Original bytes: {original_bytes.hex().upper()}")
            print(f"New bytes:      {new_bytes.hex().upper()}")
            print(f"Length: {len(new_bytes)} bytes")

            # Write new bytes
            f.seek(offset)
            f.write(new_bytes)

            print("✓ Patch applied successfully")
            return True

    except Exception as e:
        print(f"Error: Failed to patch file: {e}")
        return False

def show_context(filename, offset, length=16):
    """Show hex context around an offset"""
    try:
        with open(filename, 'rb') as f:
            start = max(0, offset - length)
            f.seek(start)
            data = f.read(length * 2 + 16)  # Read extra for context

            print(f"\nHex context around offset {offset} (0x{offset:X}):")

            for i in range(0, len(data), 16):
                chunk_offset = start + i
                chunk = data[i:i+16]
                hex_str = ' '.join(f'{b:02x}' for b in chunk)

                # Mark the target offset
                marker = ""
                if chunk_offset <= offset < chunk_offset + 16:
                    marker = f" <-- OFFSET {offset}"

                print(f"{chunk_offset:08x}: {hex_str.ljust(47)}{marker}")

    except Exception as e:
        print(f"Error showing context: {e}")

def main():
    if len(sys.argv) != 4:
        print(__doc__)
        print("\nExamples:")
        print("  python hex_patch.py data.bin 100 deadbeef")
        print("  python hex_patch.py 000015.sst 6253 05000000")
        sys.exit(1)

    filename = sys.argv[1]

    try:
        offset = int(sys.argv[2])
    except ValueError:
        try:
            offset = int(sys.argv[2], 16)  # Try hex
        except ValueError:
            print(f"Error: Invalid offset '{sys.argv[2]}'. Use decimal or hex (0x...)")
            sys.exit(1)

    hex_bytes = sys.argv[3].replace(' ', '').replace(':', '')

    # Show context before patching
    if os.path.exists(filename):
        show_context(filename, offset)

    # Apply patch
    if hex_patch(filename, offset, hex_bytes):
        print(f"\n✓ Successfully patched {filename}")

        # Show context after patching
        show_context(filename, offset)
    else:
        print(f"\n✗ Failed to patch {filename}")
        sys.exit(1)

if __name__ == '__main__':
    main()
