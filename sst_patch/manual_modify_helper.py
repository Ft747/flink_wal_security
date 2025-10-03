#!/usr/bin/env python3
"""
Helper script for manually finding and modifying values in SST files.

This script provides utilities to:
1. Search for values in different encodings
2. Show hex context around found values
3. Generate modification commands

Usage: python manual_modify_helper.py <sst_file> <value_hex>
"""

import sys
import os
import struct
import subprocess

def search_hex_patterns(data, target_hex):
    """Search for a hex string in various encodings"""
    try:
        target_bytes = bytes.fromhex(target_hex)
    except ValueError:
        print(f"Invalid hex string: {target_hex}")
        return []

    results = []

    # Direct search
    pos = data.find(target_bytes)
    if pos != -1:
        results.append(("Exact match", pos, target_bytes))

    # If it's 8 bytes, try as two 32-bit integers
    if len(target_bytes) == 8:
        try:
            # Interpret as big-endian first
            val1, val2 = struct.unpack('>II', target_bytes)

            # Search for little-endian version
            le_pattern = struct.pack('<II', val1, val2)
            pos = data.find(le_pattern)
            if pos != -1:
                results.append(("Little-endian 32-bit integers", pos, le_pattern))

            # Search for individual values
            le_val1 = struct.pack('<I', val1)
            le_val2 = struct.pack('<I', val2)

            pos1 = data.find(le_val1)
            if pos1 != -1:
                results.append((f"First value {val1} (LE)", pos1, le_val1))

            pos2 = data.find(le_val2)
            if pos2 != -1:
                results.append((f"Second value {val2} (LE)", pos2, le_val2))
        except:
            pass

    return results

def show_context(data, pos, length, description):
    """Show hex context around a position"""
    print(f"\n{description} at offset {pos}:")
    start = max(0, pos - 16)
    end = min(len(data), pos + length + 16)

    for i in range(start, end, 16):
        chunk = data[i:i+16]
        hex_str = ' '.join(f'{b:02x}' for b in chunk)

        # Mark the target bytes
        marker = ""
        if pos >= i and pos < i + 16:
            marker = f" <-- TARGET at {pos}"

        print(f'{i:08x}: {hex_str.ljust(47)} {marker}')

def get_sst_dump_last_record(sst_file):
    """Get the last record from sst_dump"""
    try:
        result = subprocess.run(['sst_dump', '--file=' + sst_file, '--command=scan', '--output_hex'],
                               capture_output=True, text=True)
        if result.returncode != 0:
            print("Error running sst_dump:", result.stderr)
            return None, None

        lines = result.stdout.strip().split('\n')
        if not lines:
            return None, None

        last_line = lines[-1]
        if ' => ' not in last_line:
            return None, None

        # Parse format: '<key_hex>' seq:N, type:T => value_hex
        left_part, value_hex = last_line.split(' => ', 1)
        value_hex = value_hex.strip()

        # Extract key from quotes
        if "'" in left_part:
            start_quote = left_part.find("'")
            end_quote = left_part.find("'", start_quote + 1)
            if start_quote != -1 and end_quote != -1:
                key_hex = left_part[start_quote+1:end_quote]
            else:
                key_hex = None
        else:
            key_hex = None

        return key_hex, value_hex
    except Exception as e:
        print(f"Error getting sst_dump output: {e}")
        return None, None

def main():
    if len(sys.argv) < 2:
        print("Usage: python manual_modify_helper.py <sst_file> [value_hex]")
        print("\nIf value_hex is not provided, will use sst_dump to get the last record's value")
        sys.exit(1)

    sst_file = sys.argv[1]
    if not os.path.exists(sst_file):
        print(f"File not found: {sst_file}")
        sys.exit(1)

    # Get value to search for
    if len(sys.argv) >= 3:
        target_value_hex = sys.argv[2]
    else:
        print("Getting last record from sst_dump...")
        key_hex, target_value_hex = get_sst_dump_last_record(sst_file)
        if not target_value_hex:
            print("Could not get value from sst_dump")
            sys.exit(1)
        print(f"Last record value: {target_value_hex}")
        if key_hex:
            print(f"Last record key: {key_hex}")

    # Read the SST file
    with open(sst_file, 'rb') as f:
        data = f.read()

    print(f"\nFile size: {len(data)} bytes")
    print(f"Searching for value: {target_value_hex}")

    # Search for the value in different encodings
    results = search_hex_patterns(data, target_value_hex)

    if results:
        print(f"\nFound {len(results)} potential matches:")
        for i, (description, pos, pattern) in enumerate(results):
            print(f"\n{i+1}. {description}")
            show_context(data, pos, len(pattern), f"Match {i+1}")

            # Generate modification command
            print(f"\nTo modify this occurrence:")
            print(f"python -c 'import modify_sst; modify_sst.modify_sst(\"{sst_file}\", {pos}, \"<NEW_HEX_VALUE>\")'")
    else:
        print(f"\nNo matches found for {target_value_hex}")
        print("\nTrying partial searches...")

        # Try searching for parts of the value
        try:
            target_bytes = bytes.fromhex(target_value_hex)
            for i in range(1, len(target_bytes)):
                partial = target_bytes[:i]
                pos = data.find(partial)
                if pos != -1:
                    print(f"Found partial match (first {i} bytes): {partial.hex()}")
                    show_context(data, pos, len(partial), f"Partial match")
                    break
        except:
            pass

        print(f"\nManual search suggestions:")
        print(f"1. Try: hexdump -C {sst_file} | grep -i '{target_value_hex[:8]}'")
        print(f"2. Open {sst_file} in a hex editor")
        print(f"3. The value might be stored in a different encoding")

if __name__ == '__main__':
    main()
