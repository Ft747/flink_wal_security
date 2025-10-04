#!/usr/bin/env python3
"""
Increment the last value in an SST file by parsing it as a hex-encoded integer.
Handles proper padding to maintain the same value length.
"""

import sys
import subprocess
import re
from datetime import datetime

def get_current_value_info(filepath):
    """Extract current value and its length from SST file."""
    try:
        result = subprocess.run(
            ['./modify_last_record', filepath, '--inspect'],
            capture_output=True
        )
        # Decode with error handling for binary data
        output = result.stdout.decode('utf-8', errors='replace') + result.stderr.decode('utf-8', errors='replace')
        
        # Extract value length
        length_match = re.search(r'Value length:\s*(\d+)', output)
        if not length_match:
            print("Error: Could not find value length in output")
            return None, None
        
        value_length = int(length_match.group(1))
        
        # Try to extract the value - look for hex pattern after "Old value:"
        # The value appears after "Old value:" line
        lines = output.split('\n')
        value_start_idx = None
        for i, line in enumerate(lines):
            if 'Old value:' in line:
                value_start_idx = i + 1
                break
        
        if value_start_idx is None:
            print("Error: Could not find 'Old value:' in output")
            return None, None
        
        # Get the next line which should contain the value
        if value_start_idx < len(lines):
            value_line = lines[value_start_idx].strip()
            # Extract hex-like pattern (alphanumeric at start)
            hex_match = re.match(r'^([0-9a-fA-F]+)', value_line)
            if hex_match:
                current_hex = hex_match.group(1)
                return current_hex, value_length
        
        print("Error: Could not extract current value")
        return None, None
        
    except Exception as e:
        print(f"Error inspecting file: {e}")
        return None, None

def increment_hex_value(hex_str, target_length):
    """Increment a hex string value and pad to target length."""
    try:
        # Parse hex as integer
        int_value = int(hex_str, 16)
        print(f"Current value (hex): {hex_str}")
        print(f"Current value (dec): {int_value}")
        
        # Increment
        new_int_value = int_value + 1
        print(f"New value (dec): {new_int_value}")
        
        # Convert back to hex (without 0x prefix)
        new_hex = format(new_int_value, 'x')
        
        # Ensure it fits in the same number of hex digits
        hex_digits = len(hex_str)
        new_hex = new_hex.zfill(hex_digits)
        
        print(f"New value (hex): {new_hex}")
        
        # Create padded value
        new_value_bytes = new_hex.encode('ascii')
        padding_needed = target_length - len(new_value_bytes)
        
        if padding_needed < 0:
            print(f"Warning: New hex value is longer than original!")
            return None
        
        # Pad with null bytes
        padded_value = new_value_bytes + (b'\x00' * padding_needed)
        
        print(f"Value length: {len(padded_value)} bytes (target: {target_length})")
        
        return padded_value
        
    except ValueError as e:
        print(f"Error parsing hex value: {e}")
        return None

def modify_sst_file(filepath, new_value_bytes):
    """Call modify_last_record with the new value."""
    try:
        # Create backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{filepath}.backup.{timestamp}"
        subprocess.run(['cp', filepath, backup_file], check=True)
        print(f"Backup created: {backup_file}")
        
        # Modify file
        result = subprocess.run(
            ['./modify_last_record', filepath],
            input=new_value_bytes,
            capture_output=True
        )
        
        if result.returncode == 0:
            print("\n✓ Success! File modified.")
            print(f"  Backup: {backup_file}")
            return True
        else:
            print("\n✗ Modification failed")
            print(result.stdout.decode('utf-8', errors='ignore'))
            print(result.stderr.decode('utf-8', errors='ignore'))
            # Restore backup
            subprocess.run(['cp', backup_file, filepath])
            print("Original file restored")
            return False
            
    except Exception as e:
        print(f"Error modifying file: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: ./increment_hex_value.py <file_path>")
        print()
        print("This script:")
        print("  1. Reads the last value from the SST file")
        print("  2. Parses it as a hex-encoded integer")
        print("  3. Increments it by 1")
        print("  4. Writes it back with null-byte padding")
        print()
        print("Example:")
        print("  ./increment_hex_value.py 448b2cfd-db0d-4e9f-83b8-dc78d69165ef")
        sys.exit(1)
    
    filepath = sys.argv[1]
    
    print("=== SST Hex Value Incrementer ===")
    print(f"File: {filepath}\n")
    
    # Step 1: Get current value
    print("Step 1: Reading current value...")
    current_hex, value_length = get_current_value_info(filepath)
    
    if current_hex is None or value_length is None:
        print("Failed to read current value")
        sys.exit(1)
    
    print()
    
    # Step 2: Increment value
    print("Step 2: Incrementing value...")
    new_value_bytes = increment_hex_value(current_hex, value_length)
    
    if new_value_bytes is None:
        print("Failed to create new value")
        sys.exit(1)
    
    print()
    
    # Step 3: Modify file
    print("Step 3: Modifying file...")
    success = modify_sst_file(filepath, new_value_bytes)
    
    if not success:
        sys.exit(1)

if __name__ == '__main__':
    main()
