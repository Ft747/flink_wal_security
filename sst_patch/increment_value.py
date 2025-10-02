#!/usr/bin/env python3
"""
Increment the last value in an SST file.
User provides the current value, script handles padding and modification.
"""

import sys
import subprocess
import re
from datetime import datetime

def get_value_length(filepath):
    """Get the value length from SST file."""
    try:
        result = subprocess.run(
            ['./modify_last_record', filepath, '--inspect'],
            capture_output=True
        )
        output = result.stdout.decode('utf-8', errors='replace') + result.stderr.decode('utf-8', errors='replace')
        
        length_match = re.search(r'Value length:\s*(\d+)', output)
        if length_match:
            return int(length_match.group(1))
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def increment_hex_value(hex_str, target_length):
    """Increment a hex string value and pad to target length."""
    try:
        # Remove any whitespace or non-hex characters
        hex_str = ''.join(c for c in hex_str if c in '0123456789abcdefABCDEF')
        
        # Parse hex as integer
        int_value = int(hex_str, 16)
        print(f"  Current value (hex): {hex_str}")
        print(f"  Current value (dec): {int_value}")
        
        # Increment
        new_int_value = int_value + 1
        print(f"  New value (dec): {new_int_value}")
        
        # Convert back to hex (maintain same number of digits)
        hex_digits = len(hex_str)
        new_hex = format(new_int_value, f'0{hex_digits}x')
        print(f"  New value (hex): {new_hex}")
        
        # Create padded value
        new_value_bytes = new_hex.encode('ascii')
        padding_needed = target_length - len(new_value_bytes)
        
        if padding_needed < 0:
            print(f"  Error: New hex value is too long!")
            return None
        
        # Pad with null bytes
        padded_value = new_value_bytes + (b'\x00' * padding_needed)
        print(f"  Padded length: {len(padded_value)} bytes")
        
        return padded_value
        
    except ValueError as e:
        print(f"  Error parsing hex value: {e}")
        return None

def modify_sst_file(filepath, new_value_bytes):
    """Call modify_last_record with the new value."""
    try:
        # Create backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{filepath}.backup.{timestamp}"
        subprocess.run(['cp', filepath, backup_file], check=True)
        print(f"  Backup created: {backup_file}")
        
        # Write new value to temp file to avoid shell issues
        temp_file = '/tmp/new_sst_value.bin'
        with open(temp_file, 'wb') as f:
            f.write(new_value_bytes)
        
        # Modify file by reading from temp file
        with open(temp_file, 'rb') as f:
            new_value_str = f.read().decode('latin-1')  # Preserve binary data
        
        result = subprocess.run(
            ['./modify_last_record', filepath, new_value_str],
            capture_output=True
        )
        
        if result.returncode == 0:
            print("\n✓ Success! File modified.")
            print(f"  Backup: {backup_file}")
            return True
        else:
            print("\n✗ Modification failed:")
            print(result.stdout.decode('utf-8', errors='replace'))
            print(result.stderr.decode('utf-8', errors='replace'))
            # Restore backup
            subprocess.run(['cp', backup_file, filepath])
            print("  Original file restored")
            return False
            
    except Exception as e:
        print(f"  Error: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: ./increment_value.py <file_path> [current_hex_value]")
        print()
        print("This script increments the last value in an SST file.")
        print()
        print("Parameters:")
        print("  file_path          : Path to the SST file")
        print("  current_hex_value  : Optional - current hex value (e.g., 0000000400000001)")
        print("                       If not provided, you'll be prompted to enter it")
        print()
        print("Example:")
        print("  ./increment_value.py 448b2cfd-db0d-4e9f-83b8-dc78d69165ef 0000000400000001")
        print()
        print("To find the current value, use sstscan or inspect the file.")
        sys.exit(1)
    
    filepath = sys.argv[1]
    
    print("=" * 50)
    print("SST Value Incrementer")
    print("=" * 50)
    print(f"File: {filepath}\n")
    
    # Get value length
    print("Step 1: Getting value length...")
    value_length = get_value_length(filepath)
    
    if value_length is None:
        print("  Error: Could not determine value length")
        sys.exit(1)
    
    print(f"  Value length: {value_length} bytes\n")
    
    # Get current hex value
    if len(sys.argv) >= 3:
        current_hex = sys.argv[2]
    else:
        print("Step 2: Enter current hex value")
        print("  (Use sstscan to find it, or check last record)")
        current_hex = input("  Current hex value: ").strip()
    
    if not current_hex:
        print("  Error: No value provided")
        sys.exit(1)
    
    print(f"\nStep 3: Incrementing value...")
    new_value_bytes = increment_hex_value(current_hex, value_length)
    
    if new_value_bytes is None:
        print("  Failed to create new value")
        sys.exit(1)
    
    print(f"\nStep 4: Modifying file...")
    success = modify_sst_file(filepath, new_value_bytes)
    
    if not success:
        sys.exit(1)
    
    print("\nDone!")

if __name__ == '__main__':
    main()
