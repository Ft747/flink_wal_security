#!/bin/bash
# Wrapper script for modify_last_record tool
# Automatically sets LD_LIBRARY_PATH and provides better error handling

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROCKSDB_LIB="/home/dtome/.nix-profile/lib"
TOOL="${SCRIPT_DIR}/modify_last_record"

# Check if tool exists
if [ ! -f "$TOOL" ]; then
    echo "Error: modify_last_record not found. Run 'make' first." >&2
    exit 1
fi

# Check arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <sst_file_path_without_extension> <new_value_hex>" >&2
    echo "" >&2
    echo "Example: $0 a4208370-e176-40a6-8954-9c353fbb33bd 0000000000000000" >&2
    echo "" >&2
    echo "Parameters:" >&2
    echo "  sst_file_path_without_extension - Path to SST file (without .sst)" >&2
    echo "  new_value_hex                   - New value as hex string (even length)" >&2
    exit 1
fi

SST_FILE="$1"
NEW_VALUE="$2"

# Validate file exists
if [ ! -f "$SST_FILE" ]; then
    echo "Error: File '$SST_FILE' not found." >&2
    exit 1
fi

# Validate hex value
if ! [[ "$NEW_VALUE" =~ ^[0-9A-Fa-f]*$ ]] || [ $((${#NEW_VALUE} % 2)) -ne 0 ]; then
    echo "Error: Invalid hex value '$NEW_VALUE'. Must be even-length hex string." >&2
    exit 1
fi

# Create backup with timestamp
BACKUP_FILE="${SST_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
cp "$SST_FILE" "$BACKUP_FILE"
echo "Created backup: $BACKUP_FILE"

# Run the tool
export LD_LIBRARY_PATH="$ROCKSDB_LIB"
if "$TOOL" "$SST_FILE" "$NEW_VALUE"; then
    echo "✓ Successfully modified $SST_FILE"
    echo "  Backup saved as: $BACKUP_FILE"
    
    # Verify it's still a valid SST file (if renamed with .sst extension)
    if command -v sst_dump &> /dev/null; then
        TEST_FILE="${SST_FILE}.sst"
        if [ -f "$TEST_FILE" ]; then
            echo "  Validating modified SST file..."
            if LD_LIBRARY_PATH="$ROCKSDB_LIB" sst_dump --file="$TEST_FILE" --command=check 2>&1 | grep -q "valid"; then
                echo "  ✓ File is a valid SST file"
            fi
        fi
    fi
else
    echo "✗ Modification failed. Restoring from backup..." >&2
    cp "$BACKUP_FILE" "$SST_FILE"
    exit 1
fi
