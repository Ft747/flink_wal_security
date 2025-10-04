#!/bin/bash

# Script to increment the last value in an SST file by one
# Assumes the value starts with a hex-encoded integer

if [ $# -lt 1 ]; then
    echo "Usage: $0 <file_path> [value_format]"
    echo ""
    echo "Parameters:"
    echo "  file_path     : Path to the SST file"
    echo "  value_format  : Optional format of the value (default: hex16)"
    echo "                  Options:"
    echo "                    hex16  - 16-byte hex string like 0000000400000001"
    echo "                    hex8   - 8-byte hex string"
    echo "                    int    - Plain integer"
    echo ""
    echo "Examples:"
    echo "  $0 myfile.sst"
    echo "  $0 myfile.sst hex16"
    exit 1
fi

FILE="$1"
VALUE_FORMAT="${2:-hex16}"

# Check if file exists
if [ ! -f "$FILE" ]; then
    echo "Error: File not found: $FILE"
    exit 1
fi

echo "=== SST Last Value Incrementer ==="
echo "File: $FILE"
echo "Format: $VALUE_FORMAT"
echo ""

# Get the current value length
echo "Step 1: Inspecting file..."
VALUE_LENGTH=$(./modify_last_record "$FILE" --inspect 2>&1 | strings | grep "Value length:" | awk '{print $3}')

if [ -z "$VALUE_LENGTH" ] || ! [[ "$VALUE_LENGTH" =~ ^[0-9]+$ ]]; then
    echo "Error: Could not determine value length from file"
    exit 1
fi

echo "Value length: $VALUE_LENGTH bytes"

# Extract the current value (first few bytes as text)
echo ""
echo "Step 2: Extracting current value..."

# Use python to read the last record value from the SST file
CURRENT_VALUE=$(./modify_last_record "$FILE" --inspect 2>&1 | \
    grep -A1 "Old value:" | tail -1 | head -c 50 | tr -d '\n' | tr -d ' ')

# If we can't extract it cleanly, ask user to provide it
if [ -z "$CURRENT_VALUE" ] || [ ${#CURRENT_VALUE} -lt 8 ]; then
    echo "Could not automatically extract current value."
    echo "Please enter the current value (e.g., 0000000400000001):"
    read -r CURRENT_VALUE
fi

echo "Current value (first chars): $CURRENT_VALUE"

# Parse and increment based on format
case "$VALUE_FORMAT" in
    "hex16")
        # Extract first 16 hex characters
        HEX_VALUE="${CURRENT_VALUE:0:16}"
        echo "Hex value: $HEX_VALUE"
        
        # Convert to decimal, increment, convert back
        DECIMAL_VALUE=$((16#$HEX_VALUE))
        echo "Decimal value: $DECIMAL_VALUE"
        
        NEW_DECIMAL=$((DECIMAL_VALUE + 1))
        echo "New decimal value: $NEW_DECIMAL"
        
        # Convert back to 16-char hex
        NEW_HEX=$(printf "%016x" $NEW_DECIMAL)
        echo "New hex value: $NEW_HEX"
        
        # Pad to full length
        PADDING_NEEDED=$((VALUE_LENGTH - 16))
        NEW_VALUE=$(python3 -c "import sys; sys.stdout.buffer.write(b'$NEW_HEX' + b'\\x00' * $PADDING_NEEDED)")
        ;;
        
    "hex8")
        # Extract first 8 hex characters
        HEX_VALUE="${CURRENT_VALUE:0:8}"
        echo "Hex value: $HEX_VALUE"
        
        DECIMAL_VALUE=$((16#$HEX_VALUE))
        echo "Decimal value: $DECIMAL_VALUE"
        
        NEW_DECIMAL=$((DECIMAL_VALUE + 1))
        echo "New decimal value: $NEW_DECIMAL"
        
        NEW_HEX=$(printf "%08x" $NEW_DECIMAL)
        echo "New hex value: $NEW_HEX"
        
        PADDING_NEEDED=$((VALUE_LENGTH - 8))
        NEW_VALUE=$(python3 -c "import sys; sys.stdout.buffer.write(b'$NEW_HEX' + b'\\x00' * $PADDING_NEEDED)")
        ;;
        
    "int")
        # Try to extract as plain integer
        INT_VALUE=$(echo "$CURRENT_VALUE" | grep -o -E '[0-9]+' | head -1)
        echo "Integer value: $INT_VALUE"
        
        NEW_INT=$((INT_VALUE + 1))
        echo "New integer value: $NEW_INT"
        
        # Pad to full length
        NEW_VALUE_STR="$NEW_INT"
        PADDING_NEEDED=$((VALUE_LENGTH - ${#NEW_VALUE_STR}))
        NEW_VALUE=$(python3 -c "print('$NEW_VALUE_STR' + '\\x00' * $PADDING_NEEDED, end='')")
        ;;
        
    *)
        echo "Error: Unknown format: $VALUE_FORMAT"
        exit 1
        ;;
esac

if [ ${#NEW_VALUE} -ne $VALUE_LENGTH ]; then
    echo "Error: New value length (${#NEW_VALUE}) doesn't match expected length ($VALUE_LENGTH)"
    echo "Falling back to manual padding..."
    
    # Manual approach: just provide the hex string and pad with nulls
    if [ "$VALUE_FORMAT" = "hex16" ]; then
        python3 << EOF > /tmp/new_value.bin
import sys
new_hex = "$NEW_HEX"
padding = b'\\x00' * ($VALUE_LENGTH - len(new_hex))
sys.stdout.buffer.write(new_hex.encode() + padding)
EOF
        NEW_VALUE=$(cat /tmp/new_value.bin)
        rm -f /tmp/new_value.bin
    fi
fi

echo ""
echo "Step 3: Creating backup..."
BACKUP_FILE="${FILE}.backup.$(date +%Y%m%d_%H%M%S)"
cp "$FILE" "$BACKUP_FILE"
echo "Backup: $BACKUP_FILE"

echo ""
echo "Step 4: Writing new value..."
./modify_last_record "$FILE" "$NEW_VALUE"
RESULT=$?

if [ $RESULT -eq 0 ]; then
    echo ""
    echo "✓ Success! Value incremented."
    echo "  Backup: $BACKUP_FILE"
    echo ""
    echo "Verify with:"
    echo "  ./modify_last_record $FILE --inspect | tail -5"
else
    echo ""
    echo "✗ Failed to modify file"
    cp "$BACKUP_FILE" "$FILE"
    echo "Original file restored from backup"
    exit 1
fi
