#!/bin/bash

# Helper script to modify SST file with automatic padding

if [ $# -lt 2 ]; then
    echo "Usage: $0 <file_path> <new_value> [padding_char]"
    echo ""
    echo "Parameters:"
    echo "  file_path     : Path to the SST file"
    echo "  new_value     : New value (will be padded to match old value length)"
    echo "  padding_char  : Optional padding character (default: null byte \\x00)"
    echo "                  Options: 'null', 'space', or any single character"
    echo ""
    echo "Examples:"
    echo "  $0 myfile 0000000400000002"
    echo "  $0 myfile 0000000400000002 space"
    echo "  $0 myfile 0000000400000002 X"
    exit 1
fi

FILE="$1"
NEW_VALUE="$2"
PADDING_TYPE="${3:-null}"

# Check if file exists
if [ ! -f "$FILE" ]; then
    echo "Error: File not found: $FILE"
    exit 1
fi

# Get the current value length
echo "Inspecting file to determine value length..."
VALUE_LENGTH=$(./modify_last_record "$FILE" --inspect 2>&1 | strings | grep "Value length:" | awk '{print $3}')

if [ -z "$VALUE_LENGTH" ] || ! [[ "$VALUE_LENGTH" =~ ^[0-9]+$ ]]; then
    echo "Error: Could not determine value length from file"
    exit 1
fi

echo "Current value length: $VALUE_LENGTH bytes"
echo "New value length: ${#NEW_VALUE} bytes"

# Check if new value is already the right length
if [ ${#NEW_VALUE} -eq $VALUE_LENGTH ]; then
    echo "Value is already the correct length, no padding needed"
    PADDED_VALUE="$NEW_VALUE"
elif [ ${#NEW_VALUE} -gt $VALUE_LENGTH ]; then
    echo "Error: New value ($NEW_VALUE bytes) is longer than old value ($VALUE_LENGTH bytes)"
    echo "New value will be truncated to fit"
    PADDED_VALUE="${NEW_VALUE:0:$VALUE_LENGTH}"
else
    # Calculate padding needed
    PADDING_NEEDED=$((VALUE_LENGTH - ${#NEW_VALUE}))
    echo "Padding needed: $PADDING_NEEDED bytes"
    
    # Generate padding based on type
    case "$PADDING_TYPE" in
        "null")
            echo "Using null byte padding (\\x00)"
            PADDED_VALUE=$(python3 -c "import sys; sys.stdout.buffer.write(b'$NEW_VALUE' + b'\\x00' * $PADDING_NEEDED)")
            ;;
        "space")
            echo "Using space padding"
            PADDED_VALUE=$(python3 -c "print('$NEW_VALUE' + ' ' * $PADDING_NEEDED, end='')")
            ;;
        *)
            echo "Using custom padding character: '$PADDING_TYPE'"
            PADDED_VALUE=$(python3 -c "print('$NEW_VALUE' + '$PADDING_TYPE' * $PADDING_NEEDED, end='')")
            ;;
    esac
fi

echo "Final padded value length: ${#PADDED_VALUE} bytes"
echo ""

# Create backup
BACKUP_FILE="${FILE}.backup.$(date +%Y%m%d_%H%M%S)"
echo "Creating backup: $BACKUP_FILE"
cp "$FILE" "$BACKUP_FILE"

# Modify the file
echo ""
echo "Modifying file..."
./modify_last_record "$FILE" "$PADDED_VALUE"
RESULT=$?

if [ $RESULT -eq 0 ]; then
    echo ""
    echo "✓ Success! File modified."
    echo "  Backup saved: $BACKUP_FILE"
    echo ""
    echo "To verify the modification:"
    echo "  ./modify_last_record $FILE --inspect | tail -20"
    echo ""
    echo "To restore from backup:"
    echo "  cp $BACKUP_FILE $FILE"
else
    echo ""
    echo "✗ Modification failed. Original file unchanged."
    rm "$BACKUP_FILE"
    exit 1
fi
