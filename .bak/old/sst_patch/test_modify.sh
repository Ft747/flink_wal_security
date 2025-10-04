#!/bin/bash

# Test script for SST file modification

set -e

FILE="448b2cfd-db0d-4e9f-83b8-dc78d69165ef"

echo "=== SST File Modification Test ==="
echo

# Step 1: Inspect the file
echo "Step 1: Inspecting original file..."
./modify_last_record "$FILE" --inspect | tail -10
echo

# Step 2: Get the value length
VALUE_LENGTH=$(./modify_last_record "$FILE" --inspect 2>&1 | strings | grep "Value length:" | awk '{print $3}')
echo "Last record value length: $VALUE_LENGTH bytes"
echo

# Validate we got a number
if [ -z "$VALUE_LENGTH" ] || ! [[ "$VALUE_LENGTH" =~ ^[0-9]+$ ]]; then
    echo "Error: Could not extract value length"
    exit 1
fi

# Step 3: Create a backup
echo "Step 3: Creating backup..."
cp "$FILE" "$FILE.test_backup"
echo "Backup created: $FILE.test_backup"
echo

# Step 4: Generate a test value of the same length
echo "Step 4: Generating test value ($VALUE_LENGTH bytes)..."
TEST_VALUE=$(python3 -c "print('MODIFIED_VALUE_' * ($VALUE_LENGTH//15) + 'X' * ($VALUE_LENGTH % 15), end='')")
echo "Test value length: ${#TEST_VALUE} bytes"
echo

# Step 5: Modify the file
echo "Step 5: Modifying the file..."
./modify_last_record "$FILE" "$TEST_VALUE"
echo

# Step 6: Verify the modification
echo "Step 6: Verifying modification..."
./modify_last_record "$FILE" --inspect 2>&1 | grep -A5 "Last Record" | head -10
echo

# Step 7: Restore from backup
echo "Step 7: Restoring from backup..."
mv "$FILE.test_backup" "$FILE"
echo "Original file restored"
echo

echo "=== Test Complete ==="
