#!/bin/bash

# Compile the program
make

# Check if SST file exists
if [ ! -f "000015.sst" ]; then
    echo "SST file 000015.sst not found"
    exit 1
fi

# Run the modification
./modify_last_record 000015.sst 000015_modified.sst

# Verify with sst_dump
echo "Original file scan:"
sst_dump --file=000015.sst --command=scan | tail -n 5

echo "Modified file scan:"
sst_dump --file=000015_modified.sst --command=scan | tail -n 5
