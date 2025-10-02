#!/bin/sh

# Get current timestamp down to minutes
now=$(date +"%Y-%m-%d %H:%M")

# Commit with timestamp as message
jj commit -m "$now"

# (Optional) Update description too
jj describe -m "$now"

# Ensure master bookmark points to the new commit
jj bookmark set master

# Push to Git
jj git push
