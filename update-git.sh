#!/bin/sh
now=$(date +"%Y-%m-%d %H:%M")
jj commit -m "$now"
jj bookmark set master -r @-
jj git push
