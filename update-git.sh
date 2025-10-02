#!/bin/sh
now=$(date +"%Y-%m-%d %H:%M")
jj commit -m "$now"
wait 1 
jj describe -m "$now" 
jj bookmark set master 
jj git push
