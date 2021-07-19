#!/bin/bash
NUM_WORKERS=4
START=1
# Start locust workers
while [[ $i -le $NUM_WORKERS ]]
do
        echo "Starting worker$number"
        locust -f load.py --worker &
        ((i = i + 1))
done
# Start locust master
locust -f load.py --master
