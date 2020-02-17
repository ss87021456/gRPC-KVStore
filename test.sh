#!/bin/bash

# Description:
# You should also include a simple test script that initializes the server and then starts a few clients (e.g., 10)
# that perform some writes and reads (e.g., 10K). The script then must print the following stats on the console:
# server start time, #total_sets done, #total_gets done, #total_getprefixes done. Finally, the script must stop the server.

make clean
make build

# this script will setup 1 server and 10 client
CLIENT_NUM=10

./server/kvserver -mode test -exp_time 10 -p 8888 &

for i in `eval echo {1..$CLIENT_NUM}`
do
    ./client/kvclient -mode test -exp_time 5 -p 8888 &
done

exit 0