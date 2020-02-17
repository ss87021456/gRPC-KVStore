# gRPC-KVStore
This is the project 1 for UW-Madison 2020 Spring CS739 Distributed System

## Author
Chia-Wei Chen cchen562@wisc.edu <br>
Pei-Hsuan Wu  pwu59@wisc.edu

## Project descrption
The goal of this project to get some experience building a simple distributed system with a server and a few clients. Specifically, you will build a non-replicated key-value service and provide a set of simple operations to clients. You will learn to use an RPC library, think about how to design a server that can recover correctly and produce correct answers, and perform measurements using simple benchmarks.

## Goal
In addition to performance, you must also validate correctness, particularly, that your store provides strong durability in the presence of failures. E.g. randomly crash the server when a workload is running and checking if the server can recover all written data. 

## Quick start
```
# this will setup 1 server with 10 client, and up for 30 sec.
./test.sh
```

## General Usage for OSX, Linux
```
make build
```
Server
```
./server/kvserver
```
Client
```
./client/kvclient
```

## Use Docker to build environment
```
docker build -t [tag-name-for-image] -f Dockerfile . <br>
docker run -it -p 6000:6000 -v `pwd`:/app --name [name-of-container] [tag-name-for-image] <br>
docker exec -it [name-of-container] bash <br>

[Windows run container]
docker run -it -p 6000:6000 -v ${PWD}:/app --name grpc_container grpc_image <br>
```

## Milestone
- [x] Start with a simple GRPC server and client, make sure communication between each other.
- [x] Adding the kvstore methods, but keep the kvstore non-persistent (just maintain an in-memory hash table of the kv pairs). Get the basic API working for one client first and then test multiple clients.
- [x] Add persistence, i.e., start writing data to disk upon sets. Also, gets may go to disk. Make sure everything still works as expected.
- [x] Crash-recovery, i.e., ensure that the server can correctly recover data from disk when it restarts.
- [x] Implement the stat method, and take care of other things mentioned on the project page.

## Experiment
Exp1. Vary the value size from 512B, 4KB, 512KB, 1MB, 4MB, and measure end-to-end latency of two workloads: read-only, 50% reads+50% writes.
Exp2. Take the server down and restart it. Measure the time it takes for the server to restart, load 4GB data into RAM, and start serving read requests.
Exp3. Vary the number of clients from 1, 2, 4, 8, 16, 32..., and measure the latency and throughput(op/sec) of two workloads: read-only, 50% reads+50% writes. Stop adding #clients if throughput does not increase any further. 


## Reference
[UW-Madison 2020 Spring CS739](http://pages.cs.wisc.edu/~ra/Classes/739-sp20/index.html) <br>
[gRPC - Go](https://grpc.io/docs/quickstart/go/)
