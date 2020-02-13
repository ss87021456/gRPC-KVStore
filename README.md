# gRPC-KVStore
This is the project 1 for UW-Madison 2020 Spring CS739 Distributed System

## Author
Chia-Wei Chen cchen562@wisc.edu <br>
Pei-Hsuan Wu  pwu59@wisc.edu

## Project descrption
The goal of this project to get some experience building a simple distributed system with a server and a few clients. Specifically, you will build a non-replicated key-value service and provide a set of simple operations to clients. You will learn to use an RPC library, think about how to design a server that can recover correctly and produce correct answers, and perform measurements using simple benchmarks.

## Usage
Server
```
cd server/ && go build
cd ../ && ./server/server
```
Client
```
go run client/client.go
```

## Use Docker to build environment
`docker build -t [tag-name-for-image] -f .\Dockerfile .`
`docker run -it -p 6000:6000 -v `pwd`:/app --name [name-of-container] [tag-name-for-image]`
[windows]`docker run -it -p 6000:6000 -v C:\Users\EricaWu\_DDisk\2020_Spring\Distributed_System\Project1\gRPC-KVStore:/app --name grpc_container grpc_image `
`docker exec -it [name-of-container] bash`

## Milestone
- [x] Start with a simple GRPC server and client, make sure communication between each other.
- [x] Adding the kvstore methods, but keep the kvstore non-persistent (just maintain an in-memory hash table of the kv pairs). Get the basic API working for one client first and then test multiple clients.
- [x] Add persistence, i.e., start writing data to disk upon sets. Also, gets may go to disk. Make sure everything still works as expected.
- [ ] Handle large file i.e. > RAM data
- [x] Crash-recovery, i.e., ensure that the server can correctly recover data from disk when it restarts.
- [ ] Implement the stat method, and take care of other things mentioned on the project page.
- [ ] Look for and implement optimizations (e.g. Disk Layer optimize)

## Reference
[UW-Madison 2020 Spring CS739](http://pages.cs.wisc.edu/~ra/Classes/739-sp20/index.html) <br>
[gRPC - Go](https://grpc.io/docs/quickstart/go/)
