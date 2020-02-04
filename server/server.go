package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"

	pb "github.com/ss87021456/gRPC-KVStore/proto"
	"google.golang.org/grpc"
)

const (
	port = ":6000"
)

type server struct {
	inMemoryCache map[string]string
}

func (s *server) GetPrefix(ctx context.Context, getPrefixReq *pb.GetPrefixRequest) (*pb.GetPrefixResponse, error) {
	returnList := []string{}
	for k, v := range s.inMemoryCache {
		fmt.Printf("key[%s] value[%s]\n", k, v)
		if strings.Contains(k, getPrefixReq.GetKey()) {
			returnList = append(returnList, k)
		}
	}
	return &pb.GetPrefixResponse{Values: returnList}, nil
}

func (s *server) Set(ctx context.Context, setReq *pb.SetRequest) (*pb.Empty, error) {
	log.Printf("Set key: %s, value: %s", setReq.GetKey(), setReq.GetValue())
	s.inMemoryCache[setReq.GetKey()] = setReq.GetValue()
	return &pb.Empty{}, nil
}

func (s *server) Get(ctx context.Context, getReq *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("Get key: %s", getReq.GetKey())
	if val, ok := s.inMemoryCache[getReq.GetKey()]; ok {
		return &pb.GetResponse{Value: val}, nil
	}
	return nil, nil // need key not found error
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, &server{inMemoryCache: make(map[string]string)})
	if err = grpcServer.Serve(lis); err != nil {
		fmt.Printf("server has shut down: %v", err)
	}

}
