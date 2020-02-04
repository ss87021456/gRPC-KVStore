package main

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "github.com/ss87021456/gRPC-KVStore/proto"
	"google.golang.org/grpc"
)

const (
	port = ":6000"
)

type server struct {
}

func (s *server) GetPrefix(ctx context.Context, p *pb.GetPrefixRequest) (*pb.GetPrefixResponse, error) {
	return &pb.GetPrefixResponse{}, nil
}

func (s *server) Set(ctx context.Context, setReq *pb.SetRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (s *server) Get(ctx context.Context, getReq *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("Get key: %s", getReq.GetKey())
	return &pb.GetResponse{Value: getReq.GetKey()}, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, &server{})
	if err = grpcServer.Serve(lis); err != nil {
		fmt.Printf("server has shut down: %v", err)
	}

}
