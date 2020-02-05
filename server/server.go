package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	pb "github.com/ss87021456/gRPC-KVStore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	panichandler "github.com/kazegusuri/grpc-panic-handler"
)

const (
	port     string = ":6000"
	FILENAME string = "data.json"
)

var (
	customFunc grpc_recovery.RecoveryHandlerFunc
	kaep       = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}
	kasp = keepalive.ServerParameters{
		MaxConnectionIdle:     15 * time.Second, // If a client is idle for 15 seconds, send a GOAWAY
		MaxConnectionAge:      30 * time.Second, // If any connection is alive for more than 30 seconds, send a GOAWAY
		MaxConnectionAgeGrace: 5 * time.Second,  // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
		Time:                  5 * time.Second,  // Ping the client if it is idle for 5 seconds to ensure the connection is still active
		Timeout:               1 * time.Second,  // Wait 1 second for the ping ack before assuming the connection is dead
	}
)

type serverMgr struct {
	mux           sync.Mutex
	inMemoryCache map[string]string
}

type JsonData struct {
	Key, Value string
}

func (s *serverMgr) GetPrefix(ctx context.Context, getPrefixReq *pb.GetPrefixRequest) (*pb.GetPrefixResponse, error) {
	returnList := []string{}
	for k, v := range s.inMemoryCache {
		fmt.Printf("key[%s] value[%s]\n", k, v)
		if strings.Contains(k, getPrefixReq.GetKey()) {
			returnList = append(returnList, k)
		}
	}
	return &pb.GetPrefixResponse{Values: returnList}, nil
}

func (s *serverMgr) Set(ctx context.Context, setReq *pb.SetRequest) (*pb.Empty, error) {
	log.Printf("Set key: %s, value: %s", setReq.GetKey(), setReq.GetValue())
	s.mux.Lock()
	s.inMemoryCache[setReq.GetKey()] = setReq.GetValue()
	s.writeToFile(FILENAME)
	s.mux.Unlock()
	return &pb.Empty{}, nil
}

func (s *serverMgr) Get(ctx context.Context, getReq *pb.GetRequest) (*pb.GetResponse, error) {
	key := getReq.GetKey()
	log.Printf("Get key: %s", key)
	if val, ok := s.inMemoryCache[key]; ok {
		return &pb.GetResponse{Value: val}, nil
	} else if fileVal, err := s.searchFromFile(FILENAME, key); err == nil {
		return &pb.GetResponse{Value: fileVal}, nil
	}
	return &pb.GetResponse{}, fmt.Errorf("cannot get key: %s", key) // need key not found error
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return
	}

	s := &serverMgr{inMemoryCache: make(map[string]string)}
	s.loadFromFile(FILENAME)

	// recovery from panic
	panichandler.InstallPanicHandler(func(r interface{}) {
		log.Printf("panic happened: %v", r)
		// TODO: need to do recovery here
		// If there's a unfinish job, need to record it into log
		// Therefore, after next time reboot. We can finish that job
		// and then serve as normal for clients
	})
	uIntOpt := grpc.UnaryInterceptor(panichandler.UnaryPanicHandler)
	grpcServer := grpc.NewServer(uIntOpt, grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	pb.RegisterKVStoreServer(grpcServer, s)
	log.Printf("grpc server live successfully!\n")

	// graceful shutdown
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func(s *serverMgr) {
		<-c
		fmt.Println("test gracefully shutdown")
		s.writeToFile(FILENAME)
		os.Exit(1)
	}(s)

	if err = grpcServer.Serve(lis); err != nil {
		fmt.Printf("server has shut down: %v", err)
	}
}
