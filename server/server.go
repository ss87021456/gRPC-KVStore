package main

import (
	"context"
	"encoding/json"
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

type serverMgr struct {
	mux           sync.Mutex
	inMemoryCache map[string]string
}

type JsonData struct {
	Key, Value string
}

func (s *serverMgr) makeData() interface{} {
	datas := []map[string]interface{}{}
	for k, v := range s.inMemoryCache {
		data := map[string]interface{}{
			"Key":   k,
			"Value": v,
		}
		datas = append(datas, data)
	}
	return datas
}

func (s *serverMgr) writeToFile(filename string) {
	oFile, _ := os.OpenFile(filename, os.O_CREATE, os.ModePerm)
	oFile.Truncate(0)
	oFile.Seek(0, 0)
	defer oFile.Close()
	encoder := json.NewEncoder(oFile)
	encoder.Encode(s.makeData())
}

func (s *serverMgr) searchFromFile(filename string, key string) (string, error) {
	log.Printf("search from file and search key: %s", key)
	iFile, _ := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	defer iFile.Close()
	decoder := json.NewDecoder(iFile)
	// Read the array open bracket
	if _, err := decoder.Token(); err != nil {
		log.Fatal("Encounter wrong json format data - 1...", err)
	}
	// while the array contains values
	for decoder.More() {
		var m JsonData
		err := decoder.Decode(&m)
		if err != nil {
			log.Fatal("Encounter wrong json format data - 2...", err)
		}
		// fmt.Printf("data: %v; value: %v \n", m.Key, m.Value)
		if m.Key == key {
			s.mux.Lock()
			s.inMemoryCache[m.Key] = m.Value
			s.mux.Unlock()
			return m.Value, nil
		}
	}
	// read closing bracket
	if _, err := decoder.Token(); err != nil {
		log.Fatal("Encounter wrong json format data - 3...", err)
	}
	return "", fmt.Errorf("No matched key in stored json file\n")
}

func (s *serverMgr) loadFromFile(filename string) error {
	log.Printf("Initializing cache from file: %s\n", filename)
	iFile, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Println("Need to create new data storage...", err)
		return err
	}
	defer iFile.Close()
	decoder := json.NewDecoder(iFile)
	// Read the array open bracket
	if _, err := decoder.Token(); err != nil {
		log.Fatal("Encounter wrong json format data...", err)
		return err
	}
	// while the array contains values
	for decoder.More() {
		var m JsonData
		err := decoder.Decode(&m)
		if err != nil {
			log.Fatal("Encounter wrong json format data...", err)
			return err
		}
		s.mux.Lock()
		s.inMemoryCache[m.Key] = m.Value
		s.mux.Unlock()
	}
	// read closing bracket
	if _, err := decoder.Token(); err != nil {
		log.Fatal("Encounter wrong json format data...", err)
		return err
	}
	fmt.Println(s.inMemoryCache)
	return nil
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
