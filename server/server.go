package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	pb "github.com/ss87021456/gRPC-KVStore/proto"
	"google.golang.org/grpc"
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
	log.Printf("Get key: %s", getReq.GetKey())
	if val, ok := s.inMemoryCache[getReq.GetKey()]; ok {
		return &pb.GetResponse{Value: val}, nil
	} else if fileVal, err := s.searchFromFile(FILENAME, getReq.GetKey()); err == nil {
		return &pb.GetResponse{Value: fileVal}, nil
	}

	return nil, fmt.Errorf("key not found error\n")
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return
	}

	s := &serverMgr{inMemoryCache: make(map[string]string)}
	s.loadFromFile(FILENAME)

	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, s)
	if err = grpcServer.Serve(lis); err != nil {
		fmt.Printf("server has shut down: %v", err)
	}

}
