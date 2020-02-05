package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	pb "github.com/ss87021456/gRPC-KVStore/proto"
)

type ServerMgr struct {
	mux           sync.Mutex
	inMemoryCache map[string]string
}

type JsonData struct {
	Key, Value string
}

func NewServerMgr() *ServerMgr {
	return &ServerMgr{
		inMemoryCache: make(map[string]string),
	}
}

func (s *ServerMgr) GetPrefix(ctx context.Context, getPrefixReq *pb.GetPrefixRequest) (*pb.GetPrefixResponse, error) {
	returnList := []string{}
	for k, v := range s.inMemoryCache {
		fmt.Printf("key[%s] value[%s]\n", k, v)
		if strings.Contains(k, getPrefixReq.GetKey()) {
			returnList = append(returnList, v)
		}
	}
	return &pb.GetPrefixResponse{Values: returnList}, nil
}

func (s *ServerMgr) Set(ctx context.Context, setReq *pb.SetRequest) (*pb.Empty, error) {
	log.Printf("Set key: %s, value: %s", setReq.GetKey(), setReq.GetValue())
	s.mux.Lock()
	s.inMemoryCache[setReq.GetKey()] = setReq.GetValue()
	s.WriteToFile(FILENAME)
	s.mux.Unlock()
	return &pb.Empty{}, nil
}

func (s *ServerMgr) Get(ctx context.Context, getReq *pb.GetRequest) (*pb.GetResponse, error) {
	key := getReq.GetKey()
	log.Printf("Get key: %s", key)
	if val, ok := s.inMemoryCache[key]; ok {
		return &pb.GetResponse{Value: val}, nil
	} else if fileVal, err := s.SearchFromFile(FILENAME, key); err == nil {
		return &pb.GetResponse{Value: fileVal}, nil
	}
	return &pb.GetResponse{}, fmt.Errorf("cannot get key: %s", key) // need key not found error
}

func (s *ServerMgr) makeData() interface{} {
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

func (s *ServerMgr) WriteToFile(filename string) {
	oFile, _ := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	// clean up the file, but need to use a more efficient way
	oFile.Truncate(0)
	oFile.Seek(0, 0)
	defer oFile.Close()
	encoder := json.NewEncoder(oFile)
	err := encoder.Encode(s.makeData())
	if err != nil {
		log.Printf("fail to write to file %s", err)
	}
}

func (s *ServerMgr) SearchFromFile(filename string, key string) (string, error) {
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

func (s *ServerMgr) LoadFromFile(filename string) error {
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
