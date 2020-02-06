package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strings"
	"sync"

	pb "github.com/ss87021456/gRPC-KVStore/proto"
)

var (
	MAPSIZE int = 32
)

type ServerMgr struct {
	inMemoryCache SharedCache
}

type SharedCache []*SingleCache

type SingleCache struct {
	lock  sync.RWMutex
	cache map[string]string
}

type JsonData struct {
	Key, Value string
}

func NewServerMgr() *ServerMgr {
	m := make(SharedCache, MAPSIZE)
	for i := 0; i < MAPSIZE; i++ {
		m[i] = &SingleCache{cache: make(map[string]string)}
	}
	return &ServerMgr{inMemoryCache: m}
}

func (s *ServerMgr) hash(key string) uint32 { // hash key to get map idx
	h := fnv.New32a()
	h.Write([]byte(key))
	log.Printf("string: %s, hash: %d\n", key, h.Sum32()%(uint32(MAPSIZE)))
	return h.Sum32() % uint32(MAPSIZE)
}

func (s *ServerMgr) GetPrefix(ctx context.Context, getPrefixReq *pb.GetPrefixRequest) (*pb.GetPrefixResponse, error) {
	key := getPrefixReq.GetKey()
	hashcode := s.hash(key)
	s.inMemoryCache[hashcode].lock.RLock()
	defer s.inMemoryCache[hashcode].lock.RUnlock()

	returnList := []string{}
	for idx := 0; idx < MAPSIZE; idx++ {
		for k, v := range s.inMemoryCache[idx].cache {
			fmt.Printf("key[%s] value[%s]\n", k, v)
			if strings.Contains(k, getPrefixReq.GetKey()) {
				returnList = append(returnList, v)
			}
		}
	}
	return &pb.GetPrefixResponse{Values: returnList}, nil
}

func (s *ServerMgr) Set(ctx context.Context, setReq *pb.SetRequest) (*pb.Empty, error) {
	key := setReq.GetKey()
	hashcode := s.hash(key)
	log.Printf("Set key: %s, value: %s", key, setReq.GetValue())
	s.inMemoryCache[hashcode].lock.Lock()

	s.inMemoryCache[hashcode].cache[key] = setReq.GetValue()
	s.WriteToFile(FILENAME)
	s.inMemoryCache[hashcode].lock.Unlock()
	return &pb.Empty{}, nil
}

func (s *ServerMgr) Get(ctx context.Context, getReq *pb.GetRequest) (*pb.GetResponse, error) {
	key := getReq.GetKey()
	log.Printf("Get key: %s", key)
	hashcode := s.hash(key)
	s.inMemoryCache[hashcode].lock.RLock()
	defer s.inMemoryCache[hashcode].lock.RUnlock()

	if val, ok := s.inMemoryCache[hashcode].cache[key]; ok {
		return &pb.GetResponse{Value: val}, nil
	} else if fileVal, err := s.SearchFromFile(FILENAME, key); err == nil {
		return &pb.GetResponse{Value: fileVal}, nil
	}
	return &pb.GetResponse{}, fmt.Errorf("cannot get key: %s", key) // need key not found error
}

func (s *ServerMgr) makeData() interface{} {
	datas := []map[string]interface{}{}
	for i := 0; i < MAPSIZE; i++ {
		for k, v := range s.inMemoryCache[i].cache {
			data := map[string]interface{}{
				"Key":   k,
				"Value": v,
			}
			datas = append(datas, data)
		}
	}
	return datas
}

func (s *ServerMgr) WriteToFile(filename string) {
	oFile, _ := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
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
	log.Printf("Get key: %s", key)
	hashcode := s.hash(key)
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
			s.inMemoryCache[hashcode].lock.Lock()
			s.inMemoryCache[hashcode].cache[m.Key] = m.Value
			s.inMemoryCache[hashcode].lock.Unlock()
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
		log.Fatal("Encounter wrong json format data1...", err)
		return err
	}
	// while the array contains values
	for decoder.More() {
		var m JsonData
		err := decoder.Decode(&m)
		if err != nil {
			log.Fatal("Encounter wrong json format data3...", err)
			return err
		}
		hashcode := s.hash(m.Key)
		s.inMemoryCache[hashcode].lock.Lock()
		s.inMemoryCache[hashcode].cache[m.Key] = m.Value
		s.inMemoryCache[hashcode].lock.Unlock()

	}
	// read closing bracket
	if _, err := decoder.Token(); err != nil {
		log.Fatal("Encounter wrong json format data...", err)
		return err
	}
	log.Printf("Finish initializing cache from file: %s\n", filename)
	return nil
}
