package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/ss87021456/gRPC-KVStore/proto"
)

var (
	MAPSIZE int = 32
)

type ServerMgr struct {
	inMemoryCache SharedCache
	lastSnapTime  int64
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
	key, value := setReq.GetKey(), setReq.GetValue()
	log.Printf("Set key: %s, value: %s", key, value)
	s.WriteAheadLog("start", key, value)
	s.setHelper(key, value)
	s.WriteAheadLog("done", key, value)

	return &pb.Empty{}, nil
}

func (s *ServerMgr) setHelper(key string, value string) {
	hashcode := s.hash(key)
	s.inMemoryCache[hashcode].lock.Lock()
	s.inMemoryCache[hashcode].cache[key] = value
	s.inMemoryCache[hashcode].lock.Unlock()
}

func (s *ServerMgr) Get(ctx context.Context, getReq *pb.GetRequest) (*pb.GetResponse, error) {
	key := getReq.GetKey()
	log.Printf("Get key: %s", key)
	hashcode := s.hash(key)
	s.inMemoryCache[hashcode].lock.RLock()
	defer s.inMemoryCache[hashcode].lock.RUnlock()

	if val, ok := s.inMemoryCache[hashcode].cache[key]; ok {
		return &pb.GetResponse{Value: val}, nil
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

func (s *ServerMgr) WriteAheadLog(mode string, key string, value string) {
	logFile, err := os.OpenFile("history.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	defer logFile.Close()
	if err != nil {
		log.Println("Failed to create history.log", err)
	}
	outStr := fmt.Sprintf("%v,%s,%s,%s\n", time.Now().Unix(), key, value, mode)
	if _, err := logFile.WriteString(outStr); err != nil {
		log.Println(err)
	}
}

func (s *ServerMgr) SnapShot(filename string) {
	oFile, _ := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	// clean up the file, but need to use a more efficient way
	oFile.Truncate(0)
	oFile.Seek(0, 0)
	defer oFile.Close()
	encoder := json.NewEncoder(oFile)
	encoder.Encode(time.Now().Unix())
	err := encoder.Encode(s.makeData())
	if err != nil {
		log.Printf("fail to write to file %s", err)
	}
}

func (s *ServerMgr) LoadFromSnapshot(filename string) error {
	log.Printf("Initializing cache from file: %s\n", filename)
	iFile, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Println("Need to create new data storage...", err)
		return err
	}
	defer iFile.Close()
	timestampByte := make([]byte, 10) // 10 : unix time length
	iFile.Read(timestampByte)
	timestamp, err := strconv.Atoi(fmt.Sprintf("%s", timestampByte))
	if err != nil {
		log.Printf("Parse timestamp encounter err: %s", err)
	}
	s.lastSnapTime = int64(timestamp)
	iFile.Seek(10, 0)
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

func (s *ServerMgr) LoadFromHistoryLog(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		arr := strings.Split(scanner.Text(), ",")

		if arr[3] == "start" {
			fmt.Println("Recover: ", arr[1], arr[2])
			s.setHelper(arr[1], arr[2]) // [timestamp, key, value, mode]
		}
	}
	// s.showCache()
	return nil
}

func (s *ServerMgr) showCache() {
	fmt.Println("========= showCache ========")
	for i := 0; i < MAPSIZE; i++ {
		fmt.Println("Cache Idx", i)
		for k, v := range s.inMemoryCache[i].cache {
			fmt.Println(k, v)
		}
	}
	fmt.Println("========= showCache ========")
}
