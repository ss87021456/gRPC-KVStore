package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	cmap "github.com/orcaman/concurrent-map"
	pb "github.com/ss87021456/gRPC-KVStore/proto"
)

type ServerMgr struct {
	inMemoryCache cmap.ConcurrentMap
	lastSnapTime  int64
	logLock       sync.Mutex
	logFile       *os.File
}

type SharedCache []*SingleCache

type SingleCache struct {
	lock  sync.RWMutex
	cache map[string]string
}

type JsonData struct {
	Key, Value string
}

func NewServerMgr(f *os.File) *ServerMgr {
	return &ServerMgr{inMemoryCache: cmap.New(), logFile: f}
}

func (s *ServerMgr) Get(ctx context.Context, getReq *pb.GetRequest) (*pb.GetResponse, error) {
	key := getReq.GetKey()
	// log.Printf("Get key: %s", key)
	val, err := getHelper(s, key)
	return &pb.GetResponse{Value: val}, err

}

func (s *ServerMgr) Set(ctx context.Context, setReq *pb.SetRequest) (*pb.Empty, error) {
	key, value := setReq.GetKey(), setReq.GetValue()
	// log.Printf("Set key: %s, value: %s", key, value)
	err := writeAheadLog(s, key, value)
	if err != nil {
		return &pb.Empty{}, nil
	}
	setHelper(s, key, value)
	return &pb.Empty{}, nil
}

func (s *ServerMgr) GetPrefix(ctx context.Context, getPrefixReq *pb.GetPrefixRequest) (*pb.GetPrefixResponse, error) {
	res := prefixHelper(s, getPrefixReq.GetKey())
	// log.Printf("Get prefix: %s", getPrefixReq.GetKey())
	if len(res) > 0 {
		return &pb.GetPrefixResponse{Values: res}, nil
	}
	return &pb.GetPrefixResponse{}, fmt.Errorf("No specific prefix %s found", getPrefixReq.GetKey())
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

func (s *ServerMgr) makeData() interface{} {
	datas := []map[string]interface{}{}
	for m := range s.inMemoryCache.Iter() {
		data := map[string]interface{}{
			"Key":   m.Key,
			"Value": m.Val,
		}
		datas = append(datas, data)
	}
	return datas
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
		setHelper(s, m.Key, m.Value)
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
	s.logLock.Lock()
	defer s.logLock.Unlock()

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		arr := strings.Split(scanner.Text(), ",")
		log.Printf("Recover: %s %s\n", arr[1], arr[2])
		setHelper(s, arr[1], arr[2]) // arr layout -> [timestamp, key, value]
	}

	// perform log filtering for next time
	newFile, err := os.OpenFile("new.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	newFile.Truncate(0)
	newFile.Seek(0, 0)
	defer newFile.Close()
	if err != nil {
		log.Println("Failed to create ntemporary file: new.log", err)
	}
	for m := range s.inMemoryCache.Iter() {
		outStr := fmt.Sprintf("%v,%s,%s\n", time.Now().Unix(), m.Key, m.Val)
		if _, err := newFile.WriteString(outStr); err != nil {
			log.Println(err)
		}
	}
	c := exec.Command("mv", "new.log", "history.log")
	if err := c.Run(); err != nil {
		fmt.Println("Exec mv new.log failed: ", err)
	}

	return nil
}
