package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strings"
	"time"
)

func hash(key string) uint32 { // hash key to get map idx
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % uint32(MAPSIZE)
}

func writeAheadLog(mode string, key string, value string) {
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

func getHelper(s *ServerMgr, key string) (string, error) {
	hashcode := hash(key)
	s.inMemoryCache[hashcode].lock.RLock()
	defer s.inMemoryCache[hashcode].lock.RUnlock()
	if val, ok := s.inMemoryCache[hashcode].cache[key]; ok {
		return val, nil
	}
	return "", fmt.Errorf("key: %s not exist", key)
}

func setHelper(s *ServerMgr, key string, value string) {
	hashcode := hash(key)
	s.inMemoryCache[hashcode].lock.Lock()
	s.inMemoryCache[hashcode].cache[key] = value
	s.inMemoryCache[hashcode].lock.Unlock()
}

func prefixHelper(s *ServerMgr, prefix string) []string {
	s.prefixLock.RLock()
	defer s.prefixLock.RUnlock()

	returnList := []string{}
	for idx := 0; idx < MAPSIZE; idx++ {
		for k, v := range s.inMemoryCache[idx].cache {
			// log.Printf("key[%s] value[%s]\n", k, v)
			if strings.Contains(k, prefix) {
				returnList = append(returnList, v)
			}
		}
	}
	return returnList
}

func showCache(s *ServerMgr) {
	fmt.Println("========= showCache ========")
	for i := 0; i < MAPSIZE; i++ {
		fmt.Println("Cache Idx", i)
		for k, v := range s.inMemoryCache[i].cache {
			fmt.Println(k, v)
		}
	}
	fmt.Println("========= showCache ========")
}
