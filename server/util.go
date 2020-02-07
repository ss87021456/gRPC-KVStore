package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

func writeAheadLog(s *ServerMgr, key string, value string) {
	s.logLock.Lock()
	defer s.logLock.Unlock()
	logFile, err := os.OpenFile("history.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	defer logFile.Close()
	if err != nil {
		log.Println("Failed to create history.log", err)
	}
	outStr := fmt.Sprintf("%v,%s,%s\n", time.Now().Unix(), key, value)
	if _, err := logFile.WriteString(outStr); err != nil {
		log.Println(err)
	}
}

func getHelper(s *ServerMgr, key string) (string, error) {
	// Retrieve item from map.
	if tmp, ok := s.inMemoryCache.Get(key); ok {
		log.Printf("key: %s val: %s", key, tmp)
		return tmp.(string), nil
	}
	return "", fmt.Errorf("key: %s not exist", key)
}

func setHelper(s *ServerMgr, key string, value string) {
	s.inMemoryCache.Set(key, value)
}

func prefixHelper(s *ServerMgr, prefix string) []string {
	// TODO here i think we can use concurrently access to speedup
	returnList := []string{}
	for m := range s.inMemoryCache.Iter() {
		if strings.Contains(m.Key, prefix) {
			returnList = append(returnList, string(m.Val.(string)))
		}
	}
	return returnList
}

func showCache(s *ServerMgr) {
	fmt.Println("========= showCache ========")
	for m := range s.inMemoryCache.Iter() {
		log.Printf("key %s value %s", m.Key, m.Val)
	}
	fmt.Println("========= showCache ========")
}
