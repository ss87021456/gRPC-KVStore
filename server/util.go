package main

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	cmap "github.com/orcaman/concurrent-map"
)

func writeAheadLog(s *ServerMgr, key string, value string) error {
	s.logLock.Lock()
	defer s.logLock.Unlock()

	outStr := fmt.Sprintf("%v,%s,%s,done\n", time.Now().Unix(), key, value) // done for the checksum
	var err error
	if _, err = s.logFile.WriteString(outStr); err != nil {
		log.Println(err)
		return err
	}
	return s.logFile.Sync() // ensure write to stable disk
}

func getHelper(s *ServerMgr, key string) (string, error) {
	// Retrieve item from map.
	if tmp, ok := s.inMemoryCache.Get(key); ok {
		return tmp.(string), nil
	}
	return "", fmt.Errorf("key: %s not exist", key)
}

func setHelper(s *ServerMgr, key string, value string) {
	s.inMemoryCache.Set(key, value)
}

func prefixHelper(s *ServerMgr, prefix string) []string {
	returnList := []string{}
	in := s.inMemoryCache.Iter()
	workers := make([]<-chan string, runtime.NumCPU())
	// fan-out, distribute to multiple workers
	for i := 0; i < runtime.NumCPU(); i++ {
		workers[i] = checkTuples(in, prefix)
	}
	// consume the merged output from  multiple workers
	for res := range merge(workers...) {
		returnList = append(returnList, res)
	}
	return returnList
}

func checkTuples(items <-chan cmap.Tuple, prefix string) <-chan string {
	out := make(chan string) // maybe buffer will be helpful
	go func() {
		for item := range items {
			if strings.HasPrefix(item.Key, prefix) {
				out <- item.Val.(string)
			}
		}
		close(out)
	}()
	return out
}

func merge(cs ...<-chan string) <-chan string {
	var wg sync.WaitGroup
	out := make(chan string)

	output := func(c <-chan string) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func showCache(s *ServerMgr) {
	fmt.Println("========= showCache ========")
	for m := range s.inMemoryCache.Iter() {
		log.Printf("key %s value %s", m.Key, m.Val)
	}
	fmt.Println("========= showCache ========")
}
