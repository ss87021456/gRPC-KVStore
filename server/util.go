package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

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
	// clean up the file, but need to use a more efficient way
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
