package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	pb "github.com/ss87021456/gRPC-KVStore/proto"
)

type JsonData struct {
	Key, Value string
}

func getKey(client pb.KVStoreClient, key string) (string, error) {
	// log.Printf("Getting key: %s", key)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return "", fmt.Errorf("failed to get key: %s: %v,", key, err)
	}
	return result.GetValue(), nil
}

func setKey(client pb.KVStoreClient, key string, value string) error {
	// log.Printf("Setting key: %s, value: %d", key, len(value))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.Set(ctx, &pb.SetRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("failed to set key: %s, with error: %s", key, err)
	}
	return nil
}

func getPrefixKey(client pb.KVStoreClient, key string) ([]string, error) {
	// log.Printf("Get Prefix key: %s", key)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := client.GetPrefix(ctx, &pb.GetPrefixRequest{Key: key})
	if err != nil {
		return []string{}, fmt.Errorf("failed to get prefix key: %s: ", key)
	}
	return result.GetValues(), nil
}

func sendrequest(client pb.KVStoreClient, in <-chan node, wg *sync.WaitGroup) {
	defer wg.Done()
	for n := range in {
		switch n.action {
		case 0:
			getKey(client, n.key)
		case 1:
			err := setKey(client, n.key, n.value)
			if err != nil {
				log.Printf("err: %s\n", err)
			}
		case 2:
			getPrefixKey(client, n.key)
		default:
			log.Fatal("n.action error")
		}
	}
}

func LoadFromHistoryLog(filename string) []JsonData {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var dataset []JsonData
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 1024*1024*5)
	for scanner.Scan() {
		arr := strings.Split(scanner.Text(), ",")
		dataset = append(dataset, JsonData{Key: arr[1], Value: arr[2]})
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Printf("done parsing dataset %s with dataset size %d\n", filename, len(dataset))
	return dataset
}

// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
func RandStringBytesMaskImpr(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
