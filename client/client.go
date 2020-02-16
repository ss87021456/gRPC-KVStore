package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "github.com/ss87021456/gRPC-KVStore/proto"
)

var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
	Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
}

type node struct {
	key    string
	value  string
	action int // 0: set; 1: get; 2:getPrefix
}

const keyLength = 128
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

type JsonData struct {
	Key, Value string
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

var COUNT int = 200
var serverIp = "localhost"
var port = 6000
var valueSize = 4194304
var mode = "interactive"
var modeRW = "r"
var datasetFile = "KV_10k_128B_512B.txt"
var exp_time = 60

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.IntVar(&port, "p", port, "the target server's port")
	flag.StringVar(&serverIp, "ip", serverIp, "the target server's ip address")
	flag.IntVar(&valueSize, "size", valueSize, "value size")
	flag.IntVar(&COUNT, "count", COUNT, "ops total count")
	flag.IntVar(&exp_time, "exp_time", exp_time, "total experiment time")
	flag.StringVar(&mode, "mode", mode, "the mode of client, interative or benchmark")
	flag.StringVar(&modeRW, "modeRW", modeRW, "the mode of client action, `r` for readonly, `rw` for 50% read 50% write")
	flag.StringVar(&datasetFile, "dataset", datasetFile, "dataset for benchmark, e.g. KV_10k_128B_512B.txt")
	flag.Parse()

	log.Printf("After parsing...\n")

	conn, err := grpc.Dial(serverIp+":"+strconv.Itoa(port), grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp))
	if err != nil {
		fmt.Printf("failed to connect to server: %s", err)
		return
	}
	defer conn.Close()
	client := pb.NewKVStoreClient(conn)

	if mode == "benchmark" {
		/*
			First, you will set the value size to 512B, 4KB, 512KB, 1MB, 4MB and measure the end-to-end latency
			for the following two workloads using a single client

			a read-only workload
			a 50% reads and 50% writes workload
			For all workloads, the key distribution must be uniform, i.e., all keys are equally likely to be read and updated.
		*/
		var opsCount = make([]int, 3)
		dataset := LoadFromHistoryLog(datasetFile)
		start := time.Now()

		/* call func */
		timeout := time.After(time.Duration(exp_time) * time.Second)
		/*
			in := make(chan node)
			go func(count int, in chan node) {
				// for i := 0; i < count; i++ {
				// 	if i%10000 == 0 {
				// 		log.Printf("progress: %d / %d\n", i, count)
				// 	}

				for {
					select {
					case <-timeout:
						close(in)
						return
					default:
						var n node
						// log.Printf("len of dataset:%d\n", len(dataset))
						index := rand.Intn(len(dataset))

						if modeRW == "r" {
							n = node{key: dataset[index].Key, action: 0}
							// n = node{key: RandStringBytesMaskImpr(keyLength), value: RandStringBytesMaskImpr(valueSize), action: 0}
						} else if modeRW == "rw" {
							n = node{key: dataset[index].Key, value: dataset[index].Value, action: rand.Intn(2)}
							// n = node{key: RandStringBytesMaskImpr(keyLength), value: RandStringBytesMaskImpr(valueSize), action: 1}
							// log.Printf("key: %v, value len: %d\n", n.key, len(n.value))
							// n = node{key: iData.Key, value: iData.Value, action: rand.Intn(2)}
						}
						opsCount[n.action]++
						in <- n
					}
				}
				// }(in, inputData)
			}(COUNT, in)

			var wg sync.WaitGroup
			for i := 0; i < runtime.NumCPU(); i++ {
				wg.Add(1)
				go sendrequest(client, in, &wg)
			}
			wg.Wait()
			info := fmt.Sprintf("elapsed time: %s Total getCount: %d setCount: %d total: %d", time.Since(start), opsCount[0], opsCount[1], opsCount[0]+opsCount[1])
			log.Print(info) // benchmark time
		*/
		for {
			select {
			case <-timeout:
				info := fmt.Sprintf("elapsed time: %s Total getCount: %d setCount: %d total: %d", time.Since(start), opsCount[0], opsCount[1], opsCount[0]+opsCount[1])
				log.Print(info) // benchmark time
				return
			default:
				var n node
				index := rand.Intn(len(dataset))

				if modeRW == "r" {
					n = node{key: dataset[index].Key, action: 0}
				} else if modeRW == "rw" {
					n = node{key: dataset[index].Key, value: dataset[index].Value, action: rand.Intn(2)}
				}
				opsCount[n.action]++

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

	}

	if mode == "interactive" {
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("> set, get or getprefix (i.e. set key value): ")
			text, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("failed to read from stdin: %s\n", err)
				return
			}

			items := strings.Split(text, " ")
			if len(items) < 2 {
				continue
			}

			for i := range items {
				items[i] = strings.TrimSpace(items[i])
			}

			switch items[0] {
			case "get":
				value, err := getKey(client, items[1])
				if err != nil {
					log.Printf("failed to get from server: %s\n", err)
					continue
				}
				log.Printf("successfully get %s \n", value)

			case "set":
				if len(items) != 3 {
					continue
				}
				if err := setKey(client, items[1], items[2]); err != nil {
					log.Printf("failed to set to server: %s\n", err)
					continue
				}
				log.Println("successfully published")

			case "getprefix":
				values, err := getPrefixKey(client, items[1])

				if err != nil {
					log.Printf("failed to get prefix from server: %s \n", err)
					continue
				}
				log.Printf("successfully get %s \n", values)

			default:
				continue
			}
		}
	}
}

func getKey(client pb.KVStoreClient, key string) (string, error) {
	// log.Printf("Getting key: %s", key)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return "", fmt.Errorf("failed to get key: %s: %v,", key, err)
	}
	return result.GetValue(), nil
}

func setKey(client pb.KVStoreClient, key string, value string) error {
	// log.Printf("Setting key: %s, value: %d", key, len(value))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := client.Set(ctx, &pb.SetRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("failed to set key: %s, with error: %s", key, err)
	}
	return nil
}

func getPrefixKey(client pb.KVStoreClient, key string) ([]string, error) {
	// log.Printf("Get Prefix key: %s", key)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := client.GetPrefix(ctx, &pb.GetPrefixRequest{Key: key})
	if err != nil {
		return []string{}, fmt.Errorf("failed to get prefix key: %s: ", key)
	}
	return result.GetValues(), nil
}

func LoadFromSnapshot(filename string, inputData chan<- JsonData) error {
	log.Printf("Initializing cache from file: %s\n", filename)
	iFile, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Println("Need to create new data storage...", err)
		return err
	}
	defer iFile.Close()

	timestampByte := make([]byte, 10) // 10 : unix time length
	iFile.Read(timestampByte)
	if err != nil {
		log.Printf("Parse timestamp encounter err: %s", err)
	}
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
		inputData <- m
	}
	// read closing bracket
	if _, err := decoder.Token(); err != nil {
		log.Fatal("Encounter wrong json format data...", err)
		return err
	}
	log.Printf("Finish initializing cache from file: %s\n", filename)
	close(inputData)
	return nil
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
