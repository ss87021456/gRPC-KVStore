package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
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
			setKey(client, n.key, n.value)
		case 2:
			getPrefixKey(client, n.key)
		default:
			log.Fatal("n.action error")
		}
	}
}

var COUNT int = 10000
var serverIp = "localhost"
var port = 6000
var valueSize = 512
var mode = "interative"
var modeRW = "r"

func main() {
	flag.IntVar(&port, "p", port, "the target server's port")
	flag.StringVar(&serverIp, "ip", serverIp, "the target server's ip address")
	flag.IntVar(&valueSize, "size", valueSize, "value size")
	flag.IntVar(&COUNT, "count", COUNT, "ops total count")
	flag.StringVar(&mode, "mode", mode, "the mode of client, interative or benchmark")
	flag.StringVar(&modeRW, "modeRW", modeRW, "the mode of client action, `r` for readonly, `rw` for 50% read 50% write")
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
		start := time.Now()

		/*
			for i := 0; i < COUNT; i++ {
				n := node{key: RandStringBytesMaskImpr(keyLength), value: RandStringBytesMaskImpr(512), action: rand.Intn(2)}
				opsCount[n.action]++
				switch n.action {
				case 0:
					setKey(client, n.key, n.value)
				case 1:
					getKey(client, n.key)
				case 2:
					getPrefixKey(client, n.key)
				default:
					log.Fatal("n.action error")
				}
			}
		*/
		in := make(chan node)
		go func(count int, in chan node) {
			for i := 0; i < count; i++ {
				var n node
				if modeRW == "r" {
					n = node{key: RandStringBytesMaskImpr(keyLength), value: RandStringBytesMaskImpr(valueSize), action: rand.Intn(1)}
				} else if modeRW == "rw" {
					n = node{key: RandStringBytesMaskImpr(keyLength), value: RandStringBytesMaskImpr(valueSize), action: rand.Intn(2)}
				}
				opsCount[n.action]++
				in <- n
			}
			close(in)
		}(COUNT, in)

		var wg sync.WaitGroup
		for i := 0; i < runtime.NumCPU(); i++ {
			wg.Add(1)
			go sendrequest(client, in, &wg)
		}
		wg.Wait()

		// fmt.Println(getPrefixKey(client, "1"))
		info := fmt.Sprintf("elapsed time: %s Total getCount: %d setCount: %d", time.Since(start), opsCount[0], opsCount[1])
		log.Print(info) // benchmark time
	}

	if mode == "interative" {
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
	// log.Printf("Setting key: %s, value: %s", key, value)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := client.Set(ctx, &pb.SetRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("failed to get key: %s: %s, with error: %s", key, value, err)
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
