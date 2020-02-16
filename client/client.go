package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "github.com/ss87021456/gRPC-KVStore/proto"
)

// keep alive param
var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
	Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
}

// for random string
const keyLength = 128
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

type node struct {
	key    string
	value  string
	action int // 0: set; 1: get; 2:getPrefix
}

var COUNT int = 1000
var serverIp = "localhost"
var port = 6000
var valueSize = 4194304
var mode = "interactive"
var modeRW = "r"
var datasetFile = "KV_10k_128B_512B.txt"
var exp_time = 60
var maxMsgSize = 1024 * 1024 * 10

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.IntVar(&COUNT, "count", COUNT, "ops total count")
	flag.IntVar(&valueSize, "size", valueSize, "value size")
	flag.IntVar(&port, "p", port, "the target server's port")
	flag.IntVar(&exp_time, "exp_time", exp_time, "total experiment time")
	flag.StringVar(&serverIp, "ip", serverIp, "the target server's ip address")
	flag.StringVar(&mode, "mode", mode, "the mode of client, interative or benchmark")
	flag.StringVar(&datasetFile, "dataset", datasetFile, "dataset for benchmark, e.g. KV_10k_128B_512B.txt")
	flag.StringVar(&modeRW, "modeRW", modeRW, "the mode of client action, `r` for readonly, `rw` for 50% read 50% write")
	flag.Parse()

	conn, err := grpc.Dial(serverIp+":"+strconv.Itoa(port),
		grpc.WithInsecure(),
		grpc.WithKeepaliveParams(kacp),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)))
	if err != nil {
		log.Fatalf("failed to connect to server: %s", err)
	}
	defer conn.Close()
	client := pb.NewKVStoreClient(conn)

	if mode == "benchmark" {
		var opsCount = make([]int, 3)
		dataset := LoadFromHistoryLog(datasetFile)
		start := time.Now()

		timeout := time.After(time.Duration(exp_time) * time.Second)
		for {
			select {
			case <-timeout:
				info := fmt.Sprintf("elapsed time: %s, #total_gets: %d, #total_sets: %d, #total_getprefixes: %d, #total_ops: %d",
					time.Since(start), opsCount[0], opsCount[1], opsCount[2], opsCount[0]+opsCount[1]+opsCount[2])
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
					_, err := getKey(client, n.key)
					if err != nil {
						log.Printf("err: %s\n", err)
					}
				case 1:
					err := setKey(client, n.key, n.value)
					if err != nil {
						log.Printf("err: %s\n", err)
					}
				case 2:
					_, err := getPrefixKey(client, n.key)
					if err != nil {
						log.Printf("err: %s\n", err)
					}
				default:
					log.Fatal("n.action error")
				}

			}
		}
	} else if mode == "interactive" {
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
