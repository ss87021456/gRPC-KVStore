package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
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

func main() {
	conn, err := grpc.Dial("localhost:6000", grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp))
	if err != nil {
		fmt.Printf("failed to connect to server: %s", err)
		return
	}
	defer conn.Close()
	client := pb.NewKVStoreClient(conn)
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

func getKey(client pb.KVStoreClient, key string) (string, error) {
	log.Printf("Getting key: %s", key)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return "", fmt.Errorf("failed to get key: %s: %v,", key, err)
	}
	return result.GetValue(), nil
}

func setKey(client pb.KVStoreClient, key string, value string) error {
	log.Printf("Setting key: %s, value: %s", key, value)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := client.Set(ctx, &pb.SetRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("failed to get key: %s: %s, with error: %s", key, value, err)
	}
	return nil
}

func getPrefixKey(client pb.KVStoreClient, key string) ([]string, error) {
	log.Printf("Get Prefix key: %s", key)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := client.GetPrefix(ctx, &pb.GetPrefixRequest{Key: key})
	if err != nil {
		return []string{}, fmt.Errorf("failed to get prefix key: %s: ", key)
	}
	return result.GetValues(), nil
}
