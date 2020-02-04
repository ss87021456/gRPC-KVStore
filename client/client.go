package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"

	pb "github.com/ss87021456/gRPC-KVStore/proto"
)

func main() {
	conn, err := grpc.Dial("localhost:6000", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("failed to connect to server: %s", err)
		return
	}
	defer conn.Close()
	client := pb.NewKVStoreClient(conn)
	setKey(client, "animals", "dog")
	setKey(client, "apple", "good")
	setKey(client, "fruits", "banana")
	getKey(client, "animals")
	GetPrefixKey(client, "a")
}

func getKey(client pb.KVStoreClient, key string) {
	log.Printf("Getting key: %s", key)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		log.Printf("failed to get key: %s: %v,", key, err)
		return
	}
	log.Printf("Get value: %s", result.GetValue())
}

func setKey(client pb.KVStoreClient, key string, value string) {
	log.Printf("Setting key: %s, value: %s", key, key)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := client.Set(ctx, &pb.SetRequest{Key: key})
	if err != nil {
		log.Printf("failed to get key: %s: %v,", key, err)
		return
	}
	// log.Printf("Set value: %s (checked by get)", result.Set())
}

func GetPrefixKey(client pb.KVStoreClient, key string) {
	log.Printf("Get Prefix key: %s", key)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := client.GetPrefix(ctx, &pb.GetPrefixRequest{Key: key})
	if err != nil {
		log.Printf("failed to get key: %s: %v,", key, err)
		return
	}
	log.Printf("Matched Prefix keys: %s ", result.GetValues())
}
