package main

import (
	"flag"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	pb "github.com/ss87021456/gRPC-KVStore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	port        int    = 6000
	serverIp    string = "localhost"
	FILENAME    string = "data.json"
	datasetFile string = "history.log"
)

var (
	customFunc grpc_recovery.RecoveryHandlerFunc
	kaep       = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}
	kasp = keepalive.ServerParameters{
		MaxConnectionIdle:     15 * time.Second, // If a client is idle for 15 seconds, send a GOAWAY
		MaxConnectionAge:      30 * time.Second, // If any connection is alive for more than 30 seconds, send a GOAWAY
		MaxConnectionAgeGrace: 5 * time.Second,  // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
		Time:                  5 * time.Second,  // Ping the client if it is idle for 5 seconds to ensure the connection is still active
		Timeout:               1 * time.Second,  // Wait 1 second for the ping ack before assuming the connection is dead
	}
)

func main() {
	flag.IntVar(&port, "p", port, "the target server's port")
	flag.StringVar(&serverIp, "ip", serverIp, "the target server's ip address")
	flag.StringVar(&datasetFile, "dataset", datasetFile, "dataset for benchmark, e.g. KV_10k_128B_512B.txt")
	flag.Parse()

	lis, err := net.Listen("tcp", serverIp+":"+strconv.Itoa(port))
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return
	}

	s := NewServerMgr()
	if _, err := os.Stat(datasetFile); err == nil {
		s.LoadFromHistoryLog(datasetFile)
	}

	logFile, err := os.OpenFile("history.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	defer logFile.Close()
	s.logFile = logFile

	maxMsgSize := 1024 * 1024 * 16
	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)

	pb.RegisterKVStoreServer(grpcServer, s)
	log.Printf("grpc server live successfully!\n")

	// save to disk every 2 minutes
	/*
		ticker := time.NewTicker(2 * time.Minute)
		quit := make(chan struct{})
		go func(s *ServerMgr) {
			for {
				select {
				case <-ticker.C:
					s.SnapShot(FILENAME)
				case <-quit:
					ticker.Stop()
				}
			}
		}(s)

		go func(q chan struct{}, s *ServerMgr) {
			// graceful shutdown
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt, syscall.SIGTERM)
			<-c
			close(quit)
			s.SnapShot(FILENAME)
			log.Printf("graceful shutdown, finished snapshot..")
			os.Exit(1)
		}(quit, s)
	*/

	if err = grpcServer.Serve(lis); err != nil {
		log.Printf("server has shut down: %v", err)
	}
}
