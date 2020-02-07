package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	pb "github.com/ss87021456/gRPC-KVStore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	panichandler "github.com/kazegusuri/grpc-panic-handler"
)

const (
	port     string = ":6000"
	FILENAME string = "data.json"
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
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return
	}

	// recovery from grpc panic
	panichandler.InstallPanicHandler(func(r interface{}) {
		log.Printf("panic happened: %v", r)
		// handle panic, seems never happen; thus no idea now
	})

	s := NewServerMgr()
	if _, err := os.Stat("history.log"); err == nil {
		s.LoadFromHistoryLog("history.log")
	}

	uIntOpt := grpc.UnaryInterceptor(panichandler.UnaryPanicHandler)
	grpcServer := grpc.NewServer(uIntOpt, grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	pb.RegisterKVStoreServer(grpcServer, s)
	log.Printf("grpc server live successfully!\n")

	// save to disk every 2 minutes
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

	if err = grpcServer.Serve(lis); err != nil {
		log.Printf("server has shut down: %v", err)
	}
}
