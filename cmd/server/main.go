package main

import (
	"fmt"
	"log"
	"net"
	"time"
	k "github.com/telman03/go-grpc/internal/kafka"
	userpb "github.com/telman03/go-grpc/proto/user"
	"github.com/telman03/go-grpc/internal/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"github.com/google/uuid"
	"github.com/telman03/go-grpc/internal/grpcserver"
)

const (
	topic = "my-topic"
	numberOfKeys = 20
)

var address = []string{"localhost:9092"}

func main() {
	repo := repository.NewUserRepository()
	serverImpl := grpcserver.New(repo)

	// Create Kafka producer BEFORE Serve (Serve blocks forever).
	p, err := k.NewProducer(address)
	if err != nil {
		log.Fatalf("failed to create Kafka producer: %v", err)
	}
	defer p.Close()
	keys := generateUUIDString()
	for i := range 100 {
		msg := fmt.Sprintf("Kafka message: %d", i)
		key := keys[i%numberOfKeys] // Cycle through the generated UUID keys
		if err := p.Produce(msg, topic , key, time.Now()); err != nil {
			log.Printf("failed to produce message: %v", err)
		}
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	userpb.RegisterUserServiceServer(grpcServer, serverImpl)
	reflection.Register(grpcServer)

	log.Println("gRPC server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}


func generateUUIDString() [numberOfKeys]string {
	var uuids [numberOfKeys]string
	for i := range numberOfKeys {
		uuids[i] = uuid.NewString()
	}
	return uuids
} 