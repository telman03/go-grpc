package main

import (
	"fmt"
	"log"
	"net"
	"time"
	k "github.com/telman03/go-grpc/kafka"
	userpb "github.com/telman03/go-grpc/proto/user"
	"github.com/telman03/go-grpc/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"github.com/google/uuid"
)

const (
	topic = "my-topic"
	numberOfKeys = 20
)

var address = []string{"localhost:9092"}

func main() {
	repo := repository.NewUserRepository()
	serverImpl := newUserServiceServer(repo)

	// Create Kafka producer BEFORE Serve (Serve blocks forever).
	p, err := k.NewProducer(address)
	if err != nil {
		log.Fatalf("failed to create Kafka producer: %v", err)
	}
	defer p.Close()
	keys := genereateUUIDString()
	for i := 0; i < 100; i++ {
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


func genereateUUIDString() [numberOfKeys]string {
	var uuids [numberOfKeys]string
	for i := 0; i < numberOfKeys; i++ {
		uuids[i] = uuid.NewString()
	}
	return uuids
} 