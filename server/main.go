package main

import (
	"log"
	"net"

	userpb "github.com/telman03/go-grpc/proto/user"
	"github.com/telman03/go-grpc/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	repo := repository.NewUserRepository()
	serverImpl := newUserServiceServer(repo)

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
