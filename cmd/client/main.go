package main

import (
	"context"
	"fmt"
	"log"
	"time"

	userpb "github.com/telman03/go-grpc/proto/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	// Local dev: insecure transport to localhost.
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	client := userpb.NewUserServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	created, err := createUser(ctx, client, "Alice", "alice@test.com", 30)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := getUser(ctx, client, created.GetId()); err != nil {
		log.Fatal(err)
	}

	if err := listUsers(ctx, client); err != nil {
		log.Fatal(err)
	}
}

func createUser(ctx context.Context, c userpb.UserServiceClient, name, email string, age int32) (*userpb.User, error) {
	resp, err := c.CreateUser(ctx, &userpb.CreateUserRequest{
		Name:  name,
		Email: email,
		Age:   age,
	})
	if err != nil {
		return nil, rpcErr("CreateUser", err)
	}

	u := resp.GetUser()
	fmt.Printf("CreateUser OK: id=%s name=%s email=%s age=%d\n", u.GetId(), u.GetName(), u.GetEmail(), u.GetAge())
	return u, nil
}

func getUser(ctx context.Context, c userpb.UserServiceClient, id string) (*userpb.User, error) {
	resp, err := c.GetUser(ctx, &userpb.GetUserRequest{Id: id})
	if err != nil {
		return nil, rpcErr("GetUser", err)
	}

	u := resp.GetUser()
	fmt.Printf("GetUser OK:    id=%s name=%s email=%s age=%d\n", u.GetId(), u.GetName(), u.GetEmail(), u.GetAge())
	return u, nil
}

func listUsers(ctx context.Context, c userpb.UserServiceClient) error {
	resp, err := c.ListUsers(ctx, &userpb.ListUsersRequest{})
	if err != nil {
		return rpcErr("ListUsers", err)
	}

	users := resp.GetUsers()
	fmt.Printf("ListUsers OK:  total=%d\n", len(users))
	for i, u := range users {
		fmt.Printf("  [%d] id=%s name=%s email=%s age=%d\n", i+1, u.GetId(), u.GetName(), u.GetEmail(), u.GetAge())
	}
	return nil
}

func rpcErr(method string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("%s failed: %w", method, err)
	}
	return fmt.Errorf("%s failed: code=%s message=%s", method, st.Code(), st.Message())
}
