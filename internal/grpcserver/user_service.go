package grpcserver

import (
	"context"
	"strings"

	userpb "github.com/telman03/go-grpc/proto/user"
	"github.com/telman03/go-grpc/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UserServiceServer struct {
	userpb.UnimplementedUserServiceServer
	repo *repository.UserRepository
}

func New(repo *repository.UserRepository) *UserServiceServer {
	return &UserServiceServer{repo: repo}
}

func (s *UserServiceServer) CreateUser(ctx context.Context, req *userpb.CreateUserRequest) (*userpb.CreateUserResponse, error) {
	_ = ctx

	name := strings.TrimSpace(req.GetName())
	email := strings.TrimSpace(req.GetEmail())
	age := req.GetAge()

	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if email == "" {
		return nil, status.Error(codes.InvalidArgument, "email is required")
	}
	if age <= 0 {
		return nil, status.Error(codes.InvalidArgument, "age must be greater than 0")
	}

	created := s.repo.Create(&userpb.User{
		Name: name,
		Email: email,
		Age: age,
	})

	return &userpb.CreateUserResponse{User: created}, nil
}

func (s *UserServiceServer) GetUser(ctx context.Context, req *userpb.GetUserRequest) (*userpb.GetUserResponse, error) {
	_ = ctx

	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	user, ok := s.repo.GetByID(id)
	if !ok {
		return nil, status.Error(codes.NotFound, "user not found")
	}

	return &userpb.GetUserResponse{User: user}, nil
}

func (s *UserServiceServer) ListUsers(ctx context.Context, req *userpb.ListUsersRequest) (*userpb.ListUsersResponse, error) {
	_ = ctx 

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if s.repo == nil {
		return nil, status.Error(codes.Internal, "repository is not initialized")
	}

	users := s.repo.List()
	if len(users) == 0 {
		return nil, status.Error(codes.NotFound, "no users found")
	}
	
	return &userpb.ListUsersResponse{Users: users}, nil
}