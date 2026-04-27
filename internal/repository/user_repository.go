package repository

import (
	"fmt"
	"sync"

	userpb "github.com/telman03/go-grpc/proto/user"
)

type UserRepository struct {
	mu     sync.RWMutex
	users  map[string]*userpb.User
	nextID int64
}

func NewUserRepository() *UserRepository {
	return &UserRepository{
		users:  make(map[string]*userpb.User),
		nextID: 1,
	}
}

func (r *UserRepository) Create(user *userpb.User) *userpb.User {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := fmt.Sprintf("%d", r.nextID)
	r.nextID++

	stored := &userpb.User{
		Id:    id,
		Name:  user.GetName(),
		Email: user.GetEmail(),
		Age:   user.GetAge(),
	}
	r.users[id] = stored

	return cloneUser(stored)
}

func (r *UserRepository) GetByID(id string) (*userpb.User, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	u, ok := r.users[id]
	if !ok {
		return nil, false
	}

	return cloneUser(u), true
}

func (r *UserRepository) List() []*userpb.User {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*userpb.User, 0, len(r.users))
	for _, u := range r.users {
		result = append(result, cloneUser(u))
	}

	return result
}

func cloneUser(u *userpb.User) *userpb.User {
	if u == nil {
		return nil
	}

	return &userpb.User{
		Id:    u.GetId(),
		Name:  u.GetName(),
		Email: u.GetEmail(),
		Age:   u.GetAge(),
	}
}
