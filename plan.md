# Week 2 Labs Plan: gRPC User Service

## Goal
Build a complete gRPC UserService in Go, then add interceptors and server-side streaming.
You will write all code yourself and use this file as your implementation checklist.

## Phase 0: Environment Setup

1. Initialize module:
	- `go mod init <your-module-name>`
2. Install gRPC codegen tools:
	- `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`
	- `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`
3. Install grpcurl:
	- `go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest`
4. Confirm binaries are on PATH (`$GOPATH/bin` or `$HOME/go/bin`).
5. Verify:
	- `go version`
	- `protoc --version`

Checkpoint:
- You can generate Go code from a proto file without errors.

---

## Lab 1: Complete UserService (Server + Client)

### Step 1: Define API contract (proto)

1. Create `proto/user.proto`.
2. Define messages:
	- `User` (id, name, email, age)
	- `CreateUserRequest`, `CreateUserResponse`
	- `GetUserRequest`, `GetUserResponse`
	- `ListUsersRequest` and response type (temporary unary form is OK now)
3. Define service:
	- `rpc CreateUser(CreateUserRequest) returns (CreateUserResponse);`
	- `rpc GetUser(GetUserRequest) returns (GetUserResponse);`
	- `rpc ListUsers(ListUsersRequest) returns (...)` (you will convert this in Lab 3)
4. Add `option go_package = "<module-path>/gen/userpb;userpb";`
5. Generate code:
	- `protoc --go_out=. --go-grpc_out=. proto/user.proto`

Checkpoint:
- Generated files compile.
- Package names/imports are clean.

### Step 2: Build in-memory repository

1. Create repository struct with:
	- `map[string]*User`
	- `sync.RWMutex`
2. Add methods:
	- `Create(user)`
	- `GetByID(id)`
	- `List()`
3. Use a simple ID generator (UUID or incrementing counter).

Checkpoint:
- Create/Get/List work via quick test or temporary main function.

### Step 3: Implement gRPC server handlers

1. Create server type implementing generated `UserServiceServer`.
2. `CreateUser`:
	- Validate fields (name/email not empty, age > 0)
	- Save to repository
	- Return created user
3. `GetUser`:
	- Validate request id
	- Return `NotFound` if user missing
4. `ListUsers`:
	- Return all users from repository (unary for now)
5. Return proper status codes:
	- `codes.InvalidArgument`
	- `codes.NotFound`
	- `codes.Internal` when needed
6. Start server on `:50051`.
7. Enable reflection:
	- import `google.golang.org/grpc/reflection`
	- call `reflection.Register(s)`

Checkpoint:
- Server boots and listens successfully.

### Step 4: Implement basic client

1. Dial server (`grpc.Dial` with insecure creds for local dev).
2. Add client commands/functions:
	- CreateUser
	- GetUser
	- ListUsers
3. Print clean output to terminal.

Checkpoint:
- Client can execute unary RPCs end-to-end.

### Step 5: Validate with grpcurl

1. List services:
	- `grpcurl -plaintext localhost:50051 list`
2. Describe service:
	- `grpcurl -plaintext localhost:50051 describe user.UserService`
3. Create user:
	- `grpcurl -plaintext -d '{"name":"Alice","email":"alice@test.com","age":30}' localhost:50051 user.UserService/CreateUser`
4. Get/List users with grpcurl requests.

Checkpoint:
- grpcurl can discover service and call methods successfully.

---

## Lab 2: Add Unary Interceptors

### Step 1: Logging interceptor

Implement unary interceptor that logs for every RPC:
- method name (`info.FullMethod`)
- duration (`time.Since(start)`)
- error (if present)

Flow:
1. record start time
2. call handler
3. log method + duration + error
4. return handler result

### Step 2: Recovery interceptor

Implement unary recovery interceptor:
- use `defer` + `recover()`
- on panic:
  - log panic + method
  - return `status.Errorf(codes.Internal, "internal server error")`

Suggested template:

```go
func recoveryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	 defer func() {
		  if r := recover(); r != nil {
				log.Printf("panic in %s: %v", info.FullMethod, r)
				err = status.Errorf(codes.Internal, "internal server error")
		  }
	 }()
	 return handler(ctx, req)
}
```

### Step 3: Chain interceptors on server

Use `grpc.NewServer(grpc.ChainUnaryInterceptor(...))`.

Recommended chain:
1. recovery
2. logging

Checkpoint:
- Regular calls work.
- Forced panic does not crash process.
- Panic returns gRPC Internal.
- Logs show method and duration for each request.

---

## Lab 3: Server-Side Streaming for ListUsers

### Step 1: Update proto RPC

Change ListUsers to server streaming:
- `rpc ListUsers(ListUsersRequest) returns (stream User);`

Regenerate code.

Checkpoint:
- Generated interface now expects stream handler.

### Step 2: Implement streaming server handler

1. Fetch users from repository.
2. Loop users one by one.
3. `stream.Send(user)` for each item.
4. Add small delay (`100ms` to `300ms`) between sends.
5. Respect cancellation:
	- check `stream.Context().Done()` in loop.

Checkpoint:
- Server sends incrementally, not as a single batch.

### Step 3: Implement streaming client

1. Call `ListUsers` to get stream.
2. Loop `Recv()` until `io.EOF`.
3. Print each user immediately when received.

Checkpoint:
- Output appears over time as messages arrive.

### Step 4: Verify with grpcurl

Run ListUsers via grpcurl and watch for incremental responses.

Checkpoint:
- Response messages appear one at a time while server is still sending.

---

## Suggested Build Order

1. Proto + code generation
2. Repository (in-memory)
3. Unary Create/Get
4. Reflection + grpcurl checks
5. Client for unary calls
6. Logging + recovery interceptors
7. Convert ListUsers to streaming
8. Streaming client/server validation

---

## Definition of Done

1. Server runs on `:50051`.
2. Reflection enabled and discoverable with grpcurl.
3. `CreateUser` works.
4. `GetUser` returns correct values and `NotFound` when needed.
5. Logging interceptor logs method + duration + error.
6. Recovery interceptor catches panic and returns Internal.
7. `ListUsers` uses server-side streaming.
8. Client prints streamed items as they arrive.
9. No data races in repository access.

---

## Common Mistakes to Avoid

1. Forgetting to regenerate code after proto edits.
2. Returning plain Go errors instead of gRPC status errors.
3. Accessing map without mutex protection.
4. Forgetting reflection when using grpcurl service discovery.
5. Writing streaming client that buffers all messages before printing.
6. Ignoring context cancellation in stream loop.

---

## Self-Study Rhythm (Optional)

1. Session A (60 to 90 min): proto + codegen + repository
2. Session B (60 to 90 min): unary server + grpcurl
3. Session C (60 min): client + cleanup
4. Session D (45 to 60 min): interceptors + panic test
5. Session E (60 min): streaming conversion + final verification

If blocked, debug in this order:
1. proto compile/generate
2. server startup and registration
3. grpcurl discovery
4. method input validation
5. streaming Recv/Send loop behavior
