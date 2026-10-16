.PHONY: all build test race bench clean lint run-server run-server-redis run-proxy test-redis

all: test build

build:
	go build -o bin/gameserver ./cmd/gameserver
	go build -o bin/proxy ./cmd/proxy
	go build -o bin/chaos ./cmd/chaos

test:
	go test -v ./...

race:
	go test -v -race ./...

REDIS_TEST_ADDR ?= localhost:6379

test-redis:
	REDIS_TEST_ADDR=$(REDIS_TEST_ADDR) go test -v -count=1 ./pkg/replication/...

bench:
	go test -bench=. -benchmem ./...

clean:
	rm -rf bin/

run-server:
	go run ./cmd/gameserver -role=primary -match=match-demo -seed=123

run-server-redis:
	go run ./cmd/gameserver -role=primary -match=match-demo -seed=123 -redis=localhost:6379

run-proxy:
	go run ./cmd/proxy -port=8080
