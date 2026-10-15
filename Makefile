.PHONY: all build test race bench clean lint run-server run-proxy

all: test build

build:
	go build -o bin/gameserver ./cmd/gameserver
	go build -o bin/proxy ./cmd/proxy
	go build -o bin/chaos ./cmd/chaos

test:
	go test -v ./...

race:
	go test -v -race ./...

bench:
	go test -bench=. -benchmem ./...

clean:
	rm -rf bin/

run-server:
	go run ./cmd/gameserver -role=primary -match=match-demo -seed=123

run-proxy:
	go run ./cmd/proxy -port=8080
