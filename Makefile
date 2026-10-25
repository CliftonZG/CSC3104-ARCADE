.PHONY: all build test race bench clean lint run-server run-server-redis run-proxy test-redis \
	docker-build k8s-validate k8s-up k8s-demo k8s-down k8s-status kind-up load-test

all: test build

build:
	go build -o bin/gameserver ./cmd/gameserver
	go build -o bin/proxy ./cmd/proxy
	go build -o bin/chaos ./cmd/chaos
	go build -o bin/loadprobe ./cmd/loadprobe

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

# ------------------------------------------------------------------------------
# Containers & Kubernetes (Tan Yan Ting). Windows without make: see k8s/README.md.
# ------------------------------------------------------------------------------
IMAGE_TAG ?= dev
KUBECTL ?= kubectl
PAIRS ?= 10 50 100

docker-build:
	DOCKER_BUILDKIT=1 docker build -f deployments/Dockerfile.gameserver -t arcade/gameserver:$(IMAGE_TAG) .
	DOCKER_BUILDKIT=1 docker build -f deployments/Dockerfile.proxy -t arcade/proxy:$(IMAGE_TAG) .
	DOCKER_BUILDKIT=1 docker build -f deployments/Dockerfile.loadprobe -t arcade/loadprobe:$(IMAGE_TAG) .
# Render every kustomization (needs kubectl >= 1.27; kubeconform optional).
k8s-validate:
	for d in k8s/base k8s/overlays/demo k8s/overlays/load; do $(KUBECTL) kustomize $$d > /dev/null || exit 1; done
	@command -v kubeconform >/dev/null && for d in k8s/base k8s/overlays/demo k8s/overlays/load; do \
		$(KUBECTL) kustomize $$d | kubeconform -strict -summary || exit 1; done || true

k8s-up:
	$(KUBECTL) apply -k k8s/base

k8s-demo:
	$(KUBECTL) apply -k k8s/overlays/demo

k8s-status:
	$(KUBECTL) -n arcade get pods -o wide -L arcade.io/role,arcade.io/match

k8s-down:
	$(KUBECTL) delete namespace arcade --ignore-not-found

# Multi-node cluster for the 50/100-pair load tests (needs kind).
kind-up:
	kind create cluster --config k8s/kind-cluster.yaml
	kind load docker-image arcade/gameserver:$(IMAGE_TAG) arcade/proxy:$(IMAGE_TAG) arcade/loadprobe:$(IMAGE_TAG) --name arcade

load-test:
	test/load/run-load.sh -p "$(PAIRS)" -k "$(KUBECTL)"
