APP_NAME           := node-role-controller
APP_VERSION 	   := v0.5.0
YAML_FILES         := $(shell find . -type f \( -iname "*.yml" -o -iname "*.yaml" \))
NODE_IMAGE         ?= kindest/node:v1.33.1

CONFIG_FILE        ?= kind.yaml

# Go 
GO111MODULE     := on
CGO_ENABLED	    := 0

# Environment for Go commands
GO_ENV := \
	GO111MODULE=$(GO111MODULE) \
	CGO_ENABLED=$(CGO_ENABLED)

.PHONY: all build lint clean test help tidy upgrade, tag, pre

all: help

pre: tidy lint test vet ## Run all quality checks

build: ## Build the Go binary locally
	$(GO_ENV) go build -v -o bin/$(APP_NAME) main.go

clean: ## Clean the build artifacts
	$(GO_ENV) go clean -x; \
	rm -f bin/$(APP_NAME)

tidy: ## Run go mod tidy in src
	$(GO_ENV) go fmt ./...; \
	$(GO_ENV) go mod tidy

upgrade: ## Upgrades all dependencies
	$(GO_ENV) go get -u ./...; \
	$(GO_ENV) go mod tidy;

lint: ## Lint the Go code and YAML files
	$(GO_ENV) golangci-lint -c .golangci.yaml run --modules-download-mode=readonly; \
	yamllint -c .yamllint $(YAML_FILES)

test: ## Run Go tests and generate coverage report
	$(GO_ENV) go test -count=1 -covermode=atomic -coverprofile=coverage.out ./... || exit 1; \
	echo "Generating coverage report..."; \
	$(GO_ENV) go tool cover -func=coverage.out

vet: ## Vet the Go code
	$(GO_ENV) go vet ./...

tag: ## Creates a release tag
	git tag -s -m "version bump to $(APP_VERSION)" $(APP_VERSION); \
	git push origin $(APP_VERSION)

up: ## Create a Kubernetes cluster with KinD
	kind create cluster --name $(APP_NAME) --config $(CONFIG_FILE) --wait 5m

down: ## Delete a Kubernetes cluster with KinD
	kind delete cluster --name $(APP_NAME)

integration: ## Run integration tests
	@echo "Running integration tests..."; \
	bash tests/integration 2 || exit 1;

help: ## Displays available commands
	@echo "Available make targets:"; \
	grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk \
		'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

