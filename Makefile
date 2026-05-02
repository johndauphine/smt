.PHONY: build clean test test-short test-coverage run install check setup-hooks fmt lint

# Build variables
BINARY_NAME=smt
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-s -w -X smt/internal/version.Version=$(VERSION)"

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod

all: build

build:
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME) ./cmd/smt

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-linux-amd64 ./cmd/smt

build-darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-darwin-amd64 ./cmd/smt
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-darwin-arm64 ./cmd/smt

build-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-windows-amd64.exe ./cmd/smt

build-all: build-linux build-darwin build-windows

clean:
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_NAME)-*

test:
	$(GOTEST) -v ./...

test-short:
	$(GOTEST) ./... -short

test-coverage:
	$(GOTEST) ./... -coverprofile=coverage.out
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

deps:
	$(GOMOD) download
	$(GOMOD) tidy

install: build
	cp $(BINARY_NAME) $(GOPATH)/bin/

run: build
	./$(BINARY_NAME)

fmt:
	$(GOCMD) fmt ./...

lint:
	golangci-lint run

# Docker test databases (mirrors DMT setup)
test-dbs-up:
	docker run -d --name mssql-test \
		--user root \
		-e 'ACCEPT_EULA=Y' \
		-e 'SA_PASSWORD=TestPass2024' \
		-v mssql-test-data:/var/opt/mssql \
		-p 1433:1433 \
		mcr.microsoft.com/mssql/server:2022-latest
	docker run -d --name pg-test \
		-e 'POSTGRES_PASSWORD=TestPass2024' \
		-v pg-test-data:/var/lib/postgresql/data \
		-p 5432:5432 \
		postgres:16-alpine

test-dbs-down:
	docker rm -f mssql-test pg-test 2>/dev/null || true

mysql-test-up:
	docker run -d --name mysql-test \
		-e 'MYSQL_ROOT_PASSWORD=TestPass2024' \
		-v mysql-test-data:/var/lib/mysql \
		-p 3306:3306 \
		mysql:8.0

mysql-test-down:
	docker rm -f mysql-test 2>/dev/null || true

# Pre-commit hooks
setup-hooks:
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit
	@echo "Git hooks configured to use .githooks directory"

check: fmt test
	@echo "All checks passed"
