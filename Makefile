.PHONY: build clean test test-short test-coverage test-so2010 test-crm-acceptance test-crm-fixtures-load test-crm-ci test-dbs-wait test-live-ai release-artifacts release-checksums run install check setup-hooks fmt lint

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
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-darwin-arm64 ./cmd/smt

build-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-windows-amd64.exe ./cmd/smt

build-all: build-linux build-darwin build-windows

release-artifacts:
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o dist/$(BINARY_NAME)-linux-amd64 ./cmd/smt
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o dist/$(BINARY_NAME)-darwin-arm64 ./cmd/smt
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o dist/$(BINARY_NAME)-windows-amd64.exe ./cmd/smt

release-checksums: release-artifacts
	cd dist && shasum -a 256 $(BINARY_NAME)-* > checksums.txt

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

# No-AI end-to-end acceptance: migrate StackOverflow2010 MSSQL -> PostgreSQL
# deterministically and verify the result (#65). Needs live mssql + postgres
# (load StackOverflow2010 into the mssql container first). Override
# SO2010_* env vars for non-default connection params.
test-so2010:
	mkdir -p .acceptance-artifacts/so2010
	SMT_E2E_SO2010=1 SO2010_REPORT_DIR=$(CURDIR)/.acceptance-artifacts/so2010 $(GOTEST) -count=1 -v -run TestSO2010 ./internal/orchestrator/
	@echo "Verification report: $(CURDIR)/.acceptance-artifacts/so2010/so2010_verification.json"

# No-AI CRM live acceptance matrix. Covers every supported source -> target
# permutation across SQL Server, PostgreSQL, and MySQL.
# Requires the CRM fixture databases from testdata/crm to be loaded first.
test-crm-acceptance:
	mkdir -p .acceptance-artifacts/crm
	SMT_E2E_CRM=1 CRM_REPORT_DIR=$(CURDIR)/.acceptance-artifacts/crm $(GOTEST) -count=1 -v -run TestCRM_DeterministicAcceptanceMatrix ./internal/orchestrator/
	@echo "CRM report: $(CURDIR)/.acceptance-artifacts/crm/crm_acceptance_matrix.json"

test-crm-fixtures-load:
	docker cp testdata/crm/crm_mssql.sql mssql-test:/tmp/
	docker exec mssql-test /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'TestPass2024' -C -Q "IF DB_ID(N'CRM_MSSQL') IS NOT NULL BEGIN ALTER DATABASE CRM_MSSQL SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE CRM_MSSQL; END; CREATE DATABASE CRM_MSSQL;"
	docker exec mssql-test /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'TestPass2024' -C -d CRM_MSSQL -i /tmp/crm_mssql.sql
	docker cp testdata/crm/crm_postgres.sql pg-test:/tmp/
	docker exec pg-test sh -c "PGPASSWORD=TestPass2024 dropdb --if-exists -U postgres crm_pg && PGPASSWORD=TestPass2024 createdb -U postgres crm_pg && PGPASSWORD=TestPass2024 psql -U postgres -d crm_pg -v ON_ERROR_STOP=1 -f /tmp/crm_postgres.sql"
	docker cp testdata/crm/crm_mysql.sql mysql-test:/tmp/
	docker exec mysql-test sh -c "mysql -uroot -pTestPass2024 -e 'DROP DATABASE IF EXISTS crm_mysql; CREATE DATABASE crm_mysql;' && mysql -uroot -pTestPass2024 crm_mysql < /tmp/crm_mysql.sql"

test-crm-ci: test-dbs-up mysql-test-up test-dbs-wait test-crm-fixtures-load test-crm-acceptance

# Optional live AI smoke. Explicitly opt in so default unit tests remain
# hermetic and never load host AI secrets.
test-live-ai:
	mkdir -p .acceptance-artifacts/ai
	SMT_LIVE_AI=1 SMT_LIVE_AI_REPORT_DIR=$(CURDIR)/.acceptance-artifacts/ai $(GOTEST) -count=1 -v -run TestLiveAIReviewAndAdvisorySmoke ./internal/driver/
	@echo "Live AI report: $(CURDIR)/.acceptance-artifacts/ai/live_ai_smoke.json"

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
		-e 'MSSQL_SA_PASSWORD=TestPass2024' \
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

test-dbs-wait:
	@echo "Waiting for PostgreSQL..."
	@i=0; while [ $$i -lt 90 ]; do docker exec pg-test pg_isready -U postgres >/dev/null 2>&1 && exit 0; i=$$((i + 1)); sleep 2; done; docker logs pg-test; exit 1
	@echo "Waiting for SQL Server..."
	@i=0; while [ $$i -lt 90 ]; do docker exec mssql-test /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'TestPass2024' -C -Q "SELECT 1" >/dev/null 2>&1 && exit 0; i=$$((i + 1)); sleep 2; done; docker logs mssql-test; exit 1
	@echo "Waiting for MySQL..."
	@i=0; while [ $$i -lt 90 ]; do docker exec mysql-test mysqladmin ping -uroot -pTestPass2024 --silent >/dev/null 2>&1 && exit 0; i=$$((i + 1)); sleep 2; done; docker logs mysql-test; exit 1

mysql-test-down:
	docker rm -f mysql-test 2>/dev/null || true

# Pre-commit hooks
setup-hooks:
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit
	@echo "Git hooks configured to use .githooks directory"

check: fmt test
	@echo "All checks passed"
