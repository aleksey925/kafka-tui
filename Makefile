VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS = -ldflags "-X main.versionString=$(VERSION) -X main.commit=$(COMMIT)"

BINARY ?= kafka-tui
DIST_DIR = dist
BIN_PATH = $(DIST_DIR)/$(BINARY)
MAIN_PKG = ./cmd/$(BINARY)

.PHONY: deps build snapshot install test race cover lint clean

deps:
	@go mod tidy
	@go mod vendor

build:
	@go build $(LDFLAGS) -o $(BIN_PATH) $(MAIN_PKG)

snapshot:
	@goreleaser release --snapshot --skip=publish --clean

install: build
	@mkdir -p ~/.local/bin
	@rm -f ~/.local/bin/$(BINARY)
	@cp $(BIN_PATH) ~/.local/bin/

test:
	@go test -timeout 3m -v ./...

race:
	@go test -race -timeout 3m ./...

cover:
	go test -race -coverprofile=coverage.out -timeout 3m ./...
	go tool cover -func=coverage.out
	@echo "---"
	@echo "HTML report: go tool cover -html=coverage.out"

lint:
	@prek run --all-files

clean:
	@rm -rf $(DIST_DIR) coverage.out
