.PHONY: gtron all test lint proto clean fixtures fixtures-list

GOBIN = $(shell pwd)/build/bin
GO ?= go
GOFLAGS =

gtron:
	$(GO) build $(GOFLAGS) -o $(GOBIN)/gtron ./cmd/gtron
	@echo "Done building gtron."
	@echo "Run \"$(GOBIN)/gtron\" to launch."

all: gtron

test:
	$(GO) test ./... -count=1 -timeout 300s

lint:
	golangci-lint run ./...

proto:
	@echo "Generating protobuf Go code..."
	cd proto && protoc --go_out=. --go_opt=paths=source_relative \
		core/Tron.proto \
		core/Discover.proto \
		core/contract/*.proto \
		api/api.proto
	@echo "Done."

clean:
	rm -rf build/
	$(GO) clean -cache

# Fixture extraction needs a local java-tron; not part of `all` or `test`.
# See docs/dev/fixture-tooling.md.
fixtures-list:
	@scripts/fixtures/run.sh list

fixtures:
	@scripts/fixtures/run.sh all
