.PHONY: gtron gtron-replay all test lint proto clean fixtures fixtures-list \
        conformance-replay conformance-replay-exit-gate

GOBIN = $(shell pwd)/build/bin
GO ?= go
GOFLAGS =

gtron:
	$(GO) build $(GOFLAGS) -o $(GOBIN)/gtron ./cmd/gtron
	@echo "Done building gtron."
	@echo "Run \"$(GOBIN)/gtron\" to launch."

gtron-replay:
	$(GO) build $(GOFLAGS) -o $(GOBIN)/gtron-replay ./cmd/gtron-replay
	@echo "Done building gtron-replay."

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

# Conformance replay — exercises core/conformance against one or more
# pre-captured mainnet-blocks ranges. Smoke range runs without git-lfs;
# the real mainnet ranges require `git lfs pull` before use.
# See docs/dev/conformance-harness.md (to be written in PR-5).
conformance-replay: gtron-replay
	@RANGES="$${RANGES:-smoke}" scripts/conformance_replay.sh

conformance-replay-exit-gate: gtron-replay
	@EXIT_GATE=1 RANGES="$${RANGES:-smoke range-freeze-v2 range-maintenance range-contract}" \
		scripts/conformance_replay.sh
