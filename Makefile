.PHONY: gtron gtron-replay all test lint proto clean fixtures fixtures-list \
        conformance-replay conformance-replay-exit-gate txsign system-test-flows \
        system-test-cross system-test-cross-flows zksnark-deps gtron-sapling

GOBIN = $(shell pwd)/build/bin
GO ?= go
GOFLAGS =
# Default to pure-Go builds. go-ethereum's cgo secp256k1 wrapper compiles a
# vendored C lib and can stall on small servers; callers can override with
# `make CGO_ENABLED=1 ...` when they explicitly want cgo.
CGO_ENABLED ?= 0

gtron:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build $(GOFLAGS) -o $(GOBIN)/gtron ./cmd/gtron
	@echo "Done building gtron."
	@echo "Run \"$(GOBIN)/gtron\" to launch."

gtron-replay:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build $(GOFLAGS) -o $(GOBIN)/gtron-replay ./cmd/gtron-replay
	@echo "Done building gtron-replay."

all: gtron

test:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) test ./... -count=1 -timeout 300s

lint:
	golangci-lint run ./...

proto:
	@echo "Generating protobuf Go code..."
	cd proto && protoc --go_out=. --go_opt=paths=source_relative \
		core/Tron.proto \
		core/Discover.proto \
		core/contract/*.proto
	cd proto && protoc \
		--proto_path=. \
		--go_out=. --go_opt=paths=source_relative \
		--go_opt=Mgoogle/api/annotations.proto=google.golang.org/genproto/googleapis/api/annotations \
		--go_opt=Mgoogle/api/http.proto=google.golang.org/genproto/googleapis/api/annotations \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mgoogle/api/annotations.proto=google.golang.org/genproto/googleapis/api/annotations \
		--go-grpc_opt=Mgoogle/api/http.proto=google.golang.org/genproto/googleapis/api/annotations \
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

# Build txsign utility.
txsign:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build $(GOFLAGS) -o $(GOBIN)/txsign ./cmd/txsign

# System test flows — builds binaries, starts dev node, runs HTTP flow tests.
# EXIT: non-zero if PASS < 30 or WARN > 4.
system-test-flows: gtron txsign
	@scripts/ci_system_test.sh

# Cross-impl interop smoke: gtron <-> already-running java-tron private chain.
# Requires JAVA_TRON_ADDR (default 127.0.0.1:18888) reachable; optional
# JAVA_TRON_HTTP (default 127.0.0.1:8090) enables byte-level cross-checks.
# See docs/dev/p2p-interop-status.md for the verified-against setup.
system-test-cross: gtron
	@scripts/system_test_cross.sh

# Cross-impl transaction-flow integration tests: drives 7+ contract types
# end-to-end through the gtron <-> java-tron pair and asserts the post-tx
# state on both nodes is byte-equal. java-tron must be running with the
# fixture genesis at /Users/asuka/Works/Tests/TVM/run/config.conf.
system-test-cross-flows: gtron txsign
	@scripts/system_test_cross_flows.sh

# Build the librustzcash static lib (libRustzcash.a + librustzcash.{so,dylib})
# that core/zksnark/pedersen_cgo.go links against under `-tags=sapling`. The
# Rust source lives in third_party/librustzcash as a git submodule
# (tronprotocol/librustzcash, branch release_vm_zksnarks_4.0). On fresh clone
# the submodule must be initialised first:
#
#   git submodule update --init --recursive
#
# The crate is from 2019-era Rust (rand 0.4, blake2-rfc git rev); a recent
# stable toolchain may need a pinned older `rust-toolchain` or `cargo +1.65`.
# See docs/dev/shielded-merkle-audit.md for the verified Rust version.
zksnark-deps:
	@if [ ! -f third_party/librustzcash/Cargo.toml ]; then \
		echo "third_party/librustzcash submodule missing — run \`git submodule update --init --recursive\` first."; \
		exit 1; \
	fi
	@command -v cargo >/dev/null 2>&1 || { \
		echo "cargo not found. Install Rust toolchain: https://rustup.rs"; \
		exit 1; \
	}
	cd third_party/librustzcash && cargo build --release --manifest-path librustzcash/Cargo.toml
	@echo "Built third_party/librustzcash/target/release/librustzcash.{a,so,dylib}"

# Sapling-enabled gtron build. Needs CGO_ENABLED=1 and zksnark-deps to have
# placed libzksnark_capi.{a,so,dylib} on the linker path. Without those the
# default `make gtron` build (pure-Go, no sapling tag) still works and
# shielded tests skip.
gtron-sapling:
	CGO_ENABLED=1 $(GO) build -tags=sapling $(GOFLAGS) -o $(GOBIN)/gtron ./cmd/gtron
	@echo "Done building gtron with Sapling support."
