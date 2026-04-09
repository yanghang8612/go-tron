# go-tron Phase 1: Minimal Viable Node — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a TRON node in Go that can sync blocks from TRON testnet, validate them via DPoS consensus, execute core transactions, and expose basic HTTP + JSON-RPC APIs.

**Architecture:** Follow go-ethereum's interface-driven design — Node container manages Lifecycle services, Tron backend wires blockchain/consensus/P2P/txpool, database abstracted behind KeyValueStore interface, all core types wrap protobuf messages. TRON-specific patterns: Actuator registry for transaction dispatch, ResourceProcessor for bandwidth/energy.

**Tech Stack:** Go 1.22+, protobuf (google.golang.org/protobuf), Pebble DB, urfave/cli/v2, secp256k1 (go-ethereum/crypto), Base58

**Spec:** `docs/superpowers/specs/2026-04-09-go-tron-rewrite-design.md`

**Source references:**
- java-tron: `/Users/asuka/Projects/tron/java-tron`
- go-ethereum: `/Users/asuka/Projects/ethereum/go-ethereum`

---

## Task 1: Project Scaffold

**Files:**
- Create: `go.mod`
- Create: `Makefile`
- Create: `cmd/gtron/main.go`
- Create: `.gitignore`

- [ ] **Step 1: Initialize go.mod**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
rm go.mod  # remove stale 1.17 go.mod
go mod init github.com/tronprotocol/go-tron
go mod edit -go=1.22
```

- [ ] **Step 2: Create .gitignore**

Create `.gitignore`:

```gitignore
# Build
build/
*.exe
*.test
*.out

# IDE
.idea/
.vscode/
*.swp

# OS
.DS_Store
Thumbs.db

# Go
vendor/

# Protobuf generated
proto/core/*.pb.go
proto/api/*.pb.go
```

- [ ] **Step 3: Create Makefile**

Create `Makefile`:

```makefile
.PHONY: gtron all test lint proto clean

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
```

- [ ] **Step 4: Create minimal main.go stub**

Create `cmd/gtron/main.go`:

```go
package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

var app = &cli.App{
	Name:    "gtron",
	Usage:   "TRON blockchain node (Go implementation)",
	Version: "0.1.0-dev",
	Action:  gtron,
}

func gtron(ctx *cli.Context) error {
	fmt.Println("gtron: not yet implemented")
	return nil
}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

- [ ] **Step 5: Add CLI dependency, verify build**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
go get github.com/urfave/cli/v2@latest
go mod tidy
make gtron
./build/bin/gtron --version
```

Expected: prints `gtron version 0.1.0-dev`

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum Makefile .gitignore cmd/
git commit -m "scaffold: init project with go.mod, Makefile, and gtron stub"
```

---

## Task 2: common/ — Address, Hash, Bytes

**Files:**
- Create: `common/address.go`
- Create: `common/hash.go`
- Create: `common/bytes.go`
- Test: `common/address_test.go`
- Test: `common/hash_test.go`

- [ ] **Step 1: Write Address type tests**

Create `common/address_test.go`:

```go
package common

import (
	"encoding/hex"
	"testing"
)

func TestAddressSize(t *testing.T) {
	var addr Address
	if len(addr) != AddressLength {
		t.Fatalf("expected %d, got %d", AddressLength, len(addr))
	}
}

func TestBytesToAddress(t *testing.T) {
	b, _ := hex.DecodeString("41" + "a614f803b6fd780986a42c78ec9c7f77e6ded13c")
	addr := BytesToAddress(b)
	if addr[0] != 0x41 {
		t.Fatalf("expected prefix 0x41, got 0x%x", addr[0])
	}
	if hex.EncodeToString(addr[:]) != "41a614f803b6fd780986a42c78ec9c7f77e6ded13c" {
		t.Fatalf("unexpected address: %x", addr)
	}
}

func TestAddressHex(t *testing.T) {
	b, _ := hex.DecodeString("41a614f803b6fd780986a42c78ec9c7f77e6ded13c")
	addr := BytesToAddress(b)
	want := "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
	if addr.Hex() != want {
		t.Fatalf("expected %s, got %s", want, addr.Hex())
	}
}

func TestEmptyAddress(t *testing.T) {
	var addr Address
	if !addr.IsEmpty() {
		t.Fatal("zero address should be empty")
	}
	b, _ := hex.DecodeString("41a614f803b6fd780986a42c78ec9c7f77e6ded13c")
	addr = BytesToAddress(b)
	if addr.IsEmpty() {
		t.Fatal("non-zero address should not be empty")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./common/ -v -run TestAddress
```

Expected: FAIL — package does not exist yet.

- [ ] **Step 3: Implement Address type**

Create `common/address.go`:

```go
package common

import (
	"encoding/hex"
)

const (
	// AddressLength is the TRON address length in bytes (1 prefix + 20 hash).
	AddressLength = 21

	// AddressPrefixMainnet is the mainnet address prefix byte.
	AddressPrefixMainnet = 0x41

	// AddressPrefixTestnet is the testnet address prefix byte.
	AddressPrefixTestnet = 0xa0
)

// Address represents a 21-byte TRON address (0x41 prefix + 20 bytes).
type Address [AddressLength]byte

// BytesToAddress converts a byte slice to Address. If b is shorter than 21
// bytes, it is right-aligned. If longer, it is truncated from the left.
func BytesToAddress(b []byte) Address {
	var a Address
	if len(b) > AddressLength {
		b = b[len(b)-AddressLength:]
	}
	copy(a[AddressLength-len(b):], b)
	return a
}

// Bytes returns the byte representation of the address.
func (a Address) Bytes() []byte {
	return a[:]
}

// Hex returns the hex string of the address without 0x prefix.
func (a Address) Hex() string {
	return hex.EncodeToString(a[:])
}

// IsEmpty returns true if the address is all zeros.
func (a Address) IsEmpty() bool {
	for _, b := range a {
		if b != 0 {
			return false
		}
	}
	return true
}

// String returns the hex representation for display.
func (a Address) String() string {
	return a.Hex()
}
```

- [ ] **Step 4: Run Address tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./common/ -v -run TestAddress
```

Expected: PASS

- [ ] **Step 5: Write Hash type tests**

Create `common/hash_test.go`:

```go
package common

import (
	"encoding/hex"
	"testing"
)

func TestHashSize(t *testing.T) {
	var h Hash
	if len(h) != HashLength {
		t.Fatalf("expected %d, got %d", HashLength, len(h))
	}
}

func TestBytesToHash(t *testing.T) {
	b := make([]byte, 32)
	b[31] = 0xff
	h := BytesToHash(b)
	if h[31] != 0xff {
		t.Fatal("expected last byte 0xff")
	}
}

func TestHashHex(t *testing.T) {
	b := make([]byte, 32)
	b[0] = 0xab
	h := BytesToHash(b)
	s := h.Hex()
	if s[:2] != "ab" {
		t.Fatalf("expected prefix ab, got %s", s[:2])
	}
}

func TestHexToHash(t *testing.T) {
	hexStr := "e58f33f9baf9305dc6f82b9f1934ea8f0ade2defb951258d50167028c780351f"
	h := HexToHash(hexStr)
	if hex.EncodeToString(h[:]) != hexStr {
		t.Fatalf("round trip failed")
	}
}
```

- [ ] **Step 6: Implement Hash type**

Create `common/hash.go`:

```go
package common

import (
	"crypto/sha256"
	"encoding/hex"
)

const (
	// HashLength is the length of a SHA-256 hash in bytes.
	HashLength = 32
)

// Hash represents a 32-byte SHA-256 hash.
type Hash [HashLength]byte

// BytesToHash converts a byte slice to Hash.
func BytesToHash(b []byte) Hash {
	var h Hash
	if len(b) > HashLength {
		b = b[len(b)-HashLength:]
	}
	copy(h[HashLength-len(b):], b)
	return h
}

// HexToHash converts a hex string (without 0x prefix) to Hash.
func HexToHash(s string) Hash {
	b, _ := hex.DecodeString(s)
	return BytesToHash(b)
}

// Sha256 computes the SHA-256 hash of data.
func Sha256(data []byte) Hash {
	return sha256.Sum256(data)
}

// Bytes returns the byte representation.
func (h Hash) Bytes() []byte {
	return h[:]
}

// Hex returns the hex string without 0x prefix.
func (h Hash) Hex() string {
	return hex.EncodeToString(h[:])
}

// IsEmpty returns true if the hash is all zeros.
func (h Hash) IsEmpty() bool {
	for _, b := range h {
		if b != 0 {
			return false
		}
	}
	return true
}

// String returns the hex representation.
func (h Hash) String() string {
	return h.Hex()
}
```

- [ ] **Step 7: Create bytes utilities**

Create `common/bytes.go`:

```go
package common

import "encoding/hex"

// FromHex decodes a hex string (with or without 0x prefix) to bytes.
func FromHex(s string) []byte {
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		s = s[2:]
	}
	if len(s)%2 == 1 {
		s = "0" + s
	}
	b, _ := hex.DecodeString(s)
	return b
}

// ToHex encodes bytes to a hex string without prefix.
func ToHex(b []byte) string {
	return hex.EncodeToString(b)
}

// CopyBytes returns a copy of the byte slice.
func CopyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
```

- [ ] **Step 8: Run all common tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./common/ -v
```

Expected: all PASS

- [ ] **Step 9: Commit**

```bash
git add common/
git commit -m "common: add Address (21-byte), Hash (SHA-256), and byte utilities"
```

---

## Task 3: crypto/ — Signing, Recovery, TRON Address Derivation

**Files:**
- Create: `crypto/crypto.go`
- Create: `crypto/address.go`
- Test: `crypto/crypto_test.go`
- Test: `crypto/address_test.go`

- [ ] **Step 1: Add go-ethereum crypto dependency**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
go get github.com/ethereum/go-ethereum/crypto@latest
go get github.com/mr-tron/base58@latest
go mod tidy
```

- [ ] **Step 2: Write crypto tests**

Create `crypto/crypto_test.go`:

```go
package crypto

import (
	"encoding/hex"
	"testing"
)

func TestGenerateKey(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if key == nil {
		t.Fatal("key is nil")
	}
	privBytes := PrivateKeyToBytes(key)
	if len(privBytes) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(privBytes))
	}
}

func TestSignAndRecover(t *testing.T) {
	key, _ := GenerateKey()
	msg := Keccak256([]byte("test message"))

	sig, err := Sign(msg, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(sig) != 65 {
		t.Fatalf("expected 65 byte sig, got %d", len(sig))
	}

	pub, err := SigToPub(msg, sig)
	if err != nil {
		t.Fatal(err)
	}

	expectedPub := PubkeyToBytes(&key.PublicKey)
	recoveredPub := PubkeyToBytes(pub)
	if hex.EncodeToString(expectedPub) != hex.EncodeToString(recoveredPub) {
		t.Fatal("recovered pubkey does not match")
	}
}

func TestKeccak256(t *testing.T) {
	hash := Keccak256([]byte(""))
	expected := "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
	if hex.EncodeToString(hash) != expected {
		t.Fatalf("expected %s, got %s", expected, hex.EncodeToString(hash))
	}
}
```

- [ ] **Step 3: Implement crypto.go**

Create `crypto/crypto.go`:

```go
package crypto

import (
	"crypto/ecdsa"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
)

// GenerateKey generates a new secp256k1 private key.
func GenerateKey() (*ecdsa.PrivateKey, error) {
	return ethcrypto.GenerateKey()
}

// Sign signs the hash with the private key, producing a 65-byte signature
// (R || S || V).
func Sign(hash []byte, prv *ecdsa.PrivateKey) ([]byte, error) {
	return ethcrypto.Sign(hash, prv)
}

// SigToPub recovers the public key from the hash and signature.
func SigToPub(hash, sig []byte) (*ecdsa.PublicKey, error) {
	return ethcrypto.SigToPub(hash, sig)
}

// PubkeyToBytes serializes a public key to 65 bytes (uncompressed, 0x04 prefix).
func PubkeyToBytes(pub *ecdsa.PublicKey) []byte {
	return ethcrypto.FromECDSAPub(pub)
}

// PrivateKeyToBytes serializes a private key to 32 bytes.
func PrivateKeyToBytes(prv *ecdsa.PrivateKey) []byte {
	return ethcrypto.FromECDSA(prv)
}

// BytesToPrivateKey deserializes 32 bytes to a private key.
func BytesToPrivateKey(b []byte) (*ecdsa.PrivateKey, error) {
	return ethcrypto.ToECDSA(b)
}

// Keccak256 computes the Keccak-256 hash of data.
func Keccak256(data []byte) []byte {
	return ethcrypto.Keccak256(data)
}
```

- [ ] **Step 4: Run crypto tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./crypto/ -v -run TestGenerateKey -run TestSign -run TestKeccak
```

Expected: PASS

- [ ] **Step 5: Write address derivation tests**

Create `crypto/address_test.go`:

```go
package crypto

import (
	"encoding/hex"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestPubkeyToAddress(t *testing.T) {
	// Generate a key and derive address
	key, _ := GenerateKey()
	addr := PubkeyToAddress(&key.PublicKey)

	// Must be 21 bytes with 0x41 prefix
	if len(addr) != common.AddressLength {
		t.Fatalf("expected %d bytes, got %d", common.AddressLength, len(addr))
	}
	if addr[0] != common.AddressPrefixMainnet {
		t.Fatalf("expected prefix 0x41, got 0x%x", addr[0])
	}
}

func TestAddressFromKnownKey(t *testing.T) {
	// Known private key for deterministic test
	privHex := "da146374a75310b9666e834ee4ad0866d6f4035967bfc76217c5a495fff9f0d0"
	privBytes, _ := hex.DecodeString(privHex)
	key, err := BytesToPrivateKey(privBytes)
	if err != nil {
		t.Fatal(err)
	}

	addr := PubkeyToAddress(&key.PublicKey)

	// Address should start with 0x41
	if addr[0] != 0x41 {
		t.Fatalf("expected 0x41 prefix, got 0x%x", addr[0])
	}

	// Should be deterministic
	addr2 := PubkeyToAddress(&key.PublicKey)
	if addr != addr2 {
		t.Fatal("address derivation is not deterministic")
	}
}

func TestBase58CheckEncodeDecode(t *testing.T) {
	addrHex := "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
	addrBytes, _ := hex.DecodeString(addrHex)
	addr := common.BytesToAddress(addrBytes)

	encoded := AddressToBase58(addr)
	if encoded == "" {
		t.Fatal("base58 encoding returned empty")
	}

	decoded, err := Base58ToAddress(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != addr {
		t.Fatalf("round trip failed: got %s, want %s", decoded.Hex(), addr.Hex())
	}
}
```

- [ ] **Step 6: Implement address derivation**

Create `crypto/address.go`:

TRON address derivation: pubkey (65 bytes) -> remove 0x04 prefix (64 bytes) -> Keccak256 -> take last 20 bytes -> prepend 0x41.

Base58Check: address bytes -> SHA256(SHA256(address)) -> take first 4 bytes as checksum -> Base58(address + checksum).

```go
package crypto

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"

	"github.com/mr-tron/base58"
	"github.com/tronprotocol/go-tron/common"
)

// PubkeyToAddress derives a TRON address from a public key.
// Process: uncompressed pubkey (strip 0x04) -> Keccak256 -> last 20 bytes -> prepend 0x41.
func PubkeyToAddress(pub *ecdsa.PublicKey) common.Address {
	pubBytes := PubkeyToBytes(pub)
	// Remove 0x04 uncompressed prefix
	hash := Keccak256(pubBytes[1:])
	// Take last 20 bytes
	var addr common.Address
	addr[0] = common.AddressPrefixMainnet
	copy(addr[1:], hash[len(hash)-20:])
	return addr
}

// AddressToBase58 encodes a TRON address to Base58Check format.
func AddressToBase58(addr common.Address) string {
	b := addr.Bytes()
	// Double SHA-256 checksum
	h1 := sha256.Sum256(b)
	h2 := sha256.Sum256(h1[:])
	// Append first 4 bytes of checksum
	payload := make([]byte, len(b)+4)
	copy(payload, b)
	copy(payload[len(b):], h2[:4])
	return base58.Encode(payload)
}

// Base58ToAddress decodes a Base58Check string to a TRON address.
func Base58ToAddress(s string) (common.Address, error) {
	var addr common.Address
	b, err := base58.Decode(s)
	if err != nil {
		return addr, err
	}
	if len(b) != common.AddressLength+4 {
		return addr, errors.New("invalid base58 address length")
	}
	// Verify checksum
	payload := b[:common.AddressLength]
	checksum := b[common.AddressLength:]
	h1 := sha256.Sum256(payload)
	h2 := sha256.Sum256(h1[:])
	if h2[0] != checksum[0] || h2[1] != checksum[1] ||
		h2[2] != checksum[2] || h2[3] != checksum[3] {
		return addr, errors.New("invalid base58 checksum")
	}
	copy(addr[:], payload)
	return addr, nil
}
```

- [ ] **Step 7: Run address tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./crypto/ -v
```

Expected: all PASS

- [ ] **Step 8: Commit**

```bash
git add crypto/ go.mod go.sum
git commit -m "crypto: add secp256k1 signing, Keccak256, TRON address derivation, Base58Check"
```

---

## Task 4: proto/ — Protobuf Definitions and Code Generation

**Files:**
- Create: `proto/core/Tron.proto` (copy from java-tron)
- Create: `proto/core/contract/*.proto` (copy from java-tron)
- Create: `proto/api/api.proto` (copy from java-tron)
- Create: `proto/generate.sh`

- [ ] **Step 1: Copy proto files from java-tron**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
mkdir -p proto/core/contract proto/api proto/core/tron

# Core protos
cp /Users/asuka/Projects/tron/java-tron/protocol/src/main/protos/core/Tron.proto proto/core/
cp /Users/asuka/Projects/tron/java-tron/protocol/src/main/protos/core/Discover.proto proto/core/
cp /Users/asuka/Projects/tron/java-tron/protocol/src/main/protos/core/TronInventoryItems.proto proto/core/

# Contract protos
cp /Users/asuka/Projects/tron/java-tron/protocol/src/main/protos/core/contract/*.proto proto/core/contract/

# API protos
cp /Users/asuka/Projects/tron/java-tron/protocol/src/main/protos/api/*.proto proto/api/

# Tron sub-directory protos (if they exist)
cp /Users/asuka/Projects/tron/java-tron/protocol/src/main/protos/core/tron/*.proto proto/core/tron/ 2>/dev/null || true
```

- [ ] **Step 2: Add go_package options to proto files**

Every `.proto` file needs a `go_package` option for Go code generation. Add to each file:

For `proto/core/Tron.proto`:
```
option go_package = "github.com/tronprotocol/go-tron/proto/core";
```

For `proto/core/contract/*.proto`:
```
option go_package = "github.com/tronprotocol/go-tron/proto/core/contract";
```

For `proto/api/api.proto`:
```
option go_package = "github.com/tronprotocol/go-tron/proto/api";
```

For `proto/core/tron/*.proto`:
```
option go_package = "github.com/tronprotocol/go-tron/proto/core/tron";
```

Use sed to insert after the `package` line in each proto file:

```bash
cd /Users/asuka/Projects/asuka/go/go-tron

# Core protos
for f in proto/core/*.proto; do
  if ! grep -q 'go_package' "$f"; then
    sed -i '' '/^package /a\
option go_package = "github.com/tronprotocol/go-tron/proto/core";
' "$f"
  fi
done

# Contract protos
for f in proto/core/contract/*.proto; do
  if ! grep -q 'go_package' "$f"; then
    sed -i '' '/^package /a\
option go_package = "github.com/tronprotocol/go-tron/proto/core/contract";
' "$f"
  fi
done

# API protos
for f in proto/api/*.proto; do
  if ! grep -q 'go_package' "$f"; then
    sed -i '' '/^package /a\
option go_package = "github.com/tronprotocol/go-tron/proto/api";
' "$f"
  fi
done

# Tron sub-directory protos
for f in proto/core/tron/*.proto; do
  if ! grep -q 'go_package' "$f"; then
    sed -i '' '/^package /a\
option go_package = "github.com/tronprotocol/go-tron/proto/core/tron";
' "$f"
  fi
done 2>/dev/null || true
```

- [ ] **Step 3: Fix import paths in proto files**

The java-tron proto files use imports like `core/Tron.proto`. These need to be prefixed with the proto root. Check and fix any import path issues:

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
# Verify imports resolve correctly by doing a dry-run compile
protoc --proto_path=proto --go_out=proto --go_opt=paths=source_relative \
  proto/core/contract/common.proto 2>&1 || echo "Fix imports as needed"
```

Fix any import path issues that arise. Common fixes:
- In contract protos: change `import "core/Tron.proto"` paths to match your directory layout
- In api.proto: fix imports to contract and core protos

- [ ] **Step 4: Generate Go code**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

protoc --proto_path=proto \
  --go_out=proto --go_opt=paths=source_relative \
  proto/core/Tron.proto \
  proto/core/Discover.proto \
  proto/core/contract/*.proto

# API proto may need gRPC plugin — skip gRPC for now, generate message types only
protoc --proto_path=proto \
  --go_out=proto --go_opt=paths=source_relative \
  proto/api/api.proto 2>/dev/null || echo "API proto may need gRPC fixes — handle in Phase 3"
```

- [ ] **Step 5: Add protobuf dependency, verify generated code compiles**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
go get google.golang.org/protobuf@latest
go mod tidy
go build ./proto/core/...
go build ./proto/core/contract/...
```

Expected: compiles without errors.

- [ ] **Step 6: Commit**

```bash
git add proto/ go.mod go.sum
git commit -m "proto: add TRON protobuf definitions and generated Go code"
```

---

## Task 5: core/types/ — Block, Transaction, Account Wrappers

**Files:**
- Create: `core/types/block.go`
- Create: `core/types/transaction.go`
- Create: `core/types/account.go`
- Create: `core/types/witness.go`
- Test: `core/types/block_test.go`
- Test: `core/types/transaction_test.go`

- [ ] **Step 1: Write Block type tests**

Create `core/types/block_test.go`:

```go
package types

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestNewBlock(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeader_Raw{
				Number:    100,
				Timestamp: 1000000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	if b.Number() != 100 {
		t.Fatalf("expected number 100, got %d", b.Number())
	}
	if b.Timestamp() != 1000000 {
		t.Fatalf("expected timestamp 1000000, got %d", b.Timestamp())
	}
}

func TestBlockHash(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeader_Raw{
				Number:    1,
				Timestamp: 3000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	h := b.Hash()
	if h.IsEmpty() {
		t.Fatal("hash should not be empty")
	}
	// Hash should be deterministic
	h2 := b.Hash()
	if h != h2 {
		t.Fatal("hash not deterministic")
	}
}

func TestBlockSerialize(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeader_Raw{
				Number:    42,
				Timestamp: 9000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	data, err := b.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	b2, err := UnmarshalBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	if b2.Number() != 42 {
		t.Fatalf("expected 42, got %d", b2.Number())
	}
}

func TestBlockID(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeader_Raw{
				Number: 5,
			},
		},
	}
	b := NewBlockFromPB(pb)
	id := b.ID()
	// BlockID encodes block number in first 8 bytes
	num := id.Number()
	if num != 5 {
		t.Fatalf("expected block number 5 from ID, got %d", num)
	}
}

func TestBlockParentHash(t *testing.T) {
	parent := common.HexToHash("aabbccdd")
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeader_Raw{
				ParentHash: parent.Bytes(),
			},
		},
	}
	b := NewBlockFromPB(pb)
	if b.ParentHash() != parent {
		t.Fatal("parent hash mismatch")
	}
}

// Ensure proto round-trip preserves data
func TestBlockProtoRoundTrip(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeader_Raw{
				Number:         999,
				Timestamp:      123456789,
				WitnessAddress: []byte{0x41, 0x01, 0x02},
				Version:        34,
			},
		},
	}
	b := NewBlockFromPB(pb)
	pb2 := b.Proto()
	if !proto.Equal(pb, pb2) {
		t.Fatal("proto round trip not equal")
	}
}
```

- [ ] **Step 2: Implement Block type**

Create `core/types/block.go`:

```go
package types

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// BlockID combines a block hash with its number. The first 8 bytes of the hash
// are overwritten with the big-endian block number.
type BlockID struct {
	Hash   common.Hash
	Num    uint64
}

// Number returns the block number encoded in the ID.
func (id BlockID) Number() uint64 {
	return id.Num
}

// Block wraps a protobuf Block message with cached derived fields.
type Block struct {
	pb *corepb.Block

	hash     common.Hash
	hashOnce sync.Once
}

// NewBlockFromPB creates a Block from a protobuf message.
func NewBlockFromPB(pb *corepb.Block) *Block {
	return &Block{pb: pb}
}

// Proto returns the underlying protobuf message.
func (b *Block) Proto() *corepb.Block {
	return b.pb
}

// Number returns the block number.
func (b *Block) Number() uint64 {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return 0
	}
	return uint64(b.pb.BlockHeader.RawData.Number)
}

// Timestamp returns the block timestamp in milliseconds.
func (b *Block) Timestamp() int64 {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return 0
	}
	return b.pb.BlockHeader.RawData.Timestamp
}

// ParentHash returns the parent block hash.
func (b *Block) ParentHash() common.Hash {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return common.Hash{}
	}
	return common.BytesToHash(b.pb.BlockHeader.RawData.ParentHash)
}

// WitnessAddress returns the witness address that produced this block.
func (b *Block) WitnessAddress() common.Address {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return common.Address{}
	}
	return common.BytesToAddress(b.pb.BlockHeader.RawData.WitnessAddress)
}

// WitnessSignature returns the block signature bytes.
func (b *Block) WitnessSignature() []byte {
	if b.pb.BlockHeader == nil {
		return nil
	}
	return b.pb.BlockHeader.WitnessSignature
}

// Version returns the block version.
func (b *Block) Version() int32 {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return 0
	}
	return b.pb.BlockHeader.RawData.Version
}

// Transactions returns the transactions in this block as wrapped types.
func (b *Block) Transactions() []*Transaction {
	txs := make([]*Transaction, len(b.pb.Transactions))
	for i, pb := range b.pb.Transactions {
		txs[i] = NewTransactionFromPB(pb)
	}
	return txs
}

// Hash computes the SHA-256 hash of the serialized BlockHeader.raw.
func (b *Block) Hash() common.Hash {
	b.hashOnce.Do(func() {
		if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
			return
		}
		data, err := proto.Marshal(b.pb.BlockHeader.RawData)
		if err != nil {
			return
		}
		b.hash = sha256.Sum256(data)
	})
	return b.hash
}

// ID returns the BlockID (hash with block number encoded in first 8 bytes).
func (b *Block) ID() BlockID {
	h := b.Hash()
	num := b.Number()
	binary.BigEndian.PutUint64(h[:8], num)
	return BlockID{Hash: h, Num: num}
}

// Marshal serializes the block to protobuf bytes.
func (b *Block) Marshal() ([]byte, error) {
	return proto.Marshal(b.pb)
}

// UnmarshalBlock deserializes protobuf bytes to a Block.
func UnmarshalBlock(data []byte) (*Block, error) {
	pb := &corepb.Block{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	return NewBlockFromPB(pb), nil
}
```

- [ ] **Step 3: Write Transaction type tests**

Create `core/types/transaction_test.go`:

```go
package types

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestTransactionHash(t *testing.T) {
	transfer := &contractpb.TransferContract{
		OwnerAddress: []byte{0x41, 0x01},
		ToAddress:    []byte{0x41, 0x02},
		Amount:       1000000,
	}
	anyParam, _ := anypb.New(transfer)

	pb := &corepb.Transaction{
		RawData: &corepb.Transaction_Raw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferContract,
					Parameter: anyParam,
				},
			},
			Timestamp:  12345,
			Expiration: 99999,
		},
	}

	tx := NewTransactionFromPB(pb)
	h := tx.Hash()
	if h.IsEmpty() {
		t.Fatal("tx hash should not be empty")
	}

	// Deterministic
	h2 := tx.Hash()
	if h != h2 {
		t.Fatal("tx hash not deterministic")
	}
}

func TestTransactionContractType(t *testing.T) {
	transfer := &contractpb.TransferContract{
		OwnerAddress: []byte{0x41, 0x01},
		ToAddress:    []byte{0x41, 0x02},
		Amount:       100,
	}
	anyParam, _ := anypb.New(transfer)

	pb := &corepb.Transaction{
		RawData: &corepb.Transaction_Raw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferContract,
					Parameter: anyParam,
				},
			},
		},
	}

	tx := NewTransactionFromPB(pb)
	ct := tx.ContractType()
	if ct != corepb.Transaction_Contract_TransferContract {
		t.Fatalf("expected TransferContract, got %v", ct)
	}
}

func TestTransactionMarshalRoundTrip(t *testing.T) {
	pb := &corepb.Transaction{
		RawData: &corepb.Transaction_Raw{
			Timestamp:  42,
			Expiration: 100,
		},
	}
	tx := NewTransactionFromPB(pb)
	data, err := tx.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := UnmarshalTransaction(data)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(tx.Proto(), tx2.Proto()) {
		t.Fatal("round trip failed")
	}
}
```

- [ ] **Step 4: Implement Transaction type**

Create `core/types/transaction.go`:

```go
package types

import (
	"crypto/sha256"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Transaction wraps a protobuf Transaction message.
type Transaction struct {
	pb *corepb.Transaction

	hash     common.Hash
	hashOnce sync.Once
}

// NewTransactionFromPB creates a Transaction from a protobuf message.
func NewTransactionFromPB(pb *corepb.Transaction) *Transaction {
	return &Transaction{pb: pb}
}

// Proto returns the underlying protobuf message.
func (tx *Transaction) Proto() *corepb.Transaction {
	return tx.pb
}

// Hash computes the SHA-256 hash of the serialized Transaction.raw.
func (tx *Transaction) Hash() common.Hash {
	tx.hashOnce.Do(func() {
		if tx.pb.RawData == nil {
			return
		}
		data, err := proto.Marshal(tx.pb.RawData)
		if err != nil {
			return
		}
		tx.hash = sha256.Sum256(data)
	})
	return tx.hash
}

// ContractType returns the contract type of the first contract in the transaction.
func (tx *Transaction) ContractType() corepb.Transaction_Contract_ContractType {
	if tx.pb.RawData == nil || len(tx.pb.RawData.Contract) == 0 {
		return -1
	}
	return tx.pb.RawData.Contract[0].Type
}

// Contract returns the first contract in the transaction.
func (tx *Transaction) Contract() *corepb.Transaction_Contract {
	if tx.pb.RawData == nil || len(tx.pb.RawData.Contract) == 0 {
		return nil
	}
	return tx.pb.RawData.Contract[0]
}

// Timestamp returns the transaction timestamp.
func (tx *Transaction) Timestamp() int64 {
	if tx.pb.RawData == nil {
		return 0
	}
	return tx.pb.RawData.Timestamp
}

// Expiration returns the transaction expiration time.
func (tx *Transaction) Expiration() int64 {
	if tx.pb.RawData == nil {
		return 0
	}
	return tx.pb.RawData.Expiration
}

// Signatures returns the transaction signatures.
func (tx *Transaction) Signatures() [][]byte {
	return tx.pb.Signature
}

// Size returns the serialized size in bytes.
func (tx *Transaction) Size() int {
	return proto.Size(tx.pb)
}

// Marshal serializes the transaction to protobuf bytes.
func (tx *Transaction) Marshal() ([]byte, error) {
	return proto.Marshal(tx.pb)
}

// UnmarshalTransaction deserializes protobuf bytes to a Transaction.
func UnmarshalTransaction(data []byte) (*Transaction, error) {
	pb := &corepb.Transaction{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	return NewTransactionFromPB(pb), nil
}
```

- [ ] **Step 5: Implement Account and Witness types**

Create `core/types/account.go`:

```go
package types

import (
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Account wraps a protobuf Account message.
type Account struct {
	pb *corepb.Account
}

// NewAccountFromPB creates an Account from a protobuf message.
func NewAccountFromPB(pb *corepb.Account) *Account {
	return &Account{pb: pb}
}

// NewAccount creates a new Account with the given address and type.
func NewAccount(addr common.Address, accType corepb.AccountType) *Account {
	return &Account{
		pb: &corepb.Account{
			Address: addr.Bytes(),
			Type:    accType,
		},
	}
}

func (a *Account) Proto() *corepb.Account    { return a.pb }
func (a *Account) Address() common.Address    { return common.BytesToAddress(a.pb.Address) }
func (a *Account) Balance() int64             { return a.pb.Balance }
func (a *Account) SetBalance(b int64)         { a.pb.Balance = b }
func (a *Account) Type() corepb.AccountType   { return a.pb.Type }
func (a *Account) IsWitness() bool            { return a.pb.IsWitness }
func (a *Account) SetIsWitness(v bool)        { a.pb.IsWitness = v }
func (a *Account) CreateTime() int64          { return a.pb.CreateTime }
func (a *Account) SetCreateTime(t int64)      { a.pb.CreateTime = t }

func (a *Account) Marshal() ([]byte, error) {
	return proto.Marshal(a.pb)
}

func UnmarshalAccount(data []byte) (*Account, error) {
	pb := &corepb.Account{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	return NewAccountFromPB(pb), nil
}
```

Create `core/types/witness.go`:

```go
package types

import (
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Witness wraps a protobuf Witness message.
type Witness struct {
	pb *corepb.Witness
}

func NewWitnessFromPB(pb *corepb.Witness) *Witness {
	return &Witness{pb: pb}
}

func NewWitness(addr common.Address, url string) *Witness {
	return &Witness{
		pb: &corepb.Witness{
			Address: addr.Bytes(),
			Url:     url,
		},
	}
}

func (w *Witness) Proto() *corepb.Witness     { return w.pb }
func (w *Witness) Address() common.Address     { return common.BytesToAddress(w.pb.Address) }
func (w *Witness) VoteCount() int64            { return w.pb.VoteCount }
func (w *Witness) SetVoteCount(v int64)        { w.pb.VoteCount = v }
func (w *Witness) URL() string                 { return w.pb.Url }
func (w *Witness) TotalProduced() int64        { return w.pb.TotalProduced }
func (w *Witness) SetTotalProduced(v int64)    { w.pb.TotalProduced = v }
func (w *Witness) TotalMissed() int64          { return w.pb.TotalMissed }
func (w *Witness) SetTotalMissed(v int64)      { w.pb.TotalMissed = v }
func (w *Witness) IsJobs() bool                { return w.pb.IsJobs }
func (w *Witness) SetIsJobs(v bool)            { w.pb.IsJobs = v }

func (w *Witness) Marshal() ([]byte, error) {
	return proto.Marshal(w.pb)
}

func UnmarshalWitness(data []byte) (*Witness, error) {
	pb := &corepb.Witness{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	return NewWitnessFromPB(pb), nil
}
```

- [ ] **Step 6: Run all types tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/types/ -v
```

Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add core/types/
git commit -m "core/types: add Block, Transaction, Account, Witness protobuf wrappers"
```

---

## Task 6: trondb/ — Database Abstraction Layer

**Files:**
- Create: `trondb/database.go`
- Create: `trondb/memorydb/memorydb.go`
- Test: `trondb/memorydb/memorydb_test.go`

- [ ] **Step 1: Write database interface tests (using memorydb)**

Create `trondb/memorydb/memorydb_test.go`:

```go
package memorydb

import (
	"testing"
)

func TestPutGet(t *testing.T) {
	db := New()
	defer db.Close()

	if err := db.Put([]byte("key1"), []byte("val1")); err != nil {
		t.Fatal(err)
	}
	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val1" {
		t.Fatalf("expected val1, got %s", string(val))
	}
}

func TestHas(t *testing.T) {
	db := New()
	defer db.Close()

	has, _ := db.Has([]byte("missing"))
	if has {
		t.Fatal("should not have key")
	}
	db.Put([]byte("exists"), []byte("v"))
	has, _ = db.Has([]byte("exists"))
	if !has {
		t.Fatal("should have key")
	}
}

func TestDelete(t *testing.T) {
	db := New()
	defer db.Close()

	db.Put([]byte("k"), []byte("v"))
	db.Delete([]byte("k"))
	has, _ := db.Has([]byte("k"))
	if has {
		t.Fatal("key should be deleted")
	}
}

func TestGetMissing(t *testing.T) {
	db := New()
	defer db.Close()

	_, err := db.Get([]byte("nope"))
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestBatch(t *testing.T) {
	db := New()
	defer db.Close()

	batch := db.NewBatch()
	batch.Put([]byte("b1"), []byte("v1"))
	batch.Put([]byte("b2"), []byte("v2"))
	batch.Delete([]byte("b1"))

	// Before write, db should not have keys
	has, _ := db.Has([]byte("b2"))
	if has {
		t.Fatal("batch not yet written")
	}

	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	has, _ = db.Has([]byte("b1"))
	if has {
		t.Fatal("b1 should be deleted in batch")
	}
	val, _ := db.Get([]byte("b2"))
	if string(val) != "v2" {
		t.Fatalf("expected v2, got %s", string(val))
	}
}

func TestIterator(t *testing.T) {
	db := New()
	defer db.Close()

	db.Put([]byte("a-1"), []byte("v1"))
	db.Put([]byte("a-2"), []byte("v2"))
	db.Put([]byte("b-1"), []byte("v3"))

	iter := db.NewIterator([]byte("a-"), nil)
	defer iter.Release()

	count := 0
	for iter.Next() {
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 items with prefix a-, got %d", count)
	}
}
```

- [ ] **Step 2: Define database interfaces**

Create `trondb/database.go`:

```go
package trondb

import "io"

// KeyValueReader provides read access to a key-value store.
type KeyValueReader interface {
	Has(key []byte) (bool, error)
	Get(key []byte) ([]byte, error)
}

// KeyValueWriter provides write access to a key-value store.
type KeyValueWriter interface {
	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

// KeyValueStore combines read and write access with iteration and batch support.
type KeyValueStore interface {
	KeyValueReader
	KeyValueWriter
	NewBatch() Batch
	NewIterator(prefix []byte, start []byte) Iterator
	Stat() (string, error)
	Compact(start []byte, limit []byte) error
	io.Closer
}

// Database is the main database interface.
type Database interface {
	KeyValueStore
}

// Batch is a write-only batch that commits atomically.
type Batch interface {
	KeyValueWriter
	ValueSize() int
	Write() error
	Reset()
}

// Iterator iterates over key-value pairs in key order.
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Release()
	Error() error
}
```

- [ ] **Step 3: Implement memorydb**

Create `trondb/memorydb/memorydb.go`:

```go
package memorydb

import (
	"bytes"
	"errors"
	"sort"
	"sync"

	"github.com/tronprotocol/go-tron/trondb"
)

var errNotFound = errors.New("not found")

// Database is an in-memory key-value database for testing.
type Database struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// New creates a new in-memory database.
func New() *Database {
	return &Database{data: make(map[string][]byte)}
}

func (db *Database) Has(key []byte) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, ok := db.data[string(key)]
	return ok, nil
}

func (db *Database) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	val, ok := db.data[string(key)]
	if !ok {
		return nil, errNotFound
	}
	return append([]byte{}, val...), nil
}

func (db *Database) Put(key []byte, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[string(key)] = append([]byte{}, value...)
	return nil
}

func (db *Database) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.data, string(key))
	return nil
}

func (db *Database) NewBatch() trondb.Batch {
	return &batch{db: db}
}

func (db *Database) NewIterator(prefix []byte, start []byte) trondb.Iterator {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var keys []string
	for k := range db.data {
		if prefix != nil && !bytes.HasPrefix([]byte(k), prefix) {
			continue
		}
		if start != nil && bytes.Compare([]byte(k), start) < 0 {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	items := make([]kv, len(keys))
	for i, k := range keys {
		items[i] = kv{key: []byte(k), value: append([]byte{}, db.data[k]...)}
	}
	return &iterator{items: items, pos: -1}
}

func (db *Database) Stat() (string, error) { return "memorydb", nil }
func (db *Database) Compact(start []byte, limit []byte) error { return nil }
func (db *Database) Close() error { return nil }

// Verify interface compliance.
var _ trondb.Database = (*Database)(nil)

// batch

type batchOp struct {
	key    []byte
	value  []byte
	delete bool
}

type batch struct {
	db  *Database
	ops []batchOp
	size int
}

func (b *batch) Put(key, value []byte) error {
	b.ops = append(b.ops, batchOp{key: append([]byte{}, key...), value: append([]byte{}, value...)})
	b.size += len(key) + len(value)
	return nil
}

func (b *batch) Delete(key []byte) error {
	b.ops = append(b.ops, batchOp{key: append([]byte{}, key...), delete: true})
	b.size += len(key)
	return nil
}

func (b *batch) ValueSize() int { return b.size }

func (b *batch) Write() error {
	b.db.mu.Lock()
	defer b.db.mu.Unlock()
	for _, op := range b.ops {
		if op.delete {
			delete(b.db.data, string(op.key))
		} else {
			b.db.data[string(op.key)] = op.value
		}
	}
	return nil
}

func (b *batch) Reset() {
	b.ops = b.ops[:0]
	b.size = 0
}

// iterator

type kv struct {
	key   []byte
	value []byte
}

type iterator struct {
	items []kv
	pos   int
}

func (it *iterator) Next() bool {
	it.pos++
	return it.pos < len(it.items)
}

func (it *iterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.items) {
		return nil
	}
	return it.items[it.pos].key
}

func (it *iterator) Value() []byte {
	if it.pos < 0 || it.pos >= len(it.items) {
		return nil
	}
	return it.items[it.pos].value
}

func (it *iterator) Release() {}
func (it *iterator) Error() error { return nil }
```

- [ ] **Step 4: Run tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./trondb/... -v
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add trondb/
git commit -m "trondb: add Database/KeyValueStore interfaces and memorydb implementation"
```

---

## Task 7: core/rawdb/ — Database Schema and Accessors

**Files:**
- Create: `core/rawdb/schema.go`
- Create: `core/rawdb/accessors_chain.go`
- Create: `core/rawdb/accessors_block.go`
- Create: `core/rawdb/accessors_account.go`
- Test: `core/rawdb/accessors_test.go`

- [ ] **Step 1: Write accessor tests**

Create `core/rawdb/accessors_test.go`:

```go
package rawdb

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/tronprotocol/go-tron/trondb/memorydb"
)

func TestWriteReadBlock(t *testing.T) {
	db := memorydb.New()
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeader_Raw{
				Number:    42,
				Timestamp: 126000,
			},
		},
	}
	block := types.NewBlockFromPB(pb)
	WriteBlock(db, block)

	got := ReadBlock(db, block.Number())
	if got == nil {
		t.Fatal("block not found")
	}
	if got.Number() != 42 {
		t.Fatalf("expected 42, got %d", got.Number())
	}
}

func TestWriteReadBlockByHash(t *testing.T) {
	db := memorydb.New()
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeader_Raw{Number: 10},
		},
	}
	block := types.NewBlockFromPB(pb)
	WriteBlock(db, block)

	num := ReadBlockNumber(db, block.Hash())
	if num == nil {
		t.Fatal("hash->number mapping not found")
	}
	if *num != 10 {
		t.Fatalf("expected 10, got %d", *num)
	}
}

func TestHeadBlock(t *testing.T) {
	db := memorydb.New()
	WriteHeadBlockHash(db, common.HexToHash("aabb"))
	h := ReadHeadBlockHash(db)
	if h != common.HexToHash("aabb") {
		t.Fatal("head block hash mismatch")
	}
}

func TestWriteReadAccount(t *testing.T) {
	db := memorydb.New()
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := types.NewAccount(addr, corepb.AccountType_Normal)
	acc.SetBalance(1000000)

	WriteAccount(db, addr, acc)
	got := ReadAccount(db, addr)
	if got == nil {
		t.Fatal("account not found")
	}
	if got.Balance() != 1000000 {
		t.Fatalf("expected 1000000, got %d", got.Balance())
	}
}
```

- [ ] **Step 2: Implement schema**

Create `core/rawdb/schema.go`:

```go
package rawdb

import "encoding/binary"

// Key prefixes for the chain database.
var (
	headBlockKey      = []byte("LastBlock")
	headSolidBlockKey = []byte("LastSolidBlock")

	blockPrefix     = []byte("b-")
	blockHashPrefix = []byte("bh-")
	txPrefix        = []byte("tx-")
	txInfoPrefix    = []byte("ti-")
	accountPrefix   = []byte("a-")
	witnessPrefix   = []byte("w-")
	votesPrefix     = []byte("v-")
	proposalPrefix  = []byte("p-")
	codePrefix      = []byte("c-")
	contractPrefix  = []byte("ct-")
	storagePrefix   = []byte("s-")
	dynPropPrefix   = []byte("dp-")

	witnessScheduleKey = []byte("ws")
)

func blockKey(number uint64) []byte {
	k := make([]byte, len(blockPrefix)+8)
	copy(k, blockPrefix)
	binary.BigEndian.PutUint64(k[len(blockPrefix):], number)
	return k
}

func blockHashKey(hash []byte) []byte {
	return append(append([]byte{}, blockHashPrefix...), hash...)
}

func txKey(hash []byte) []byte {
	return append(append([]byte{}, txPrefix...), hash...)
}

func accountKey(addr []byte) []byte {
	return append(append([]byte{}, accountPrefix...), addr...)
}

func witnessKey(addr []byte) []byte {
	return append(append([]byte{}, witnessPrefix...), addr...)
}

func dynPropKey(name string) []byte {
	return append(append([]byte{}, dynPropPrefix...), []byte(name)...)
}
```

- [ ] **Step 3: Implement block accessors**

Create `core/rawdb/accessors_block.go`:

```go
package rawdb

import (
	"encoding/binary"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/trondb"
)

// WriteBlock writes a block to the database, indexed by number and hash.
func WriteBlock(db trondb.KeyValueWriter, block *types.Block) {
	data, err := block.Marshal()
	if err != nil {
		return
	}
	// Index by number
	db.Put(blockKey(block.Number()), data)

	// Index hash -> number
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, block.Number())
	db.Put(blockHashKey(block.Hash().Bytes()), num)
}

// ReadBlock reads a block by number.
func ReadBlock(db trondb.KeyValueReader, number uint64) *types.Block {
	data, err := db.Get(blockKey(number))
	if err != nil {
		return nil
	}
	block, err := types.UnmarshalBlock(data)
	if err != nil {
		return nil
	}
	return block
}

// ReadBlockNumber reads the block number for a given hash.
func ReadBlockNumber(db trondb.KeyValueReader, hash common.Hash) *uint64 {
	data, err := db.Get(blockHashKey(hash.Bytes()))
	if err != nil || len(data) != 8 {
		return nil
	}
	num := binary.BigEndian.Uint64(data)
	return &num
}
```

- [ ] **Step 4: Implement chain metadata and account accessors**

Create `core/rawdb/accessors_chain.go`:

```go
package rawdb

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/trondb"
)

// WriteHeadBlockHash writes the hash of the current head block.
func WriteHeadBlockHash(db trondb.KeyValueWriter, hash common.Hash) {
	db.Put(headBlockKey, hash.Bytes())
}

// ReadHeadBlockHash reads the hash of the current head block.
func ReadHeadBlockHash(db trondb.KeyValueReader) common.Hash {
	data, err := db.Get(headBlockKey)
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteHeadSolidBlockHash writes the hash of the latest solidified block.
func WriteHeadSolidBlockHash(db trondb.KeyValueWriter, hash common.Hash) {
	db.Put(headSolidBlockKey, hash.Bytes())
}

// ReadHeadSolidBlockHash reads the hash of the latest solidified block.
func ReadHeadSolidBlockHash(db trondb.KeyValueReader) common.Hash {
	data, err := db.Get(headSolidBlockKey)
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteDynamicProperty writes a named dynamic property.
func WriteDynamicProperty(db trondb.KeyValueWriter, name string, value []byte) {
	db.Put(dynPropKey(name), value)
}

// ReadDynamicProperty reads a named dynamic property.
func ReadDynamicProperty(db trondb.KeyValueReader, name string) []byte {
	data, err := db.Get(dynPropKey(name))
	if err != nil {
		return nil
	}
	return data
}
```

Create `core/rawdb/accessors_account.go`:

```go
package rawdb

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/trondb"
)

// WriteAccount writes an account to the database.
func WriteAccount(db trondb.KeyValueWriter, addr common.Address, acc *types.Account) {
	data, err := acc.Marshal()
	if err != nil {
		return
	}
	db.Put(accountKey(addr.Bytes()), data)
}

// ReadAccount reads an account from the database.
func ReadAccount(db trondb.KeyValueReader, addr common.Address) *types.Account {
	data, err := db.Get(accountKey(addr.Bytes()))
	if err != nil {
		return nil
	}
	acc, err := types.UnmarshalAccount(data)
	if err != nil {
		return nil
	}
	return acc
}

// DeleteAccount removes an account from the database.
func DeleteAccount(db trondb.KeyValueWriter, addr common.Address) {
	db.Delete(accountKey(addr.Bytes()))
}

// HasAccount checks if an account exists.
func HasAccount(db trondb.KeyValueReader, addr common.Address) bool {
	has, _ := db.Has(accountKey(addr.Bytes()))
	return has
}

// WriteWitness writes a witness to the database.
func WriteWitness(db trondb.KeyValueWriter, addr common.Address, w *types.Witness) {
	data, err := w.Marshal()
	if err != nil {
		return
	}
	db.Put(witnessKey(addr.Bytes()), data)
}

// ReadWitness reads a witness from the database.
func ReadWitness(db trondb.KeyValueReader, addr common.Address) *types.Witness {
	data, err := db.Get(witnessKey(addr.Bytes()))
	if err != nil {
		return nil
	}
	w, err := types.UnmarshalWitness(data)
	if err != nil {
		return nil
	}
	return w
}
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/rawdb/ -v
```

Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add core/rawdb/
git commit -m "core/rawdb: add DB schema, block/account/chain metadata accessors"
```

---

## Task 8: params/ — Chain Config, Constants, Genesis

**Files:**
- Create: `params/protocol_params.go`
- Create: `params/config.go`
- Create: `params/genesis.go`
- Test: `params/config_test.go`

- [ ] **Step 1: Write constants and config test**

Create `params/config_test.go`:

```go
package params

import (
	"testing"
)

func TestBlockInterval(t *testing.T) {
	if BlockProducedInterval != 3000 {
		t.Fatalf("expected 3000, got %d", BlockProducedInterval)
	}
}

func TestMaxActiveWitnesses(t *testing.T) {
	if MaxActiveWitnessNum != 27 {
		t.Fatalf("expected 27, got %d", MaxActiveWitnessNum)
	}
}

func TestMainnetConfig(t *testing.T) {
	cfg := MainnetChainConfig
	if cfg.ChainID != 1 {
		t.Fatalf("expected mainnet chain ID 1, got %d", cfg.ChainID)
	}
	if cfg.P2PVersion != 11111 {
		t.Fatalf("expected P2P version 11111, got %d", cfg.P2PVersion)
	}
}

func TestNileConfig(t *testing.T) {
	cfg := NileChainConfig
	if cfg.P2PVersion != 201910292 {
		t.Fatalf("expected nile P2P version 201910292, got %d", cfg.P2PVersion)
	}
}
```

- [ ] **Step 2: Implement protocol constants**

Create `params/protocol_params.go`:

```go
package params

// TRON protocol constants matching java-tron ChainConstant.
const (
	// BlockProducedInterval is the time between blocks in milliseconds.
	BlockProducedInterval = 3000

	// MaxActiveWitnessNum is the number of active block-producing witnesses.
	MaxActiveWitnessNum = 27

	// WitnessStandbyLength is the total number of standby witnesses.
	WitnessStandbyLength = 127

	// SingleRepeat is how many consecutive slots each witness gets.
	SingleRepeat = 1

	// SolidifiedThreshold is the percentage of witnesses needed for solidification.
	SolidifiedThreshold = 70

	// MaintenanceSkipSlots is slots skipped during maintenance.
	MaintenanceSkipSlots = 2

	// MaxVoteNumber is max votes per VoteWitness transaction.
	MaxVoteNumber = 30

	// TRXPrecision is 1 TRX in sun (smallest unit).
	TRXPrecision = 1_000_000

	// BlockSize is the max block size in bytes.
	BlockSize = 2_000_000

	// ClockMaxDelay is the max acceptable clock drift in milliseconds.
	ClockMaxDelay = 3_600_000

	// BlockProduceTimeoutPercent is the block production timeout percentage.
	BlockProduceTimeoutPercent = 50

	// FrozenPeriod is the minimum freeze duration in milliseconds (1 day).
	FrozenPeriod = 86_400_000

	// DelegatePeriod is the delegate lock period in milliseconds (3 days).
	DelegatePeriod = 3 * 86_400_000

	// DefaultMaintenanceInterval is 6 hours in milliseconds.
	DefaultMaintenanceInterval = 6 * 3600 * 1000

	// BlockVersion is the current block version.
	BlockVersion = 34

	// WindowSizeMs is the bandwidth/energy recovery window (24 hours in ms).
	WindowSizeMs = 86_400_000

	// WindowSizeSlots is WindowSizeMs / BlockProducedInterval.
	WindowSizeSlots = WindowSizeMs / BlockProducedInterval
)
```

- [ ] **Step 3: Implement chain config**

Create `params/config.go`:

```go
package params

import "github.com/tronprotocol/go-tron/common"

// ChainConfig holds the configuration for a TRON chain.
type ChainConfig struct {
	ChainID    int64
	P2PVersion int32

	// Genesis block hash (set after genesis is built)
	GenesisHash common.Hash

	// Default ports
	P2PPort     int
	HTTPPort    int
	GRPCPort    int
	JSONRPCPort int
}

// MainnetChainConfig is the chain config for TRON mainnet.
var MainnetChainConfig = &ChainConfig{
	ChainID:     1,
	P2PVersion:  11111,
	P2PPort:     18888,
	HTTPPort:    8090,
	GRPCPort:    50051,
	JSONRPCPort: 8545,
}

// NileChainConfig is the chain config for Nile testnet.
var NileChainConfig = &ChainConfig{
	ChainID:     3448148188,
	P2PVersion:  201910292,
	P2PPort:     18888,
	HTTPPort:    8090,
	GRPCPort:    50051,
	JSONRPCPort: 8545,
}
```

- [ ] **Step 4: Implement genesis**

Create `params/genesis.go`:

```go
package params

import "github.com/tronprotocol/go-tron/common"

// GenesisAccount defines an initial account allocation.
type GenesisAccount struct {
	Address common.Address
	Balance int64
}

// GenesisWitness defines an initial witness.
type GenesisWitness struct {
	Address   common.Address
	VoteCount int64
	URL       string
}

// Genesis defines the genesis block configuration.
type Genesis struct {
	Timestamp int64
	ParentHash common.Hash
	Accounts  []GenesisAccount
	Witnesses []GenesisWitness
}
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./params/ -v
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add params/
git commit -m "params: add protocol constants, ChainConfig, Genesis definitions"
```

---

## Task 9: consensus/dpos/ — DPoS Slot Calculation and Witness Scheduling

**Files:**
- Create: `consensus/consensus.go`
- Create: `consensus/dpos/slot.go`
- Create: `consensus/dpos/schedule.go`
- Test: `consensus/dpos/slot_test.go`
- Test: `consensus/dpos/schedule_test.go`

- [ ] **Step 1: Write slot calculation tests**

Create `consensus/dpos/slot_test.go`:

```go
package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/params"
)

func TestAbsoluteSlot(t *testing.T) {
	genesisTime := int64(0) // epoch
	tests := []struct {
		time int64
		want int64
	}{
		{0, 0},
		{3000, 1},
		{6000, 2},
		{9000, 3},
		{2999, 0},
		{3001, 1},
		{90000, 30},
	}
	for _, tt := range tests {
		got := AbsoluteSlot(tt.time, genesisTime)
		if got != tt.want {
			t.Errorf("AbsoluteSlot(%d, %d) = %d, want %d", tt.time, genesisTime, got, tt.want)
		}
	}
}

func TestSlotTime(t *testing.T) {
	genesisTime := int64(0)
	// If head block is at genesis, time for slot 1 = genesis + 3000
	headTime := int64(0)
	got := SlotTime(1, headTime, genesisTime, false, 0)
	if got != 3000 {
		t.Fatalf("expected 3000, got %d", got)
	}

	got = SlotTime(2, headTime, genesisTime, false, 0)
	if got != 6000 {
		t.Fatalf("expected 6000, got %d", got)
	}
}

func TestSlotTimeAligned(t *testing.T) {
	// Head block at timestamp 10000 (not aligned to 3s boundary)
	// genesis = 0, so aligned head = 10000 - (10000 % 3000) = 9000
	genesisTime := int64(0)
	headTime := int64(10000)
	got := SlotTime(1, headTime, genesisTime, false, 0)
	// slot 1 = aligned(10000) + 3000 = 9000 + 3000 = 12000
	if got != 12000 {
		t.Fatalf("expected 12000, got %d", got)
	}
}

func TestSlotForTime(t *testing.T) {
	genesisTime := int64(0)
	headTime := int64(0)
	// Time 3000 should be slot 1
	got := SlotForTime(3000, headTime, genesisTime, false, 0)
	if got != 1 {
		t.Fatalf("expected slot 1, got %d", got)
	}

	// Time 6000 should be slot 2
	got = SlotForTime(6000, headTime, genesisTime, false, 0)
	if got != 2 {
		t.Fatalf("expected slot 2, got %d", got)
	}
}

func TestWitnessIndex(t *testing.T) {
	_ = params.MaxActiveWitnessNum
	// With 27 witnesses, absolute slot 0 -> witness 0, slot 1 -> witness 1, etc.
	tests := []struct {
		absSlot      int64
		witnessCount int
		want         int
	}{
		{0, 27, 0},
		{1, 27, 1},
		{26, 27, 26},
		{27, 27, 0}, // wraps
		{28, 27, 1},
		{54, 27, 0}, // wraps again
	}
	for _, tt := range tests {
		got := WitnessIndex(tt.absSlot, tt.witnessCount)
		if got != tt.want {
			t.Errorf("WitnessIndex(%d, %d) = %d, want %d", tt.absSlot, tt.witnessCount, got, tt.want)
		}
	}
}
```

- [ ] **Step 2: Implement slot calculation**

Create `consensus/dpos/slot.go`:

```go
package dpos

import "github.com/tronprotocol/go-tron/params"

// AbsoluteSlot returns the absolute slot number for a given timestamp.
// Formula: (time - genesisTime) / BLOCK_PRODUCED_INTERVAL
func AbsoluteSlot(timestamp, genesisTime int64) int64 {
	return (timestamp - genesisTime) / params.BlockProducedInterval
}

// SlotTime returns the timestamp for a given relative slot number.
// Matches java-tron DposSlot.getTime().
func SlotTime(slot int64, headTimestamp, genesisTime int64, isMaintenance bool, maintenanceSkipSlots int64) int64 {
	if slot == 0 {
		return 0
	}
	interval := int64(params.BlockProducedInterval)

	// If chain just started (head == genesis), calculate from genesis
	if headTimestamp == genesisTime {
		return genesisTime + slot*interval
	}

	if isMaintenance {
		slot += maintenanceSkipSlots
	}

	// Align head timestamp to interval boundary
	aligned := headTimestamp - ((headTimestamp - genesisTime) % interval)
	return aligned + interval*slot
}

// SlotForTime returns the relative slot number for a given timestamp.
// Matches java-tron DposSlot.getSlot().
func SlotForTime(timestamp, headTimestamp, genesisTime int64, isMaintenance bool, maintenanceSkipSlots int64) int64 {
	firstSlotTime := SlotTime(1, headTimestamp, genesisTime, isMaintenance, maintenanceSkipSlots)
	if timestamp < firstSlotTime {
		return 0
	}
	return (timestamp-firstSlotTime)/int64(params.BlockProducedInterval) + 1
}

// WitnessIndex returns which witness should produce for a given absolute slot.
// Formula: (absoluteSlot % (witnessCount * SINGLE_REPEAT)) / SINGLE_REPEAT
func WitnessIndex(absoluteSlot int64, witnessCount int) int {
	if witnessCount <= 0 {
		return 0
	}
	idx := absoluteSlot % int64(witnessCount*params.SingleRepeat)
	return int(idx / params.SingleRepeat)
}
```

- [ ] **Step 3: Write schedule tests**

Create `consensus/dpos/schedule_test.go`:

```go
package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestGetScheduledWitness(t *testing.T) {
	// Create 3 witnesses for simple testing
	witnesses := []common.Address{
		common.BytesToAddress([]byte{0x41, 1}),
		common.BytesToAddress([]byte{0x41, 2}),
		common.BytesToAddress([]byte{0x41, 3}),
	}

	genesisTime := int64(0)
	headTimestamp := int64(0) // at genesis

	// Slot 1 (time=3000): absSlot = (0+3000)/3000 = 1 -> witness index 1%3=1
	addr := GetScheduledWitness(1, headTimestamp, genesisTime, witnesses, false, 0)
	if addr != witnesses[1] {
		t.Fatalf("slot 1: expected witness[1], got %s", addr.Hex())
	}

	// Slot 3 (time=9000): absSlot = (0+9000)/3000 = 3 -> 3%3=0
	addr = GetScheduledWitness(3, headTimestamp, genesisTime, witnesses, false, 0)
	if addr != witnesses[0] {
		t.Fatalf("slot 3: expected witness[0], got %s", addr.Hex())
	}
}

func TestSortWitnesses(t *testing.T) {
	w1 := WitnessVote{Address: common.BytesToAddress([]byte{0x41, 0xaa}), Votes: 100}
	w2 := WitnessVote{Address: common.BytesToAddress([]byte{0x41, 0xbb}), Votes: 200}
	w3 := WitnessVote{Address: common.BytesToAddress([]byte{0x41, 0xcc}), Votes: 200} // same votes as w2

	sorted := SortWitnessesByVotes([]WitnessVote{w1, w2, w3})
	// w2 and w3 have same votes (200), should come before w1 (100)
	if sorted[0].Votes != 200 {
		t.Fatalf("expected highest votes first, got %d", sorted[0].Votes)
	}
	if sorted[2].Votes != 100 {
		t.Fatalf("expected lowest votes last, got %d", sorted[2].Votes)
	}
}
```

- [ ] **Step 4: Implement schedule**

Create `consensus/dpos/schedule.go`:

```go
package dpos

import (
	"sort"

	"github.com/tronprotocol/go-tron/common"
)

// WitnessVote holds a witness address and its vote count for sorting.
type WitnessVote struct {
	Address common.Address
	Votes   int64
}

// GetScheduledWitness returns which witness should produce for the given relative slot.
func GetScheduledWitness(slot int64, headTimestamp, genesisTime int64, activeWitnesses []common.Address, isMaintenance bool, maintenanceSkipSlots int64) common.Address {
	if len(activeWitnesses) == 0 {
		return common.Address{}
	}
	currentAbsSlot := AbsoluteSlot(headTimestamp, genesisTime) + slot
	idx := WitnessIndex(currentAbsSlot, len(activeWitnesses))
	return activeWitnesses[idx]
}

// SortWitnessesByVotes sorts witnesses by vote count descending, then by
// address hex string descending (matching java-tron's sort optimization).
func SortWitnessesByVotes(witnesses []WitnessVote) []WitnessVote {
	sorted := make([]WitnessVote, len(witnesses))
	copy(sorted, witnesses)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Votes != sorted[j].Votes {
			return sorted[i].Votes > sorted[j].Votes
		}
		// Secondary sort: hex string descending
		return sorted[i].Address.Hex() > sorted[j].Address.Hex()
	})
	return sorted
}
```

- [ ] **Step 5: Define consensus Engine interface**

Create `consensus/consensus.go`:

```go
package consensus

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// Engine is the consensus engine interface.
type Engine interface {
	// VerifyHeader checks whether a header conforms to the consensus rules.
	VerifyHeader(chain ChainReader, header *types.Block) error

	// GetScheduledWitness returns the witness for a given slot.
	GetScheduledWitness(slot int64) (common.Address, error)

	// IsInMaintenance returns whether the given timestamp falls in a maintenance period.
	IsInMaintenance(timestamp int64) bool
}

// ChainReader provides chain state to the consensus engine.
type ChainReader interface {
	CurrentBlock() *types.Block
	GetBlockByNumber(number uint64) *types.Block
	GenesisTimestamp() int64
	ActiveWitnesses() []common.Address
	NextMaintenanceTime() int64
}
```

- [ ] **Step 6: Run tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./consensus/... -v
```

Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add consensus/
git commit -m "consensus: add Engine interface, DPoS slot calculation and witness scheduling"
```

---

## Task 10: actuator/ — Actuator Interface and TransferActuator

**Files:**
- Create: `actuator/actuator.go`
- Create: `actuator/transfer.go`
- Test: `actuator/transfer_test.go`

- [ ] **Step 1: Write transfer actuator tests**

Create `actuator/transfer_test.go`:

```go
package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/trondb/memorydb"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeTransferTx(from, to common.Address, amount int64) *types.Transaction {
	transfer := &contractpb.TransferContract{
		OwnerAddress: from.Bytes(),
		ToAddress:    to.Bytes(),
		Amount:       amount,
	}
	anyParam, _ := anypb.New(transfer)
	pb := &corepb.Transaction{
		RawData: &corepb.Transaction_Raw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func setupDB(accounts map[common.Address]int64) *memorydb.Database {
	db := memorydb.New()
	for addr, balance := range accounts {
		acc := types.NewAccount(addr, corepb.AccountType_Normal)
		acc.SetBalance(balance)
		rawdb.WriteAccount(db, addr, acc)
	}
	return db
}

func TestTransferValidate_Success(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	to := common.BytesToAddress([]byte{0x41, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{
		from: 10_000_000,
		to:   0,
	})

	tx := makeTransferTx(from, to, 5_000_000)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestTransferValidate_InsufficientBalance(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	to := common.BytesToAddress([]byte{0x41, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{
		from: 100,
		to:   0,
	})

	tx := makeTransferTx(from, to, 5_000_000)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should fail for insufficient balance")
	}
}

func TestTransferValidate_NegativeAmount(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	to := common.BytesToAddress([]byte{0x41, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{from: 10_000_000})

	tx := makeTransferTx(from, to, -1)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject negative amount")
	}
}

func TestTransferValidate_SelfTransfer(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{from: 10_000_000})

	tx := makeTransferTx(from, from, 100)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject self-transfer")
	}
}

func TestTransferExecute_Success(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	to := common.BytesToAddress([]byte{0x41, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{
		from: 10_000_000,
		to:   5_000_000,
	})

	tx := makeTransferTx(from, to, 3_000_000)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result == nil {
		t.Fatal("result should not be nil")
	}

	// Check balances
	fromAcc := rawdb.ReadAccount(db, from)
	toAcc := rawdb.ReadAccount(db, to)
	if fromAcc.Balance() != 7_000_000 {
		t.Fatalf("from balance: expected 7000000, got %d", fromAcc.Balance())
	}
	if toAcc.Balance() != 8_000_000 {
		t.Fatalf("to balance: expected 8000000, got %d", toAcc.Balance())
	}
}

func TestTransferExecute_CreatesRecipient(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	to := common.BytesToAddress([]byte{0x41, 3, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{from: 10_000_000})
	// 'to' does not exist

	tx := makeTransferTx(from, to, 1_000_000)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	_, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	// Recipient should now exist
	toAcc := rawdb.ReadAccount(db, to)
	if toAcc == nil {
		t.Fatal("recipient account should have been created")
	}
	if toAcc.Balance() != 1_000_000 {
		t.Fatalf("to balance: expected 1000000, got %d", toAcc.Balance())
	}
}
```

- [ ] **Step 2: Implement actuator interface and registry**

Create `actuator/actuator.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/tronprotocol/go-tron/trondb"
)

// Context holds the execution context for an actuator.
type Context struct {
	DB trondb.Database
	Tx *types.Transaction
}

// Result holds the result of actuator execution.
type Result struct {
	Fee int64
}

// Actuator is the interface for transaction executors.
type Actuator interface {
	Validate(ctx *Context) error
	Execute(ctx *Context) (*Result, error)
}

// CreateActuator creates the appropriate actuator for a transaction.
func CreateActuator(tx *types.Transaction) (Actuator, error) {
	ct := tx.ContractType()
	switch ct {
	case corepb.Transaction_Contract_TransferContract:
		return &TransferActuator{}, nil
	default:
		return nil, errors.New("unsupported contract type")
	}
}
```

- [ ] **Step 3: Implement TransferActuator**

Create `actuator/transfer.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// TransferActuator handles TransferContract (TRX transfers).
type TransferActuator struct{}

func (a *TransferActuator) getContract(ctx *Context) (*contractpb.TransferContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	tc := &contractpb.TransferContract{}
	if err := proto.UnmarshalOptions{}.Unmarshal(contract.Parameter.Value, tc); err != nil {
		// Try anypb unmarshal
		if err2 := contract.Parameter.UnmarshalTo(tc); err2 != nil {
			return nil, errors.New("failed to unmarshal TransferContract")
		}
	}
	return tc, nil
}

func (a *TransferActuator) Validate(ctx *Context) error {
	tc, err := a.getContract(ctx)
	if err != nil {
		return err
	}

	ownerAddr := common.BytesToAddress(tc.OwnerAddress)
	toAddr := common.BytesToAddress(tc.ToAddress)

	// Cannot transfer to self
	if ownerAddr == toAddr {
		return errors.New("cannot transfer to self")
	}

	// Amount must be positive
	if tc.Amount <= 0 {
		return errors.New("transfer amount must be positive")
	}

	// Owner must exist
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}

	// Sufficient balance
	if ownerAcc.Balance() < tc.Amount {
		return errors.New("insufficient balance")
	}

	return nil
}

func (a *TransferActuator) Execute(ctx *Context) (*Result, error) {
	tc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	ownerAddr := common.BytesToAddress(tc.OwnerAddress)
	toAddr := common.BytesToAddress(tc.ToAddress)

	// Deduct from owner
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	ownerAcc.SetBalance(ownerAcc.Balance() - tc.Amount)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	// Credit to recipient (create if needed)
	toAcc := rawdb.ReadAccount(ctx.DB, toAddr)
	if toAcc == nil {
		toAcc = types.NewAccount(toAddr, corepb.AccountType_Normal)
	}
	toAcc.SetBalance(toAcc.Balance() + tc.Amount)
	rawdb.WriteAccount(ctx.DB, toAddr, toAcc)

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 4: Run tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -v
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add actuator/
git commit -m "actuator: add Actuator interface, registry, and TransferActuator"
```

---

## Task 11: actuator/ — CreateAccount and WitnessCreate Actuators

**Files:**
- Create: `actuator/account.go`
- Create: `actuator/witness.go`
- Test: `actuator/account_test.go`
- Test: `actuator/witness_test.go`

- [ ] **Step 1: Write CreateAccount tests**

Create `actuator/account_test.go`:

```go
package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeCreateAccountTx(owner, newAddr common.Address) *types.Transaction {
	contract := &contractpb.AccountCreateContract{
		OwnerAddress:   owner.Bytes(),
		AccountAddress: newAddr.Bytes(),
		Type:           corepb.AccountType_Normal,
	}
	anyParam, _ := anypb.New(contract)
	pb := &corepb.Transaction{
		RawData: &corepb.Transaction_Raw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_AccountCreateContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func TestCreateAccountValidate_Success(t *testing.T) {
	owner := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	newAddr := common.BytesToAddress([]byte{0x41, 5, 5, 5, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{owner: 10_000_000})

	tx := makeCreateAccountTx(owner, newAddr)
	ctx := &Context{DB: db, Tx: tx}
	act := &CreateAccountActuator{}

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestCreateAccountValidate_AlreadyExists(t *testing.T) {
	owner := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	existing := common.BytesToAddress([]byte{0x41, 5, 5, 5, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{owner: 10_000_000, existing: 0})

	tx := makeCreateAccountTx(owner, existing)
	ctx := &Context{DB: db, Tx: tx}
	act := &CreateAccountActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject already existing account")
	}
}

func TestCreateAccountExecute(t *testing.T) {
	owner := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	newAddr := common.BytesToAddress([]byte{0x41, 7, 7, 7, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{owner: 10_000_000})

	tx := makeCreateAccountTx(owner, newAddr)
	ctx := &Context{DB: db, Tx: tx}
	act := &CreateAccountActuator{}

	_, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	newAcc := rawdb.ReadAccount(db, newAddr)
	if newAcc == nil {
		t.Fatal("new account should exist")
	}
}
```

- [ ] **Step 2: Implement CreateAccountActuator**

Create `actuator/account.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// CreateAccountActuator handles AccountCreateContract.
type CreateAccountActuator struct{}

func (a *CreateAccountActuator) getContract(ctx *Context) (*contractpb.AccountCreateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	ac := &contractpb.AccountCreateContract{}
	if err := contract.Parameter.UnmarshalTo(ac); err != nil {
		return nil, errors.New("failed to unmarshal AccountCreateContract")
	}
	return ac, nil
}

func (a *CreateAccountActuator) Validate(ctx *Context) error {
	ac, err := a.getContract(ctx)
	if err != nil {
		return err
	}

	ownerAddr := common.BytesToAddress(ac.OwnerAddress)
	newAddr := common.BytesToAddress(ac.AccountAddress)

	if ownerAddr.IsEmpty() || newAddr.IsEmpty() {
		return errors.New("invalid address")
	}

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}

	if rawdb.HasAccount(ctx.DB, newAddr) {
		return errors.New("account already exists")
	}

	return nil
}

func (a *CreateAccountActuator) Execute(ctx *Context) (*Result, error) {
	ac, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	newAddr := common.BytesToAddress(ac.AccountAddress)
	accType := ac.Type
	if accType == 0 {
		accType = corepb.AccountType_Normal
	}
	newAcc := types.NewAccount(newAddr, accType)
	rawdb.WriteAccount(ctx.DB, newAddr, newAcc)

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 3: Write WitnessCreate tests**

Create `actuator/witness_test.go`:

```go
package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeWitnessCreateTx(owner common.Address, url string) *types.Transaction {
	contract := &contractpb.WitnessCreateContract{
		OwnerAddress: owner.Bytes(),
		Url:          []byte(url),
	}
	anyParam, _ := anypb.New(contract)
	pb := &corepb.Transaction{
		RawData: &corepb.Transaction_Raw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_WitnessCreateContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func TestWitnessCreateExecute(t *testing.T) {
	owner := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{owner: 100_000_000_000})

	tx := makeWitnessCreateTx(owner, "http://test.com")
	ctx := &Context{DB: db, Tx: tx}
	act := &WitnessCreateActuator{}

	_, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	w := rawdb.ReadWitness(db, owner)
	if w == nil {
		t.Fatal("witness should exist after creation")
	}
	if w.URL() != "http://test.com" {
		t.Fatalf("expected url http://test.com, got %s", w.URL())
	}
	if w.VoteCount() != 0 {
		t.Fatalf("initial vote count should be 0, got %d", w.VoteCount())
	}
}

func TestWitnessCreateValidate_AlreadyWitness(t *testing.T) {
	owner := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{owner: 100_000_000_000})

	// Create witness first
	w := types.NewWitness(owner, "http://existing.com")
	rawdb.WriteWitness(db, owner, w)

	tx := makeWitnessCreateTx(owner, "http://new.com")
	ctx := &Context{DB: db, Tx: tx}
	act := &WitnessCreateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject duplicate witness")
	}
}
```

- [ ] **Step 4: Implement WitnessCreateActuator**

Create `actuator/witness.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// WitnessCreateActuator handles WitnessCreateContract.
type WitnessCreateActuator struct{}

func (a *WitnessCreateActuator) getContract(ctx *Context) (*contractpb.WitnessCreateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	wc := &contractpb.WitnessCreateContract{}
	if err := contract.Parameter.UnmarshalTo(wc); err != nil {
		return nil, errors.New("failed to unmarshal WitnessCreateContract")
	}
	return wc, nil
}

func (a *WitnessCreateActuator) Validate(ctx *Context) error {
	wc, err := a.getContract(ctx)
	if err != nil {
		return err
	}

	ownerAddr := common.BytesToAddress(wc.OwnerAddress)

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}

	if rawdb.ReadWitness(ctx.DB, ownerAddr) != nil {
		return errors.New("witness already exists")
	}

	if len(wc.Url) == 0 {
		return errors.New("witness URL is empty")
	}

	return nil
}

func (a *WitnessCreateActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	ownerAddr := common.BytesToAddress(wc.OwnerAddress)

	witness := types.NewWitness(ownerAddr, string(wc.Url))
	rawdb.WriteWitness(ctx.DB, ownerAddr, witness)

	// Mark account as witness
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	ownerAcc.SetIsWitness(true)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 5: Register new actuators in the factory**

Update `actuator/actuator.go` — add to the switch in `CreateActuator`:

```go
	case corepb.Transaction_Contract_AccountCreateContract:
		return &CreateAccountActuator{}, nil
	case corepb.Transaction_Contract_WitnessCreateContract:
		return &WitnessCreateActuator{}, nil
```

- [ ] **Step 6: Run tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -v
```

Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add actuator/
git commit -m "actuator: add CreateAccount and WitnessCreate actuators"
```

---

## Task 12: node/ — Node Container and Lifecycle

**Files:**
- Create: `node/lifecycle.go`
- Create: `node/node.go`
- Create: `node/config.go`
- Test: `node/node_test.go`

- [ ] **Step 1: Write node lifecycle tests**

Create `node/node_test.go`:

```go
package node

import (
	"errors"
	"testing"
)

type mockService struct {
	started bool
	stopped bool
	failStart bool
}

func (s *mockService) Start() error {
	if s.failStart {
		return errors.New("start failed")
	}
	s.started = true
	return nil
}

func (s *mockService) Stop() error {
	s.stopped = true
	return nil
}

func TestNodeStartStop(t *testing.T) {
	cfg := &Config{DataDir: t.TempDir()}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	svc := &mockService{}
	n.RegisterLifecycle(svc)

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	if !svc.started {
		t.Fatal("service should be started")
	}

	n.Stop()
	if !svc.stopped {
		t.Fatal("service should be stopped")
	}
}

func TestNodeStartFailure(t *testing.T) {
	cfg := &Config{DataDir: t.TempDir()}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	good := &mockService{}
	bad := &mockService{failStart: true}
	n.RegisterLifecycle(good)
	n.RegisterLifecycle(bad)

	if err := n.Start(); err == nil {
		t.Fatal("should fail when service fails to start")
	}
	// Good service should be stopped on rollback
	if !good.stopped {
		t.Fatal("previously started service should be stopped on failure")
	}
}
```

- [ ] **Step 2: Implement Lifecycle interface**

Create `node/lifecycle.go`:

```go
package node

// Lifecycle defines the start/stop interface for node services.
type Lifecycle interface {
	Start() error
	Stop() error
}
```

- [ ] **Step 3: Implement Config**

Create `node/config.go`:

```go
package node

// Config holds node-level configuration.
type Config struct {
	DataDir     string
	P2PPort     int
	HTTPPort    int
	JSONRPCPort int
}
```

- [ ] **Step 4: Implement Node**

Create `node/node.go`:

```go
package node

import (
	"sync"
)

// Node is the top-level container that manages service lifecycles.
type Node struct {
	config     *Config
	lifecycles []Lifecycle
	running    bool
	lock       sync.Mutex
	stop       chan struct{}
}

// New creates a new Node with the given config.
func New(config *Config) (*Node, error) {
	return &Node{
		config: config,
		stop:   make(chan struct{}),
	}, nil
}

// Config returns the node configuration.
func (n *Node) Config() *Config {
	return n.config
}

// RegisterLifecycle registers a service to be started/stopped with the node.
func (n *Node) RegisterLifecycle(lc Lifecycle) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.lifecycles = append(n.lifecycles, lc)
}

// Start starts all registered lifecycles in order.
// If any fails, previously started services are stopped in reverse order.
func (n *Node) Start() error {
	n.lock.Lock()
	defer n.lock.Unlock()

	var started []Lifecycle
	for _, lc := range n.lifecycles {
		if err := lc.Start(); err != nil {
			// Rollback: stop all previously started services
			for i := len(started) - 1; i >= 0; i-- {
				started[i].Stop()
			}
			return err
		}
		started = append(started, lc)
	}
	n.running = true
	return nil
}

// Stop stops all lifecycles in reverse order.
func (n *Node) Stop() {
	n.lock.Lock()
	defer n.lock.Unlock()

	for i := len(n.lifecycles) - 1; i >= 0; i-- {
		n.lifecycles[i].Stop()
	}
	n.running = false
	close(n.stop)
}

// Wait blocks until the node is stopped.
func (n *Node) Wait() {
	<-n.stop
}
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./node/ -v
```

Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add node/
git commit -m "node: add Node container with Lifecycle management"
```

---

## Task 13: cmd/gtron/ — CLI with Config and Version Commands

**Files:**
- Modify: `cmd/gtron/main.go`
- Create: `cmd/gtron/config.go`

- [ ] **Step 1: Update main.go with proper CLI structure**

Rewrite `cmd/gtron/main.go`:

```go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tronprotocol/go-tron/node"
	"github.com/urfave/cli/v2"
)

var (
	dataDirFlag = &cli.StringFlag{
		Name:  "datadir",
		Usage: "Data directory for the database and keystore",
		Value: defaultDataDir(),
	}
	p2pPortFlag = &cli.IntFlag{
		Name:  "p2p.port",
		Usage: "P2P listening port",
		Value: 18888,
	}
	httpPortFlag = &cli.IntFlag{
		Name:  "http.port",
		Usage: "HTTP API port",
		Value: 8090,
	}
	jsonrpcPortFlag = &cli.IntFlag{
		Name:  "jsonrpc.port",
		Usage: "JSON-RPC port",
		Value: 8545,
	}
	testnetFlag = &cli.BoolFlag{
		Name:  "testnet",
		Usage: "Use Nile testnet",
	}
)

var app = &cli.App{
	Name:    "gtron",
	Usage:   "TRON blockchain node (Go implementation)",
	Version: "0.1.0-dev",
	Flags: []cli.Flag{
		dataDirFlag,
		p2pPortFlag,
		httpPortFlag,
		jsonrpcPortFlag,
		testnetFlag,
	},
	Action: gtron,
	Commands: []*cli.Command{
		{
			Name:   "version",
			Usage:  "Print version information",
			Action: versionCmd,
		},
	},
}

func gtron(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	stack, err := node.New(cfg)
	if err != nil {
		return err
	}

	if err := stack.Start(); err != nil {
		return err
	}
	fmt.Printf("gtron started (datadir=%s, http=%d, p2p=%d)\n",
		cfg.DataDir, cfg.HTTPPort, cfg.P2PPort)

	// Wait for interrupt
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc

	fmt.Println("\nShutting down...")
	stack.Stop()
	return nil
}

func versionCmd(ctx *cli.Context) error {
	fmt.Printf("gtron version %s\n", ctx.App.Version)
	return nil
}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Create config.go**

Create `cmd/gtron/config.go`:

```go
package main

import (
	"os"
	"path/filepath"

	"github.com/tronprotocol/go-tron/node"
	"github.com/urfave/cli/v2"
)

func defaultDataDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".gtron")
}

func makeConfig(ctx *cli.Context) *node.Config {
	return &node.Config{
		DataDir:     ctx.String("datadir"),
		P2PPort:     ctx.Int("p2p.port"),
		HTTPPort:    ctx.Int("http.port"),
		JSONRPCPort: ctx.Int("jsonrpc.port"),
	}
}
```

- [ ] **Step 3: Build and test**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
make gtron
./build/bin/gtron version
./build/bin/gtron --help
```

Expected: version prints `gtron version 0.1.0-dev`, help shows all flags.

- [ ] **Step 4: Commit**

```bash
git add cmd/
git commit -m "cmd/gtron: add CLI with datadir, port flags, and version command"
```

---

## Task 14: internal/tronapi/ — Basic HTTP API

**Files:**
- Create: `internal/tronapi/backend.go`
- Create: `internal/tronapi/api.go`
- Create: `internal/tronapi/server.go`
- Test: `internal/tronapi/api_test.go`

- [ ] **Step 1: Write API tests**

Create `internal/tronapi/api_test.go`:

```go
package tronapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type mockBackend struct{}

func (m *mockBackend) CurrentBlock() *types.Block {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeader_Raw{
				Number:    100,
				Timestamp: 300000,
			},
		},
	}
	return types.NewBlockFromPB(pb)
}

func (m *mockBackend) GetBlockByNumber(num uint64) (*types.Block, error) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeader_Raw{
				Number:    int64(num),
				Timestamp: int64(num) * 3000,
			},
		},
	}
	return types.NewBlockFromPB(pb), nil
}

func (m *mockBackend) GetAccount(addr common.Address) (*types.Account, error) {
	acc := types.NewAccount(addr, corepb.AccountType_Normal)
	acc.SetBalance(5_000_000)
	return acc, nil
}

func TestGetNowBlock(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/wallet/getnowblock", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]interface{}
	json.NewDecoder(w.Body).Decode(&result)
	header := result["block_header"].(map[string]interface{})
	raw := header["raw_data"].(map[string]interface{})
	num := raw["number"].(float64)
	if int(num) != 100 {
		t.Fatalf("expected block 100, got %v", num)
	}
}

func TestGetBlockByNum(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/wallet/getblockbynum", nil)
	q := req.URL.Query()
	q.Add("num", "42")
	req.URL.RawQuery = q.Encode()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}
```

- [ ] **Step 2: Define Backend interface**

Create `internal/tronapi/backend.go`:

```go
package tronapi

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// Backend defines what the API layer depends on.
type Backend interface {
	CurrentBlock() *types.Block
	GetBlockByNumber(number uint64) (*types.Block, error)
	GetAccount(addr common.Address) (*types.Account, error)
}
```

- [ ] **Step 3: Implement API handlers**

Create `internal/tronapi/api.go`:

```go
package tronapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"google.golang.org/protobuf/encoding/protojson"
)

// API provides HTTP handlers for TRON API endpoints.
type API struct {
	backend Backend
}

// NewAPI creates a new API with the given backend.
func NewAPI(backend Backend) *API {
	return &API{backend: backend}
}

// RegisterRoutes registers all HTTP API routes.
func (api *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/wallet/getnowblock", api.getNowBlock)
	mux.HandleFunc("/wallet/getblockbynum", api.getBlockByNum)
	mux.HandleFunc("/wallet/getaccount", api.getAccount)
}

func (api *API) getNowBlock(w http.ResponseWriter, r *http.Request) {
	block := api.backend.CurrentBlock()
	if block == nil {
		http.Error(w, "no current block", http.StatusInternalServerError)
		return
	}
	writeProtoJSON(w, block.Proto())
}

func (api *API) getBlockByNum(w http.ResponseWriter, r *http.Request) {
	numStr := r.URL.Query().Get("num")
	if numStr == "" {
		// Try JSON body
		var body struct {
			Num int64 `json:"num"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			numStr = strconv.FormatInt(body.Num, 10)
		}
	}
	num, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid block number", http.StatusBadRequest)
		return
	}
	block, err := api.backend.GetBlockByNumber(num)
	if err != nil || block == nil {
		http.Error(w, "block not found", http.StatusNotFound)
		return
	}
	writeProtoJSON(w, block.Proto())
}

func (api *API) getAccount(w http.ResponseWriter, r *http.Request) {
	addrHex := r.URL.Query().Get("address")
	if addrHex == "" {
		var body struct {
			Address string `json:"address"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			addrHex = body.Address
		}
	}
	if addrHex == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(addrHex))
	acc, err := api.backend.GetAccount(addr)
	if err != nil || acc == nil {
		http.Error(w, "account not found", http.StatusNotFound)
		return
	}
	writeProtoJSON(w, acc.Proto())
}

func writeProtoJSON(w http.ResponseWriter, msg interface{ ProtoReflect() }) {
	marshaler := protojson.MarshalOptions{UseProtoNames: true}
	data, err := marshaler.Marshal(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
```

- [ ] **Step 4: Implement HTTP server wrapper**

Create `internal/tronapi/server.go`:

```go
package tronapi

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Server wraps the HTTP API server.
type Server struct {
	httpServer *http.Server
	api        *API
}

// NewServer creates a new HTTP API server.
func NewServer(backend Backend, port int) *Server {
	api := NewAPI(backend)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	return &Server{
		httpServer: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
		api: api,
	}
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	go s.httpServer.ListenAndServe()
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go test ./internal/tronapi/ -v
```

Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add internal/
git commit -m "internal/tronapi: add HTTP API with GetNowBlock, GetBlockByNum, GetAccount"
```

---

## Summary: What Phase 1 Delivers After These Tasks

After completing Tasks 1-14, the project has:

| Component | Status |
|-----------|--------|
| Project scaffold | Makefile, go.mod, CLI entry point |
| common/ | Address (21-byte), Hash (SHA-256), byte utils |
| crypto/ | secp256k1 signing, Keccak256, TRON address derivation, Base58Check |
| proto/ | All TRON protobuf definitions + generated Go code |
| core/types/ | Block, Transaction, Account, Witness wrappers |
| trondb/ | Database interfaces + memorydb for testing |
| core/rawdb/ | DB schema, block/account/chain metadata accessors |
| params/ | Protocol constants, ChainConfig, Genesis definitions |
| consensus/dpos/ | Slot calculation, witness scheduling, sorting |
| actuator/ | Interface + Transfer, CreateAccount, WitnessCreate |
| node/ | Node container with Lifecycle management |
| cmd/gtron/ | CLI with flags, version command |
| internal/tronapi/ | HTTP API server with basic endpoints |

## Remaining Phase 1 Work (follow-up plan needed)

These items are deferred to a follow-up plan because they depend on the above tasks being completed and tested:

1. **Pebble DB backend** (`trondb/pebble/`) — real persistent storage
2. **core/state/ StateDB** — in-memory account state with snapshot/revert
3. **core/blockchain.go** — block insertion, validation, chain management
4. **core/resource.go** — bandwidth/energy consumption
5. **Remaining actuators** — FreezeV2, UnfreezeV2, VoteWitness, WithdrawBalance
6. **p2p/** — TCP transport, protobuf message codec, peer management
7. **tron/protocols/** — handshake, block sync, transaction relay
8. **tron/backend.go** — wire everything together
9. **JSON-RPC** — eth_blockNumber, eth_getBlockByNumber, eth_getBalance
10. **Genesis block loading** — mainnet/testnet genesis initialization
