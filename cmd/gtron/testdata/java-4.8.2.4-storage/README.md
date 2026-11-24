# Java 4.8.2.4 storage replay

Captured from a private chain produced by the official GreatVoyage-v4.8.2.4 ARM64 FullNode JAR (Git tag commit `4e8389f159d0782f1ebcd9dab2fd88388c0e6fe9`, JAR SHA-256 `a185d0ddff6e3cebd69ac46b7f6c6fa8051c7d4c2e9fe8c55be0867bd06a21d4`).

- `genesis.json` holds the matching Go genesis allocation, witness and chain settings.
- `blocks.bin` holds the original Java block protobufs for heights 1–57 in ascending order. Each protobuf is prefixed with its unsigned varint byte length. Raw rows were read from the JAR's RocksDB block store using its read-only API, keyed by the block IDs returned by Java Wallet.
- `oracle.json` captures Java Wallet block IDs, seven transaction receipt summaries and the final chain parameter values. The storage checkpoints follow the write of 7 at block 9, activation at block 54, the write of 11 at block 55 (also returned by the Java receipt at block 56), and the final zero read from Java JSON-RPC after block 57.

Run from the repository root with `go test ./cmd/gtron -run '^TestJava4824StorageProposalReplay$' -count=1`. The test runs against an in-memory Go chain with signature and consensus validation.
