# Plan: gtron genesis ↔ java-tron parity + `--genesis <file>`

**Spec:** [2026-05-02-genesis-file-loader-design.md](../specs/2026-05-02-genesis-file-loader-design.md)

## Goal

Make `gtron` produce a genesis block whose SHA-256 over
`BlockHeaderRaw` matches what `java-tron` produces from the same
`genesis.block` config — verified against the live private chain at
`/Users/asuka/Works/Tests/TVM/run/config.conf`. Then ship `--genesis
<file>` and run a real cross-impl sync.

## Slice 1 — `core.GenesisToBlock` parity with java-tron

### 1.1 Merkle tree for genesis tx hashes

In `core/types/merkle.go` (new):

- [ ] `func MerkleRoot(leafHashes []common.Hash) common.Hash` mirroring
      `MerkleTree.createTree` semantics:
  - Empty input → `common.Hash{}` (32 zeros). java-tron sends this in
      block #1 of the private chain (no txs, txTrieRoot = 32×0).
  - Single leaf → returns that leaf hash.
  - Otherwise: pair `(i, i+1)`, parent = `SHA256(left || right)`. If
      odd count, the lone trailing leaf carries up unchanged (no
      doubling). Repeat until 1.
- [ ] Unit test `TestMerkleRoot_JavaTronVectors` with 0/1/2/3/4-leaf
      cases. Vectors taken from `java-tron`'s
      `MerkleTreeTest.testCreateTree` (2-leaf vector verified against
      live data when convenient). Single-leaf and 0-leaf cases covered
      directly.

### 1.2 Genesis transaction builder

In `core/genesis.go`:

- [ ] `func newGenesisTransferTx(toAddr common.Address, amount int64) *types.Transaction`:
  - `TransferContract { amount, owner_address: []byte("0x000000000000000000000"),
    to_address: toAddr[:] }` — note `owner_address` is the **ASCII
    bytes of the 21-char string `"0x000000000000000000000"`**, not a
    zero address. This is required for byte-for-byte parity.
  - Wrap in `Transaction.RawData.Contract = [{ type: TransferContract,
    parameter: Any{type_url, value: marshal(TransferContract)} }]`.
  - No signature. No `ret`. Default `ref_block_*` = empty.
- [ ] Re-check the `Any.type_url` java-tron uses. Java-tron's
      `TransactionCapsule(message, ContractType)` constructor calls
      `Any.pack(message)` which uses
      `"type.googleapis.com/" + message.getDescriptorForType().getFullName()`.
      Confirm that gtron's `anypb.New(transferContract)` produces the
      same string (`"type.googleapis.com/protocol.TransferContract"`).
      Add a tiny test asserting this.

### 1.3 `GenesisToBlock` rewrite

In `core/genesis.go`:

- [ ] Build `txs := []*types.Transaction` from
      `g.Accounts` — one tx per account, in slice order, **including
      negative-balance entries** (Blackhole). This is required because
      java-tron's `assets[]` ordering matters for the Merkle root.
      Decision: only emit a genesis tx when `accountType == AssetIssue`
      *or* when the entry is in a new explicit `Genesis.GenesisTxs` list.
      Alternative (simpler): emit one per `Accounts` entry where
      `Balance != 0`, in declaration order. **Going with the simpler
      rule** — matches java-tron's `assets[]` semantics for our config.
      Mainnet/Nile/dev `params.*Genesis()` will need `Accounts` re-checked
      so order matches the historical config.
- [ ] Compute `txTrieRoot = MerkleRoot([SHA256(tx.proto bytes) for tx in txs])`.
- [ ] Build `BlockHeaderRaw{ Number: 0, Timestamp, ParentHash,
      TxTrieRoot, WitnessAddress: famousQuoteBytes }`. Do NOT set
      `AccountStateRoot`. Do NOT set `WitnessSignature`. Do NOT set
      `Version` (java-tron's genesis has version 0).
- [ ] Build `Block { BlockHeader, Transactions: txs }`.
- [ ] StateDB write: still create the in-memory accounts and call
      `statedb.Commit()` so account state exists before block #1 runs,
      but discard the returned root (do not put it on the header).

In `core/genesis.go`:

- [ ] Define `var GenesisWitnessAddressBytes = []byte("A new system must
      allow existing systems to be linked together without requiring
      any central control or coordination")` (98 bytes).

### 1.4 Update `core.SetupGenesisBlock` & DP write

- [ ] DP write happens after the (now header-less-account-root) block
      is built. No structural change required: `dp.Set` and `Flush`
      still run.
- [ ] Verify writes of `genesis.Witnesses` to the witness store are
      unaffected. (They are — they don't touch the block header.)

### 1.5 Tests

In `core/genesis_test.go`:

- [ ] Replace `TestGenesisToBlock`'s `block.AccountStateRoot() ==
      (common.Hash{})` check (was: should not be zero → must now be
      zero). Adjust to match the new contract.
- [ ] New test `TestGenesisToBlock_MatchesJavaTronPrivateChain`:
  - Use the exact `params.Genesis` corresponding to
    `/Users/asuka/Works/Tests/TVM/run/config.conf`'s `genesis.block`.
  - Compute the genesis block hash via `GenesisToBlock`.
  - Assert it equals the bytes recorded by the
    `TestProbeJavaTronGenesis` log
    (`75da3fe749503edb5d6121d96d450b980294a03648934988`, with first 8
    bytes set to zero by `BlockID()`). The pinned value goes into the
    test as a constant once the implementation is green.
  - Also assert `block.AccountStateRoot()` is zero, `len(block.Transactions())==2`,
    `block.WitnessAddress()` is the famous-quote bytes.
- [ ] New test `TestGenesisToBlock_MainnetHash`:
  - Pin against java-tron mainnet's genesis hash. The standard
    java-tron mainnet genesis ID is publicly published; confirm the
    value via `git log -p` on a java-tron release tag (or by replaying
    `params.MainnetGenesis()` once the implementation is green and
    cross-checking on TRONScan). Pinned value goes in the test once
    confirmed.

### 1.6 Repair `params.MainnetGenesis()` / `NileGenesis()`

- [ ] Verify `Accounts` ordering and contents reflect the on-chain
      mainnet/Nile configs. If our list differs, gtron's mainnet
      genesis hash will diverge — adjust to match.
- [ ] If mainnet's actual genesis hash is unknown today, treat the
      mainnet test as a TODO inside the test (with a `t.Skip` and a
      `// TODO: pin once verified against TRONScan`). Don't block
      slice 1 on mainnet hash audit.

### 1.7 Compile + `make test`

- [ ] All packages compile.
- [ ] `make test` is green.

## Slice 2 — `--genesis <file>` JSON loader

(Was the original "slice 1" before the parity finding; now slice 2
because slice 1 unblocks it.)

### 2.1 JSON loader

In `cmd/gtron/genesis_file.go`:

- [ ] `genesisFile` struct:
      `{ chain_id, p2p_version, timestamp_ms, parent_hash, accounts:[{
      address, balance, name, account_type? }], witnesses:[{ address,
      vote_count, url }], dynamic_properties: {string→int64} }`.
- [ ] `loadGenesisFile(path) (*params.Genesis, error)`:
  - JSON decode.
  - Resolve addresses: `41…` hex → `tcommon.HexToAddress`,
    `T…` Base58 → `crypto.Base58ToAddress`.
  - Parse balances as `int64`, allowing negatives (Blackhole).
  - Build `params.ChainConfig{ChainID, P2PVersion}`.
  - Empty `dynamic_properties` → use a `defaultDynamicProperties()`
    helper (curated, same flags as `makeDevGenesis(fullFeatures=true)`).

### 2.2 CLI wiring

In `cmd/gtron/main.go`:

- [ ] Add `--genesis <path>` string flag on root + on `init` subcommand.
- [ ] Reject `--genesis` with `--testnet` or `--dev`.

In `cmd/gtron/config.go::makeGenesis`:

- [ ] If `--genesis` set: return `loadGenesisFile(path)`. Else existing
      mainnet/testnet branches.

In `cmd/gtron/main.go::gtron`:

- [ ] When `--genesis` is set, override
      `cfg.NetworkID = int32(genesis.Config.P2PVersion)` so libp2p
      networkId matches the peer.

### 2.3 Test fixture

- [ ] `test/fixtures/cross-impl/java-tron-private.json` matching the
      `genesis.block` from `/Users/asuka/Works/Tests/TVM/run/config.conf`.
- [ ] Unit test `TestLoadGenesisFile_JavaTronPrivate` parses it and
      derives the same genesis hash as the slice-1 parity test.

## Slice 3 — cross-impl sync smoke

Manual (no committed automation):

- [ ] `gtron init --genesis test/fixtures/cross-impl/java-tron-private.json
      --datadir /tmp/gtron-cross`.
- [ ] `gtron --datadir /tmp/gtron-cross
      --genesis test/fixtures/cross-impl/java-tron-private.json
      --p2p.port 19999 --http.port 8190 --jsonrpc.port 8546
      --grpc.port 0 --seednode 127.0.0.1:18888`.
- [ ] Curl gtron's `eth_blockNumber` at 8546 every 5 s. Expect monotonic
      increase past 0 within 60 s.
- [ ] Compare gtron block #1 hash to java-tron's via the existing probe;
      ensure they match.
- [ ] Record outcome (success or partial) in
      `docs/dev/p2p-interop-status.md`.

Slice 3 is **not** automated yet — `scripts/system_test_cross.sh` is
follow-up work. Document the manual procedure in
`docs/dev/java-tron-local.md`.

## Cleanup / commit hygiene

- [ ] Probe test `p2p/java_genesis_probe_test.go` from the diagnostic
      session: keep. It's a useful one-shot for future genesis audits.
- [ ] Subject for slice 1: `feat(core,types): genesis block parity
      with java-tron (txs + merkleRoot)`.
- [ ] Subject for slice 2: `feat(cmd): --genesis <file> for
      custom-chain bootstrap`.
- [ ] All commits GPG-signed (`E3673E008F6D506E`).

## Out of scope

- HOCON parser (`gtron genesis from-hocon`).
- `scripts/system_test_cross.sh` automation.
- Mainnet genesis-hash audit pinning (test skipped until cross-checked).
