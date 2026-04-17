# M0″ 一致性回放 Harness — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建 go-tron 状态层与 java-tron 的可复现回放对照管线，作为 G1 准入门。

**Architecture:** 三条独立流水线共用一套 fixture 格式。`core/conformance` 为纯库（seed/digest/allowlist/replay），`cmd/gtron-replay` 薄包装，`scripts/fixtures/capture_range.sh` 从本地 java-tron 抓取 corpus，`scripts/conformance_replay.sh` 跑 CI-hermetic 回放，`scripts/system_test_cross.sh` 跑 1 gtron + 1 java-tron 双节点 e2e。

**Tech Stack:** Go 1.25、protobuf `corepb.Block/BlockMessage`、`core/state.StateDB`、Pebble/in-mem ethdb、bash、git-lfs、java-tron gRPC。

**日期：** 2026-04-17
**对应 spec：** [`../specs/2026-04-17-m0-double-prime-conformance-replay-design.md`](../specs/2026-04-17-m0-double-prime-conformance-replay-design.md)
**依赖：** M0′、M1.1、M1.3、M1.4、M1.5、M1.7、M1.8 均已落地。

---

## 执行原则

本里程碑自然分两阶段：

- **Phase 1 — 在本仓库里可独立交付** (PR-1, PR-2, PR-3)：引擎 + CLI + smoke corpus + 可测试的 Go helper + 捕获协议文档。不需要 java-tron。
- **Phase 2 — 需要本地 java-tron 协作** (PR-4, PR-5)：录入真实 mainnet 三段 corpus、补 allowlist、跑 cross e2e。必须由有 java-tron 访问权限的操作员执行；本 plan 只给规范和工具，不做 speculative scripting。

原则：

1. 每 Task 独立 commit：`feat(conformance): M0'' PR-N Task M — <短描述>`。
2. PR-1 的 engine 必须 100% 单测覆盖（纯库、无外部依赖），为后续 PR 打下信任基础。
3. 关于 allowlist：Phase 1 产物的 allowlist 始终为 `[]`；Phase 2 录入真实 corpus 后才有条目，并在 PR-4 之后逐步清空。
4. Corpus 文件（`blocks.bin` 等）一律走 git-lfs（只针对 `range-*/`，smoke 小到可不走 lfs）；仓库根目录 `.gitattributes` 追加 `test/fixtures/mainnet-blocks/range-*/blocks.bin filter=lfs diff=lfs merge=lfs -text`。
5. **不写无法在当前环境验证的 bash**：capture 流程本身是 bash + java-tron 的编排，除非能在 CI 里真的跑通，否则只写规范（operator 指南）不写脚本骨架。bash 由后续有 java-tron 访问的操作员补。

---

## 文件结构（本里程碑新增/修改）

```
core/conformance/
  seed.go                       # LoadSeed → StateDB+DP
  seed_test.go
  digest.go                     # DigestB / DigestC
  digest_test.go
  allowlist.go                  # LoadAllowlist + lookup
  allowlist_test.go
  replay.go                     # ReplayRange
  replay_test.go
  report.go                     # Report + String()
  report_test.go
  fixture_format.go             # schema 常量 + json 结构体
cmd/gtron-replay/
  main.go
scripts/conformance_replay.sh   # 顶层 wrapper
scripts/fixtures/
  capture_range.sh              # 新增 entry
  lib/capture.sh                # 辅助
  scenarios/mainnet-range-freeze-v2/   # PR-3 smoke，PR-4 正式
  scenarios/mainnet-range-maintenance/
  scenarios/mainnet-range-contract/
scripts/system_test_cross.sh
test/fixtures/mainnet-blocks/
  smoke/                        # PR-2 落盘；5 块合成 range
    fixture.json
    blocks.bin
    seed.json
    oracle.ndjson
    divergence-allowlist.json
  range-freeze-v2/              # PR-4
  range-maintenance/            # PR-4
  range-contract/               # PR-4
Makefile                        # 追加 conformance-replay / system-test-cross
.gitattributes                  # lfs pattern
docs/dev/conformance-harness.md
```

---

# PR-1 · `core/conformance` 纯库 engine + 单测

**目标：** 不依赖真实 corpus，把 seed/digest/allowlist/replay 四件套搭起来，全部用合成场景单测。

## Task 1 · Fixture 数据结构与常量

**Files:**
- Create: `core/conformance/fixture_format.go`
- Create: `core/conformance/fixture_format_test.go`

- [ ] 写 `fixture_format.go`：定义 schema 常量与 JSON 结构体。

```go
package conformance

const SchemaVersion = 1

// Seed is the on-disk layout of seed.json.
type Seed struct {
    Schema           int               `json:"schema"`
    JavaTronVersion  string            `json:"javaTronVersion"`
    StartHeight      uint64            `json:"startHeight"`
    DynamicProps     map[string]int64  `json:"dynamicProperties"`
    DynamicPropsHex  map[string]string `json:"dynamicPropertiesBytes,omitempty"`
    Accounts         []SeedAccount     `json:"accounts"`
    Contracts        []SeedContract    `json:"contracts"`
    ClosureAddresses []string          `json:"closureAddresses"` // 41-prefixed hex, fixed for the range
}

type SeedAccount struct {
    Address     string            `json:"address"`
    Balance     int64             `json:"balance"`
    AccountType int32             `json:"accountType"`
    FrozenV1Net int64             `json:"frozenV1Net,omitempty"`
    // extend as we discover fields used by the 3 ranges; add fields incrementally
    Raw         json.RawMessage   `json:"raw,omitempty"` // escape hatch: full proto-JSON of Account
}

type SeedContract struct {
    Address string `json:"address"`
    CodeHex string `json:"code"`
    // ABI, energy factor, etc., grow as needed
    Raw     json.RawMessage `json:"raw,omitempty"`
}

// OracleEntry is one line in oracle.ndjson.
type OracleEntry struct {
    BlockNum uint64          `json:"blockNum"`
    DigestB  string          `json:"digestB"`            // hex(32)
    DiagC    json.RawMessage `json:"diagC,omitempty"`
}

// AllowlistEntry is one element in divergence-allowlist.json.
type AllowlistEntry struct {
    BlockNum       uint64 `json:"blockNum"`
    Field          string `json:"field"`          // dotted path, e.g. "account:41abc..:balance"
    Reason         string `json:"reason"`
    TrackingIssue  string `json:"trackingIssue"`
    ExpiresIsoDate string `json:"expires,omitempty"`
}

// FixtureMeta is fixture.json.
type FixtureMeta struct {
    Schema          int      `json:"schema"`
    Scenario        string   `json:"scenario"`
    JavaTronVersion string   `json:"javaTron.version"`
    JarSha256       string   `json:"javaTron.jarSha256"`
    CapturedAt      string   `json:"capturedAt"`
    StartBlock      uint64   `json:"startBlock"`
    EndBlock        uint64   `json:"endBlock"`
    GenesisTime     int64    `json:"genesisTime"`     // ms; passed to ProcessBlock
    ActiveWitnesses []string `json:"activeWitnesses"` // 41-hex; at StartBlock-1
}
```

- [ ] 写 `fixture_format_test.go`：JSON roundtrip 测试（marshal → unmarshal → deep-equal）。

```go
func TestSeedRoundTrip(t *testing.T) {
    orig := Seed{
        Schema: SchemaVersion, StartHeight: 100,
        DynamicProps: map[string]int64{"getEnergyFee": 100},
        Accounts: []SeedAccount{{Address: "41aaa", Balance: 1000}},
        ClosureAddresses: []string{"41aaa", "41bbb"},
    }
    data, err := json.Marshal(orig)
    if err != nil { t.Fatal(err) }
    var back Seed
    if err := json.Unmarshal(data, &back); err != nil { t.Fatal(err) }
    if !reflect.DeepEqual(orig, back) {
        t.Fatalf("roundtrip: got %+v want %+v", back, orig)
    }
}
```

- [ ] Run `go test ./core/conformance/ -run TestSeedRoundTrip -v` → PASS.
- [ ] Commit.

**验收：** 结构体稳定、roundtrip 无损。

---

## Task 2 · Seed loader

**Files:**
- Create: `core/conformance/seed.go`
- Create: `core/conformance/seed_test.go`

- [ ] 写 `seed.go`：`LoadSeed(path string) (*Loaded, error)`，返回一个打包好的 handle 以避免 4-value tuple。

```go
// Loaded bundles the artifacts produced by LoadSeed. DiskDB is exposed
// because ProcessBlock / rawdb accessors need it for per-contract state.
type Loaded struct {
    StateDB  *state.StateDB
    DynProps *state.DynamicProperties
    Closure  []tcommon.Address
    DiskDB   ethdb.KeyValueStore
}

func LoadSeed(path string) (*Loaded, error) {
    raw, err := os.ReadFile(path)
    if err != nil { return nil, fmt.Errorf("read seed: %w", err) }
    var seed Seed
    if err := json.Unmarshal(raw, &seed); err != nil {
        return nil, fmt.Errorf("parse seed: %w", err)
    }
    if seed.Schema != SchemaVersion {
        return nil, fmt.Errorf("seed schema %d != %d", seed.Schema, SchemaVersion)
    }

    diskdb := ethrawdb.NewMemoryDatabase()
    sdb := state.NewDatabase(diskdb)
    statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), sdb)
    if err != nil { return nil, fmt.Errorf("new statedb: %w", err) }

    dp := state.NewDynamicProperties()
    for k, v := range seed.DynamicProps {
        dp.Set(k, v)
    }
    statedb.SetDynamicProperties(dp)

    for _, a := range seed.Accounts {
        addr, err := parseAddr(a.Address)
        if err != nil { return nil, err }
        if err := applySeedAccount(statedb, addr, a); err != nil {
            return nil, fmt.Errorf("apply %s: %w", a.Address, err)
        }
    }
    for _, c := range seed.Contracts {
        addr, err := parseAddr(c.Address)
        if err != nil { return nil, err }
        if err := applySeedContract(statedb, diskdb, addr, c); err != nil {
            return nil, fmt.Errorf("apply contract %s: %w", c.Address, err)
        }
    }

    closure := make([]tcommon.Address, 0, len(seed.ClosureAddresses))
    for _, s := range seed.ClosureAddresses {
        a, err := parseAddr(s)
        if err != nil { return nil, err }
        closure = append(closure, a)
    }
    return &Loaded{StateDB: statedb, DynProps: dp, Closure: closure, DiskDB: diskdb}, nil
}

func parseAddr(hex string) (tcommon.Address, error) { /* 41-prefixed hex → Address */ }
func applySeedAccount(s *state.StateDB, addr tcommon.Address, a SeedAccount) error {
    s.CreateAccount(addr)
    s.SetBalance(addr, a.Balance)
    // frozen V1, assets, etc.; for fields not yet supported, error out so we
    // discover gaps at load time (not silently drop data)
    if a.Raw != nil {
        return unmarshalAccountFromProtoJSON(s, addr, a.Raw)
    }
    return nil
}
```

- [ ] 写 `seed_test.go`：用手写 mini `seed.json`（2 个账户 + 5 个 DP key）测 LoadSeed 后 StateDB/DP 与写入一致。

```go
func TestLoadSeed_MinimalRoundTrip(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "seed.json")
    writeJSON(t, path, Seed{
        Schema: SchemaVersion, StartHeight: 1000,
        DynamicProps: map[string]int64{"getEnergyFee": 420},
        Accounts: []SeedAccount{{Address: "41" + strings.Repeat("a", 40), Balance: 9999}},
        ClosureAddresses: []string{"41" + strings.Repeat("a", 40)},
    })
    loaded, err := LoadSeed(path)
    if err != nil { t.Fatal(err) }
    if loaded.DynProps.EnergyFee() != 420 { t.Fatal("dp") }
    addr := loaded.Closure[0]
    if loaded.StateDB.GetBalance(addr) != 9999 { t.Fatal("balance") }
}

func TestLoadSeed_BadSchema(t *testing.T) { /* schema=2 → error */ }
func TestLoadSeed_BadAddress(t *testing.T) { /* non-hex address → error */ }
```

- [ ] Run `go test ./core/conformance/ -run TestLoadSeed -v` → PASS.
- [ ] Commit.

**验收：** 合法 seed 加载出可用 StateDB；错 schema/错地址被拒绝。

---

## Task 3 · Digest（B + C）

**Files:**
- Create: `core/conformance/digest.go`
- Create: `core/conformance/digest_test.go`

- [ ] 写 `digest.go`：

```go
// DigestB hashes account+DP state for a fixed address set.
// Canonical: sort(addrs) || for each addr { account-proto-bytes (may be empty)
// || contract-proto-bytes (may be empty) || contract-state-proto-bytes (may be empty) }
// || sort(dp-keys) || for each key { varint(len) || key-utf8 || int64-be(value) }
func DigestB(sdb *state.StateDB, addrs []tcommon.Address, dp *state.DynamicProperties) [32]byte {
    addrsCopy := append([]tcommon.Address(nil), addrs...)
    sort.Slice(addrsCopy, func(i, j int) bool {
        return bytes.Compare(addrsCopy[i][:], addrsCopy[j][:]) < 0
    })
    h := sha256.New()
    for _, a := range addrsCopy {
        h.Write(a[:])
        writeProtoOrEmpty(h, sdb.GetAccount(a))          // nil → single 0x00 marker
        writeProtoOrEmpty(h, sdb.GetCode(a))
        writeProtoOrEmpty(h, sdb.GetContractState(a))    // nil OK
    }
    // DP: enumerate all known keys (defaults + any set)
    keys := dp.AllKeys()
    sort.Strings(keys)
    for _, k := range keys {
        v, _ := dp.Get(k)
        writeVarint(h, uint64(len(k))); h.Write([]byte(k))
        writeInt64BE(h, v)
    }
    var out [32]byte
    copy(out[:], h.Sum(nil))
    return out
}

// DigestC emits the same data as DigestB but as structured JSON for diffing.
func DigestC(sdb *state.StateDB, addrs []tcommon.Address, dp *state.DynamicProperties) json.RawMessage {
    // Map-of-maps keyed by address hex, plus "dp" key. Stable key order.
    ...
}
```

- [ ] 注意：`state.DynamicProperties.AllKeys()` 如不存在则加；访问 `ContractState` 复用 M1.7 的 `rawdb.ReadContractState`。

- [ ] 写 `digest_test.go`：

```go
func TestDigestB_Deterministic(t *testing.T) {
    // Same state, same addrs → same digest
    sdb, dp, addrs := newSeededState(t)
    d1 := DigestB(sdb, addrs, dp)
    d2 := DigestB(sdb, addrs, dp)
    if d1 != d2 { t.Fatal("digest not deterministic") }
}

func TestDigestB_AddrOrderInvariant(t *testing.T) {
    sdb, dp, addrs := newSeededState(t)
    rev := reverse(addrs)
    if DigestB(sdb, addrs, dp) != DigestB(sdb, rev, dp) {
        t.Fatal("digest must be order-invariant")
    }
}

func TestDigestB_DetectsBalanceChange(t *testing.T) {
    sdb, dp, addrs := newSeededState(t)
    d0 := DigestB(sdb, addrs, dp)
    sdb.AddBalance(addrs[0], 1)
    d1 := DigestB(sdb, addrs, dp)
    if d0 == d1 { t.Fatal("digest must change when balance changes") }
}

func TestDigestB_DetectsDPChange(t *testing.T) { /* change one DP key */ }
func TestDigestC_IsValidJSON(t *testing.T)     { /* json.Unmarshal round-trip */ }
```

- [ ] Run `go test ./core/conformance/ -run TestDigest -v` → all PASS.
- [ ] Commit.

**验收：** 确定性、顺序不变、对 balance / DP 敏感；C 是合法 JSON。

---

## Task 4 · Allowlist

**Files:**
- Create: `core/conformance/allowlist.go`
- Create: `core/conformance/allowlist_test.go`

- [ ] 写 `allowlist.go`：

```go
type Allowlist struct {
    entries map[uint64]map[string]AllowlistEntry // blockNum → field → entry
    hits    map[uint64]map[string]bool           // seen fields, for stale detection
}

func LoadAllowlist(path string) (*Allowlist, error) {
    raw, err := os.ReadFile(path)
    if err != nil && !os.IsNotExist(err) { return nil, err }
    al := &Allowlist{
        entries: make(map[uint64]map[string]AllowlistEntry),
        hits:    make(map[uint64]map[string]bool),
    }
    if len(raw) == 0 { return al, nil }
    var list []AllowlistEntry
    if err := json.Unmarshal(raw, &list); err != nil { return nil, err }
    for _, e := range list {
        if al.entries[e.BlockNum] == nil {
            al.entries[e.BlockNum] = map[string]AllowlistEntry{}
            al.hits[e.BlockNum] = map[string]bool{}
        }
        al.entries[e.BlockNum][e.Field] = e
    }
    return al, nil
}

func (a *Allowlist) IsWhitelisted(blockNum uint64, field string) bool {
    if fields := a.entries[blockNum]; fields != nil {
        if _, ok := fields[field]; ok {
            a.hits[blockNum][field] = true
            return true
        }
    }
    return false
}

func (a *Allowlist) Empty() bool { return len(a.entries) == 0 }

func (a *Allowlist) Stale() []AllowlistEntry {
    var out []AllowlistEntry
    for blk, fields := range a.entries {
        for f, e := range fields {
            if !a.hits[blk][f] { out = append(out, e) }
        }
    }
    return out
}
```

- [ ] 写 `allowlist_test.go`：空 allowlist、命中、未命中检测为 stale。

- [ ] Run `go test ./core/conformance/ -run TestAllowlist -v` → PASS.
- [ ] Commit.

**验收：** 语义准确；Empty()、Stale() 两条关键断言可直接被 replay 使用。

---

## Task 5 · Report

**Files:**
- Create: `core/conformance/report.go`
- Create: `core/conformance/report_test.go`

- [ ] `report.go`：

```go
type BlockResult struct {
    BlockNum   uint64
    Passed     bool
    Divergence *Divergence // nil if passed
}

type Divergence struct {
    BlockNum   uint64
    FieldDiffs []FieldDiff
    GotJSON    json.RawMessage
    WantJSON   json.RawMessage
}

type FieldDiff struct{ Field, Got, Want string }

type Report struct {
    RangeName              string
    Passed                 bool
    BlockResults           []BlockResult
    FirstFailure           *Divergence
    AllowlistHits          int
    StaleAllowlistEntries  []AllowlistEntry
}

func (r *Report) String() string { /* human table */ }
```

- [ ] 写 `report_test.go`：String() 渲染不 panic；JSON 可序列化；`FirstFailure` 正确指向第一个 failure。

- [ ] Commit.

**验收：** Report 可打印、可 JSON-serialize；指针语义正确。

---

## Task 6 · Replay engine

**Files:**
- Create: `core/conformance/replay.go`
- Create: `core/conformance/replay_test.go`

- [ ] `replay.go`：

```go
type ReplayConfig struct {
    RangeName     string
    SeedPath      string
    BlocksPath    string // blocks.bin
    OraclePath    string // oracle.ndjson
    AllowlistPath string
    GenesisTime   int64
    // ActiveWitnesses at the starting height — captured into seed.json's meta
    ActiveWitnesses []tcommon.Address
}

func ReplayRange(ctx context.Context, cfg ReplayConfig) (*Report, error) {
    loaded, err := LoadSeed(cfg.SeedPath)
    if err != nil { return nil, err }
    sdb, dp, closure, db := loaded.StateDB, loaded.DynProps, loaded.Closure, loaded.DiskDB

    blocks, err := openBlocksReader(cfg.BlocksPath)
    if err != nil { return nil, err }
    defer blocks.Close()

    oracle, err := openOracleReader(cfg.OraclePath)
    if err != nil { return nil, err }
    defer oracle.Close()

    allowlist, err := LoadAllowlist(cfg.AllowlistPath)
    if err != nil { return nil, err }

    rep := &Report{RangeName: cfg.RangeName, Passed: true}

    for {
        blk, err := blocks.Next()
        if err == io.EOF { break }
        if err != nil { return nil, fmt.Errorf("read block: %w", err) }

        ent, err := oracle.Next()
        if err != nil { return nil, fmt.Errorf("read oracle: %w", err) }
        if ent.BlockNum != blk.Number() {
            return nil, fmt.Errorf("oracle/block height mismatch: %d vs %d", ent.BlockNum, blk.Number())
        }

        _, procErr := core.ProcessBlock(sdb, dp, blk, db, cfg.ActiveWitnesses, cfg.GenesisTime)
        if procErr != nil {
            rep.Passed = false
            rep.FirstFailure = &Divergence{
                BlockNum:   blk.Number(),
                FieldDiffs: []FieldDiff{{Field: "_panic", Got: procErr.Error(), Want: "success"}},
            }
            rep.BlockResults = append(rep.BlockResults, BlockResult{BlockNum: blk.Number(), Passed: false, Divergence: rep.FirstFailure})
            return rep, nil
        }

        gotB := DigestB(sdb, closure, dp)
        wantB, err := hex.DecodeString(ent.DigestB)
        if err != nil { return nil, err }

        if bytes.Equal(gotB[:], wantB) {
            rep.BlockResults = append(rep.BlockResults, BlockResult{BlockNum: blk.Number(), Passed: true})
            continue
        }

        // mismatch → C-digest + field-level diff
        gotC := DigestC(sdb, closure, dp)
        diffs := diffJSON(gotC, ent.DiagC)
        unhandled := diffs[:0]
        for _, d := range diffs {
            if !allowlist.IsWhitelisted(blk.Number(), d.Field) {
                unhandled = append(unhandled, d)
            }
        }

        if len(unhandled) == 0 {
            rep.AllowlistHits += len(diffs)
            rep.BlockResults = append(rep.BlockResults, BlockResult{BlockNum: blk.Number(), Passed: true})
            continue
        }

        div := &Divergence{BlockNum: blk.Number(), FieldDiffs: unhandled, GotJSON: gotC, WantJSON: ent.DiagC}
        rep.Passed = false
        rep.FirstFailure = div
        rep.BlockResults = append(rep.BlockResults, BlockResult{BlockNum: blk.Number(), Passed: false, Divergence: div})
        return rep, nil
    }

    rep.StaleAllowlistEntries = allowlist.Stale()
    return rep, nil
}
```

- [ ] 新增两个 reader 帮手：`openBlocksReader` 读 `blocks.bin`（varint-prefix framing，每块一个 `corepb.Block`），`openOracleReader` 逐行 JSON。

```go
type blockReader struct { f *os.File; buf *bufio.Reader }
func openBlocksReader(path string) (*blockReader, error) { /* … */ }
func (r *blockReader) Next() (*types.Block, error) {
    n, err := binary.ReadUvarint(r.buf)
    if err == io.EOF { return nil, io.EOF }
    if err != nil { return nil, err }
    buf := make([]byte, n)
    if _, err := io.ReadFull(r.buf, buf); err != nil { return nil, err }
    return types.UnmarshalBlock(buf)
}
```

- [ ] `diffJSON`：把 `DigestC` 出来的 map-of-maps 两边对齐，输出 `[]FieldDiff` — key path 用 `account:<hex>:<field>` 或 `dp:<key>` 形态。

- [ ] `replay_test.go`：用 `producer` 在内存里跑 5 个区块，各块之间 digest 自己写进合成 `oracle.ndjson`，然后 `ReplayRange` 重新跑一遍期望全 pass。再加一条人为 mutate 合成 oracle 触发 divergence 的 case。

```go
func TestReplayRange_SyntheticPass(t *testing.T) { /* pass */ }
func TestReplayRange_SyntheticDivergence(t *testing.T) { /* mismatch → FirstFailure set */ }
func TestReplayRange_AllowlistCovers(t *testing.T) { /* same mismatch whitelisted → pass */ }
func TestReplayRange_StaleAllowlist(t *testing.T) { /* whitelisted but never hit */ }
```

- [ ] Run `go test ./core/conformance/ -v -count=1` → all PASS.
- [ ] Commit.

**验收：** 合成 5 块 range 在 engine 里跑通，三条状态（pass/fail/allowlisted）路径全覆盖。

---

## Task 7 · PR-1 收尾

- [ ] `go vet ./core/conformance/` 干净。
- [ ] `go test ./... -count=1 -timeout 300s` 全绿。
- [ ] 在 `core/conformance/doc.go` 加一个包 doc comment，指向 spec。
- [ ] 最后一个 commit：`docs(conformance): M0'' PR-1 wrap-up — package doc`。

---

# PR-2 · CLI + smoke corpus

**目标：** 可执行的 replay 工具 + 第一个端到端跑通的合成 corpus。

## Task 8 · `cmd/gtron-replay`

**Files:**
- Create: `cmd/gtron-replay/main.go`

- [ ] `main.go` 解析 flags：`--range=<dir>`、`--exit-gate`（要求 allowlist 空且无 stale）、`--mode=fast|full`（默认 fast）、`--verbose`。

```go
func main() {
    rangeDir := flag.String("range", "", "path to test/fixtures/mainnet-blocks/<name>")
    exitGate := flag.Bool("exit-gate", false, "fail if allowlist non-empty or stale")
    flag.Parse()
    if *rangeDir == "" { log.Fatal("--range required") }

    cfg := conformance.ReplayConfig{
        RangeName:     filepath.Base(*rangeDir),
        SeedPath:      filepath.Join(*rangeDir, "seed.json"),
        BlocksPath:    filepath.Join(*rangeDir, "blocks.bin"),
        OraclePath:    filepath.Join(*rangeDir, "oracle.ndjson"),
        AllowlistPath: filepath.Join(*rangeDir, "divergence-allowlist.json"),
    }
    // Load fixture.json for capturedAt / jarSha / genesisTime / witnesses
    meta, err := conformance.LoadFixtureMeta(filepath.Join(*rangeDir, "fixture.json"))
    if err != nil { log.Fatalf("load fixture.json: %v", err) }
    cfg.GenesisTime = meta.GenesisTime
    witnesses, err := conformance.ParseAddresses(meta.ActiveWitnesses)
    if err != nil { log.Fatalf("parse witnesses: %v", err) }
    cfg.ActiveWitnesses = witnesses

    rep, err := conformance.ReplayRange(context.Background(), cfg)
    if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(3) }
    fmt.Println(rep.String())
    switch {
    case rep.FirstFailure != nil: os.Exit(2)
    case *exitGate && !isExitClean(rep): os.Exit(1)
    default: os.Exit(0)
    }
}

func isExitClean(r *conformance.Report) bool {
    return r.Passed && r.AllowlistHits == 0 && len(r.StaleAllowlistEntries) == 0
}
```

- [ ] 确认 Task 1 已把 `GenesisTime` / `ActiveWitnesses` 加进 `FixtureMeta`；`conformance.LoadFixtureMeta` 和 `conformance.ParseAddresses` 是该包公开的小 helpers（`ParseAddresses` 的实现与 seed.go 里的 `parseAddr` 共用同一 41-prefixed-hex 解析器）。
- [ ] 手动冒烟：先不用 corpus，传一个空目录，确认错误码 3 + 清晰报错。

- [ ] Commit.

**验收：** 命令行跑通；错误码分流符合 spec §5.4。

---

## Task 9 · `scripts/conformance_replay.sh`

**Files:**
- Create: `scripts/conformance_replay.sh`

- [ ] 脚本内容：

```bash
#!/usr/bin/env bash
set -euo pipefail
BASEDIR="$(cd "$(dirname "$0")/.." && pwd)"
GTRON_REPLAY="$BASEDIR/build/bin/gtron-replay"
EXIT_GATE=${EXIT_GATE:-0}
RANGES=${RANGES:-"smoke"}  # space-separated range names

[ -f "$GTRON_REPLAY" ] || (cd "$BASEDIR" && go build -o build/bin/gtron-replay ./cmd/gtron-replay)

fail=0
for r in $RANGES; do
    dir="$BASEDIR/test/fixtures/mainnet-blocks/$r"
    [ -d "$dir" ] || { echo "missing: $dir"; fail=1; continue; }
    echo "=== $r ==="
    extra=()
    [ "$EXIT_GATE" = "1" ] && extra+=(--exit-gate)
    if "$GTRON_REPLAY" --range="$dir" "${extra[@]}"; then
        echo "  PASS"
    else
        echo "  FAIL ($?)"
        fail=1
    fi
done
exit $fail
```

- [ ] Makefile 追加：

```makefile
.PHONY: conformance-replay conformance-replay-exit-gate
conformance-replay: gtron-replay
	RANGES="smoke range-freeze-v2 range-maintenance range-contract" scripts/conformance_replay.sh || true

conformance-replay-exit-gate: gtron-replay
	EXIT_GATE=1 RANGES="smoke range-freeze-v2 range-maintenance range-contract" scripts/conformance_replay.sh

gtron-replay:
	go build -o build/bin/gtron-replay ./cmd/gtron-replay/
```

- [ ] Commit.

**验收：** `make conformance-replay` 可跑（此时还没 smoke corpus，会打印 missing）。

---

## Task 10 · Smoke corpus — 合成 5 块 range

**Files:**
- Create: `test/fixtures/mainnet-blocks/smoke/{fixture.json,seed.json,blocks.bin,oracle.ndjson,divergence-allowlist.json}`
- Modify: `.gitattributes`

- [ ] 写一个一次性的 Go 程序（或测试函数 `TestGenerateSmokeCorpus`，t.Skip 守门）用 `producer` 打 5 个合成块：

```go
//go:build smoke_gen

func TestGenerateSmokeCorpus(t *testing.T) {
    if os.Getenv("GENERATE_SMOKE") != "1" { t.Skip("set GENERATE_SMOKE=1 to regenerate") }
    // Build a chain in-memory, produce 5 blocks with a couple of transfers,
    // emit blocks.bin, then run the conformance engine's capture-side helpers
    // to emit seed.json + oracle.ndjson + fixture.json.
    ...
}
```

- [ ] 运行一次 `GENERATE_SMOKE=1 go test ./core/conformance/ -run TestGenerateSmokeCorpus -tags=smoke_gen`，把产物 checkin。
- [ ] `divergence-allowlist.json` 写 `[]`。
- [ ] `.gitattributes` 加 `test/fixtures/mainnet-blocks/*/blocks.bin filter=lfs diff=lfs merge=lfs -text`；但 smoke 的 blocks.bin 体积很小（< 50 KB），可暂时 opt-out：在对应子路径加一行覆盖规则 `test/fixtures/mainnet-blocks/smoke/blocks.bin !filter !diff !merge text`，以让 smoke 不走 lfs。
- [ ] Commit。

**验收：** `make conformance-replay` 在 smoke range 上 `PASS`。

---

## Task 11 · PR-2 收尾

- [ ] `go test ./... -count=1 -timeout 300s` 全绿。
- [ ] Makefile 的 `test:` target 不触发 conformance-replay（保持 hermetic）。
- [ ] `docs/dev/conformance-harness.md` 骨架：只列 smoke 用法。

---

# PR-3 · 捕获工具（Go helpers）+ 捕获协议文档

**目标：** 把 capture 过程中**可以在本仓库里单测的部分**写死 — 两个 Go 小工具 + 一份供操作员照做的协议文档。不写 bash capture 脚本（那份等 Phase 2 操作员补）。

## Task 12 · `cmd/fixture-closure`

**目标：** 读 `blocks.bin`，按 ContractType switch 从每个 tx 抽出 owner/receiver/contract 地址；再叠加每个 block 的 witness 地址；输出去重后的 41-hex 列表。

**Files:**
- Create: `cmd/fixture-closure/main.go`
- Create: `cmd/fixture-closure/main_test.go`（或 package 内 _test）

- [ ] 每个 ContractType 对应的地址提取规则写在 `core/conformance/closure.go` 里（测试更简单、不依赖 main 包）。main.go 做 CLI wrapper。
- [ ] 对 `ContractType_TransferContract`：parse `TransferContract` 取 `owner_address`、`to_address`。同样处理 `TransferAssetContract`, `TriggerSmartContract`（`contract_address` + `owner_address`）, `CreateSmartContract`, `VoteWitnessContract`（每个 vote 的 `vote_address`）, `FreezeBalanceContract`（v1）, `FreezeBalanceV2Contract`（v2）, `UnfreezeBalanceContract`, `DelegateResourceContract`, `UnDelegateResourceContract`, `WithdrawBalanceContract`, `AccountUpdateContract`, `AccountPermissionUpdateContract`, `UpdateBrokerageContract`。其余列入 TODO comment；未处理的 ContractType 不 panic、只 warn。
- [ ] **测试：** 针对 smoke corpus 的 `blocks.bin`（纯空块），期望 closure = {witness_address}。此外再针对手工组装的含一条 `TransferContract` 的 `*corepb.Block` 测试 owner+to 被抽出。
- [ ] Commit：`feat(conformance): M0'' PR-3 Task 12 — fixture-closure`。

**验收：** `go run ./cmd/fixture-closure --blocks=test/fixtures/mainnet-blocks/smoke/blocks.bin` 输出 `["41aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]`。

---

## Task 13 · `cmd/fixture-digest`

**目标：** 从 java-tron 输出的 JSON 状态快照（schema 见 Task 14 文档）计算 DigestB/DigestC。

**输入 JSON schema** (`capture-snapshot.json` 单个 block 版)：

```json
{
  "blockNum": 1000000,
  "accounts": [
    { "address": "41aaa…", "accountProto": "<base64 of Account proto>" },
    …
  ],
  "contractStates": [
    { "address": "41bbb…", "contractStateProto": "<base64>" }
  ],
  "code": [
    { "address": "41bbb…", "codeHex": "6080…" }
  ],
  "dp": { "energy_fee": 100, "total_energy_current_limit": 50000000000, … }
}
```

**Files:**
- Create: `cmd/fixture-digest/main.go`
- Create: `core/conformance/snapshot.go` — `LoadSnapshot(reader) (*state.StateDB, *state.DynamicProperties, []Address)`
- Create: `core/conformance/snapshot_test.go`

- [ ] `LoadSnapshot` 使用与 `LoadSeed` 同一个 in-mem StateDB 构造路径；把 `accountProto` base64-decode → `proto.Unmarshal` → `sdb.SetAccount`（新增方法？如 StateDB 无直接 SetAccount，加一个 internal helper；若太复杂，走 `CreateAccount` + 逐字段复制）；`contractStateProto` 写 rawdb；`code` 用 `sdb.SetCode`。
- [ ] `fixture-digest`：读 stdin 或 `--input`，吐出一行 `OracleEntry` JSON（含 `blockNum`、`digestB`、可选 `diagC`）。
- [ ] **测试：**
  - 构造一个 `capture-snapshot.json`（手写一个账户 + 几个 DP），喂给 `fixture-digest`。
  - 拿 smoke corpus 的 `oracle.ndjson[0]`（engine 自己算出来的），用相同账户/DP 手工构造一个 snapshot，期望 `fixture-digest` 出的 digestB 和 oracle 一致。这是"digest 算法跨格式自洽"的证据。
- [ ] Commit：`feat(conformance): M0'' PR-3 Task 13 — fixture-digest`。

**验收：** 自洽测试通过。

---

## Task 14 · Capture Protocol 文档

把 capture 的 java-tron 端协议写死在 `docs/dev/conformance-harness.md`，让下一个有 java-tron 的操作员能照做。

**Files:**
- Modify: `docs/dev/conformance-harness.md`

- [ ] 新增 "Capture protocol" 顶级段落，覆盖：
  - **Why manual**：java-tron 没有 getAccount-at-height 接口；唯一办法是 prune 后从 `start-1` 一块一块推进，每块末尾 dump 状态。不在本仓库里自动化。
  - **Expected flow**（伪流程，操作员自己补 bash）：
    1. 停 java-tron；用 `db-fast-fork` 或等效工具 prune 到 `start-1`。
    2. 启 java-tron，等它稳定在 `start-1`。
    3. `go run ./cmd/fixture-closure --blocks=<pre-fetched blocks.bin> > closure.json`。
    4. Dump snapshot at `start-1` → `seed.json`（格式见 §Capture JSON schema）。
    5. 让 java-tron 逐块推进；每块末尾 HTTP 取 closure 里每个地址的 `wallet/getaccount` + DP + contract state → `capture-snapshot.json` 喂给 `cmd/fixture-digest` → 追加一行到 `oracle.ndjson`。
    6. `fixture.json` 用同一对 `(javaTronVersion, jarSha256)` + `start/end/genesisTime/activeWitnesses` 填好。
  - **Capture JSON schema** — 给 Task 13 的 snapshot 格式写清楚。
  - **Pre-fetching blocks**：`blocks.bin` 的 varint framing 写明白，建议 `wallet/getblockbynum` 遍历 + `proto.Marshal` 拼接。
  - **Sanity check**：操作员捕获完后跑 `make conformance-replay RANGES=<name>`，应该先看到很多 divergence —— 那些就是 allowlist 的候选。
- [ ] Commit：`docs(conformance): M0'' PR-3 Task 14 — capture protocol`。

**验收：** 读过文档的操作员（本仓库外）可以写一份 bash 实现协议，**不**需要来回问问题。

---

## Task 15 · Phase 1 收尾

- [ ] `go test ./... -count=1 -timeout 300s` 全绿。
- [ ] 在 `core/conformance/doc.go` 顶部加一句 "Phase 1 delivers …; Phase 2 operator task …"。
- [ ] **PLAN.md 进度表**：M0″ 拆成 `M0″ Phase 1 (engine+helpers+docs)` = **完成**，`M0″ Phase 2 (mainnet ranges+cross e2e)` = **待 java-tron 操作员**。
- [ ] **TODO.md §6**：注明 M0″ Phase 1 落地日期 + commit range；Phase 2 blocker = operator access。
- [ ] Commit：`docs(conformance): M0'' Phase 1 complete, Phase 2 operator-gated`。

**验收：** `make test` 绿；`make conformance-replay` smoke 绿；PLAN/TODO 说明清楚 Phase 2 的前置条件。

---

# PR-4 · 录入 3 段真实 mainnet range  (Phase 2 — operator-gated)

**目标：** 真 corpus 落盘、跑起来、把所有现有 divergence 写进 allowlist，配 tracking issue。

**前置：** 能访问 mainnet-synced 本地 java-tron 节点；PR-3 的 fixture-closure / fixture-digest / 捕获协议文档已 merge。

## Task 16 · 三段 range 的高度定案

- [ ] 确认 Range-1 heights：`[X-50, X+450]` 取 proposal #62 激活前后的 500 块。查 `core/forks/forks.go` 中 Freeze V2 对应的 AllowFlag → 在 mainnet 实际激活高度（参考 tronscan），落到计划文档。
- [ ] 确认 Range-2 heights：最近一次 maintenance rollover 的 `[tick-20, tick+480]`。
- [ ] 确认 Range-3 heights：USDT 高密度交易区间，例如最近一个 24 h 窗口里交易数 top-1% 的 500 块区间。

**动作**：把确认后的高度写入 `docs/dev/conformance-harness.md`。

## Task 17 · 本地 java-tron 同步到 Range-1 前哨

- [ ] 用 `docs/dev/java-tron-local.md` 指引启动 mainnet full node；等其同步到 Range-1 起点之后。
- [ ] 运行 `capture_range.sh range-freeze-v2 $START $END`。
- [ ] 产物大小估算；确定是否开 git-lfs。

## Task 18 · 在 Range-1 上跑 replay；补 allowlist

- [ ] `gtron-replay --range=test/fixtures/mainnet-blocks/range-freeze-v2`。
- [ ] 对每一条 hard divergence：记录到 `divergence-allowlist.json`，填 reason（如 "known: reward v2 VI timing divergence at maintenance boundary"）、trackingIssue（先用 `internal:M1.5-vi-timing`）、expires=null。
- [ ] 重复直到 replay 返回 `PASS` 且 allowlist 里只有"已知"条目。

## Task 19 · Range-2、Range-3 同步重复

- [ ] 重复 Task 17–18 for range-maintenance、range-contract。

## Task 20 · PR-4 收尾

- [ ] `docs/dev/conformance-harness.md` 加 "Known divergences" 小节，列出 allowlist 条目 → follow-up issue 映射。
- [ ] 新增 `docs/dev/conformance-ranges.md`：每 range 一段，给出 heights、选取理由、corpus 大小、first-time recording date。
- [ ] Commit。

**验收：** `make conformance-replay` 三个真 range 全部返回 `PASS`（可能带 allowlist hits）；`make conformance-replay-exit-gate` 失败（因 allowlist 非空）——这是正确的阶段性状态。

---

# PR-5 · Cross e2e + docs  (Phase 2 — operator-gated)

**目标：** 双节点端到端 + 面向操作员的完整文档。

**前置：** 同 PR-4。

## Task 21 · `scripts/system_test_cross.sh`

**Files:**
- Create: `scripts/system_test_cross.sh`

- [ ] 参考 `scripts/system_test.sh` 结构；第二个节点用 java-tron 启动（复用 `scripts/fixtures/lib/api.sh` 里的 `start_java_tron` 等）。
- [ ] 场景：
  - 转账：gtron 广播，两端都确认，余额一致
  - 部署合约 + 调用：同上，合约状态一致
  - Vote + reward withdraw：跨一次 maintenance，两端奖励一致
  - Freeze-V2 delegate + undelegate：两端账户 frozenV2 / delegated 字段一致

- [ ] Makefile：`make system-test-cross`。

## Task 22 · `docs/dev/conformance-harness.md`

**Files:**
- Modify: `docs/dev/conformance-harness.md`

- [ ] 目录：
  - Overview（指 spec）
  - Prerequisites（java-tron jar、git-lfs、JDK）
  - Running replay（`make conformance-replay`）
  - Running cross e2e（`make system-test-cross`）
  - Capturing a new range（capture_range.sh 用法）
  - Interpreting a failure（Report.String() 解读 + C-digest diff）
  - Allowlist policy（entry 字段语义、何时加、何时清）
  - Exit-gate criterion（allowlist 全空 → 可宣告 M0″ 完成）

## Task 23 · 更新 PLAN.md / TODO.md

- [ ] PLAN.md 进度表：M0″ 状态从"未开始"改为"完成（allowlist 不空）"或"完成（exit-gate 绿）"视实际。
- [ ] TODO.md §6：标注 "Conformance corpus (M0″): harness landed <date>, outstanding allowlist → M1.5/M1.8 follow-up PRs"。

## Task 24 · PR-5 收尾

- [ ] `go test ./... -count=1 -timeout 300s` 绿。
- [ ] `make conformance-replay` 三 range 全绿。
- [ ] 最后 commit：`docs(m0''): milestone landed, allowlist as residual backlog`。

---

## Post-PR-5（非本计划范围）

M0″ 的"真正退出"= 所有 allowlist 条目清空。每个清空动作是一个独立 PR（不在本 plan 里）：

- 修 M1.5 VI timing → 清 range-maintenance allowlist 里的 VI 条目
- 修 M1.8 window size → 清 range-contract allowlist 里的 window 条目
- 修 M1.8 lock/unlock key 分拆 → 清相关条目

每个修复 PR 都跑 `make conformance-replay-exit-gate`，所有条目清空后它会返回 0，那一刻 M0″ 真正完成并可关掉 G1。

---

## 测试矩阵（跨 PR）

| Gate | 跑什么 | 什么时候跑 |
|---|---|---|
| `make test` | 全部单测（含 `core/conformance/` + smoke corpus replay） | 每次 commit |
| `make conformance-replay` | 3 真 range + smoke | PR-4 之后每次 state-layer 变更 |
| `make conformance-replay-exit-gate` | 同上 + 要求 allowlist 空 | M0″ 收官 check-in 时 |
| `make system-test-cross` | 1 gtron + 1 java-tron e2e | pre-merge 任一 P2P/consensus 改动 |

---

## 回滚策略

- engine 本身（PR-1）不改触及现有代码路径，回滚只需 revert。
- PR-2–4 引入的 fixture 目录、CLI、脚本都是新增；回滚只需 revert + LFS untrack（如有）。
- PR-5 的 `system_test_cross.sh` 是新增；回滚不影响 `system_test.sh`。
- `core/state.DynamicProperties.AllKeys()` 若 Task 3 为此新增，回滚时需同时 revert 该方法。
