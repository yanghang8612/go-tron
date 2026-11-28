package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/pruning"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	"google.golang.org/protobuf/proto"
)

type postingPruneAdmissionTestChain struct {
	beforeAdmission func()
	admitted        bool
}

func (c *postingPruneAdmissionTestChain) PruneStateChangePostingChunkWithAdmission(_ context.Context, _, _ uint64, _, anchor common.Hash, _ []byte, _ rawdb.StateChangePostingPruneLimits, admit core.StateChangePostingPruneAdmission) (rawdb.StateChangePostingPruneChunkResult, common.Hash, core.StateChangePostingPruneTimings, error) {
	c.beforeAdmission() // Represents state changing while the core queued its lock.
	release, ok := admit()
	if release != nil {
		defer release()
	}
	c.admitted = ok
	if !ok {
		return rawdb.StateChangePostingPruneChunkResult{}, anchor, core.StateChangePostingPruneTimings{ChainWait: 20, Admission: 3}, core.ErrStateChangePostingPruneDeferred
	}
	return rawdb.StateChangePostingPruneChunkResult{RowsScanned: 1, ScanDuration: 5, WriteDuration: 2}, anchor, core.StateChangePostingPruneTimings{ChainWait: 20, ChainHeld: 11, Admission: 3, Proof: 1, GateHeld: 8}, nil
}

func TestRuntimePostingPruneRechecksAfterCoreWait(t *testing.T) {
	for _, mode := range []string{"ready", "debt-relief", "debt-relief-late-latency", "debt-relief-late-queue", "debt-relief-gate-lost", "new-pressure", "stale-device", "unavailable", "gate-lost", "gate-hard-pressure"} {
		t.Run(mode, func(t *testing.T) {
			gate := maintenance.NewHeavyWorkGate()
			pressure := maintenance.StoragePressure{Available: true, SampledAt: time.Now(), DeviceAvailable: true, DeviceSampledAt: time.Now(), DeviceAwait: time.Millisecond}
			if !gate.CanTryAcquire() || !pruning.PostingPrunePressureReady(pressure, time.Now()) {
				t.Fatal("initial preflight must be ready")
			}
			var otherRelease func()
			defer func() {
				if otherRelease != nil {
					otherRelease()
				}
			}()
			chain := &postingPruneAdmissionTestChain{beforeAdmission: func() {
				switch mode {
				case "new-pressure":
					pressure.WriteStalled = true
				case "debt-relief":
					pressure.CompactionDebt = 40 << 30
				case "debt-relief-late-latency":
					pressure.CompactionDebt = 40 << 30
					pressure.DeviceAwait = 3 * time.Millisecond
				case "debt-relief-late-queue":
					pressure.CompactionDebt = 40 << 30
					pressure.DeviceQueueMilli = 16_001
				case "stale-device":
					pressure.DeviceSampledAt = time.Now().Add(-16 * time.Second)
				case "unavailable":
					pressure = maintenance.StoragePressure{}
				case "gate-lost", "debt-relief-gate-lost":
					if mode == "debt-relief-gate-lost" {
						pressure.CompactionDebt = 40 << 30
					}
					var ok bool
					otherRelease, ok = gate.TryAcquire()
					if !ok {
						t.Fatal("intervening owner could not acquire")
					}
				case "gate-hard-pressure":
					gate.SetAdmissionCheck(func() bool { return false })
				}
			}}
			out, err := runtimePostingPruneChunk(chain, gate, func() maintenance.StoragePressure { return pressure })(context.Background(), pruning.PostingPruneBoundary{}, common.Hash{}, nil, rawdb.StateChangePostingPruneLimits{})
			if err != nil || out.Timings.ChainWait != 20 || out.Timings.Admission != 3 {
				t.Fatalf("adapter dropped timings or leaked deferred error: %+v %v", out, err)
			}
			wantReady := mode == "ready" || mode == "debt-relief"
			wantGate := mode == "gate-lost" || mode == "gate-hard-pressure" || mode == "debt-relief-gate-lost"
			if chain.admitted != wantReady || out.GateDeferred != wantGate || out.PressureDeferred != (!wantReady && !wantGate) || out.Deferred == wantReady {
				t.Fatalf("late admission=%+v admitted=%v", out, chain.admitted)
			}
			if out.DebtRelief != (mode == "debt-relief") {
				t.Fatalf("wrong locked-admission debt tier: %+v", out)
			}
			if wantReady && (out.Timings.Proof != 1 || out.Timings.GateHeld != 8 || out.Result.ScanDuration != 5 || out.Result.WriteDuration != 2 || !gate.CanTryAcquire()) {
				t.Fatal("adapter lost phase values or lease release")
			}
		})
	}
}

func TestPostingPruneEnabled(t *testing.T) {
	for _, value := range []string{"", "0", "1", "true", " 1", "2"} {
		got, err := postingPruneEnabled(value)
		valid := value == "" || value == "0" || value == "1"
		if (err == nil) != valid || got != (value == "1") {
			t.Fatalf("%q: enabled=%v err=%v", value, got, err)
		}
	}
}

type directoryPruneUnsupportedTestChain struct{}

func (directoryPruneUnsupportedTestChain) PruneStateChangeDirectoryChunkWithAdmission(_ context.Context, _, _ uint64, _, anchor common.Hash, _ []byte, _ rawdb.StateChangePostingPruneLimits, _ core.StateChangePostingPruneAdmission) (rawdb.StateChangePostingPruneChunkResult, common.Hash, core.StateChangePostingPruneTimings, error) {
	return rawdb.StateChangePostingPruneChunkResult{}, anchor, core.StateChangePostingPruneTimings{}, core.ErrStateChangeDirectoryPruneUnsupported
}

func TestRuntimeDirectoryPruneUnsupportedAndCursorHintFile(t *testing.T) {
	gate := maintenance.NewHeavyWorkGate()
	pressure := maintenance.StoragePressure{Available: true, SampledAt: time.Now(), DeviceAvailable: true, DeviceSampledAt: time.Now(), DeviceAwait: time.Millisecond}
	out, err := runtimeDirectoryPruneChunk(directoryPruneUnsupportedTestChain{}, gate, func() maintenance.StoragePressure { return pressure })(
		context.Background(), pruning.PostingPruneBoundary{}, common.Hash{}, nil, runtimePostingPruneLimits(),
	)
	if err != nil || !out.DirectoryUnsupported {
		t.Fatalf("unexpected directory unsupported mapping: %+v %v", out, err)
	}

	hintPath := filepath.Join(t.TempDir(), "posting-prune.cursor")
	if got := loadPostingPruneCursorHint(hintPath); got != nil {
		t.Fatalf("missing hint returned %x", got)
	}
	if err := os.WriteFile(hintPath, []byte("short-bad-hint"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := loadPostingPruneCursorHint(hintPath); got != nil {
		t.Fatalf("corrupt hint returned %x", got)
	}
	valid := make([]byte, len("state-change-posting-v3-")+32+8)
	copy(valid, "state-change-posting-v3-")
	valid[len(valid)-1] = 7
	savePostingPruneCursorHint(hintPath, valid)
	if got := loadPostingPruneCursorHint(hintPath); !bytes.Equal(got, valid) {
		t.Fatalf("loaded hint = %x, want %x", got, valid)
	}
	savePostingPruneCursorHint(hintPath, nil)
	if _, err := os.Stat(hintPath); !os.IsNotExist(err) {
		t.Fatalf("cleared hint still exists: %v", err)
	}
}

func TestPostingPruneWorkerEndToEnd(t *testing.T) {
	dir := t.TempDir()
	keyBytes := make([]byte, 32)
	keyBytes[31] = 1
	key, err := crypto.BytesToPrivateKey(keyBytes)
	if err != nil {
		t.Fatal(err)
	}
	witness := crypto.PubkeyToAddress(&key.PublicKey)
	receiver := dbRebuildTraceAddressT(0xe1)

	genesisPath := filepath.Join(t.TempDir(), "genesis.json")
	encoded, err := json.Marshal(genesisFile{
		ChainID: 1999, P2PVersion: 1999, ParentHash: strings.Repeat("0", 64),
		Accounts:          []genesisFileAccount{{Address: witness.Hex(), Balance: "99000000000000000"}, {Address: receiver.Hex(), Balance: "1"}},
		Witnesses:         []genesisFileWitness{{Address: witness.Hex(), VoteCount: 1000, URL: "http://witness.test"}},
		DynamicProperties: map[string]int64{"maintenance_time_interval": 21600000, "next_maintenance_time": 1<<62 - 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(genesisPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	genesis, err := loadGenesisFile(genesisPath)
	if err != nil {
		t.Fatal(err)
	}
	genesis.Config.HistoryEnabled = true

	db, historyDir, err := openDirectHistoryRuntime(makeDBFlagSet(t, nil), dir, chainDataDir(dir), 32, 32, rawdb.DefaultPebbleOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer storedReplayTestClose(t, db)
	if err := ensurePagedDelegationLayout(db); err != nil {
		t.Fatal(err)
	}
	_, genesisHash, err := core.SetupGenesisBlock(db, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := core.NewBlockChain(db, state.NewDatabase(rawdb.WrapKeyValueStore(db)), genesis.Config)
	if err != nil {
		t.Fatal(err)
	}
	defer storedReplayTestClose(t, bc)
	bc.SetEngine(dpos.New(bc))

	parent := genesisHash
	var lastBlock *types.Block
	for n := int64(1); n <= 3; n++ {
		block := dbBackfillTransferBlock(t, n, n*3000, parent, witness, receiver, 1_000_000)
		pb := block.Proto()
		tx := pb.Transactions[0]
		tx.RawData.RefBlockBytes = []byte{0, 0}
		tx.RawData.RefBlockHash = append([]byte(nil), genesisHash[8:16]...)
		txHash := types.NewTransactionFromPB(tx).Hash()
		sig, err := crypto.Sign(txHash[:], key)
		if err != nil {
			t.Fatal(err)
		}
		tx.Signature = [][]byte{sig}
		root, err := types.TransactionMerkleRoot(pb.Transactions)
		if err != nil {
			t.Fatal(err)
		}
		pb.BlockHeader.RawData.TxTrieRoot = root.Bytes()
		pb.BlockHeader.RawData.WitnessAddress = witness.Bytes()
		pb.BlockHeader.RawData.Version = params.BlockVersion
		header, err := proto.Marshal(pb.BlockHeader.RawData)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(header)
		pb.BlockHeader.WitnessSignature, err = crypto.Sign(digest[:], key)
		if err != nil {
			t.Fatal(err)
		}
		signed := types.NewBlockFromPB(pb)
		if err := bc.InsertBlock(signed); err != nil {
			t.Fatalf("InsertBlock(%d): %v", n, err)
		}
		lastBlock = signed
		parent = signed.Hash()
	}
	if err := bc.EnsureStateHistoryIndexStage(); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, lastBlock.Number(), lastBlock.Hash()); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageStateHistoryIndex, lastBlock.Number(), lastBlock.Hash()); err != nil {
		t.Fatal(err)
	}

	// Seed 4 stale posting frames across different key hashes so we can pick a
	// middle key as the restart cursor hint (testing probe -> jump -> wrap).
	var staleKeys [][]byte
	for i := 0; i < 4; i++ {
		ch := &rawdb.StateDomainChange{
			BlockNum: 1, FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner: common.Address{0x41, byte(0x50 + i)}, Domain: kvdomains.ContractStorage,
			Key: []byte{byte(i + 1)},
		}
		if err := rawdb.WriteStateDomainChangePostingIndex(db, ch); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.ScanStateChangePostingFrames(context.Background(), db, func(row rawdb.StateChangePostingFrameCursor) error {
		if row.FirstBlock == 1 {
			staleKeys = append(staleKeys, bytes.Clone(row.Key))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(staleKeys) < 4 {
		t.Fatalf("expected >=4 stale posting frames, got %d", len(staleKeys))
	}
	sort.Slice(staleKeys, func(i, j int) bool { return bytes.Compare(staleKeys[i], staleKeys[j]) < 0 })
	hintKey := staleKeys[2] // strict middle key so both > hint and <= hint have stale rows

	liveDurable := &rawdb.StateDomainChange{
		BlockNum: 2, FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner: receiver, Domain: kvdomains.ContractStorage,
		Key: []byte("live-durable"),
	}
	if err := rawdb.WriteStateDomainChangePostingIndex(db, liveDurable); err != nil {
		t.Fatal(err)
	}

	hintPath := filepath.Join(dir, "posting-prune.cursor")
	savePostingPruneCursorHint(hintPath, hintKey)
	if historyDir == "" {
		t.Fatal("missing history dir")
	}

	gate := maintenance.NewHeavyWorkGate()
	boundary := pruning.PostingPruneBoundary{
		PrunedThrough: 1,
		ProofHead:     lastBlock.Number(),
		ProofHash:     lastBlock.Hash(),
	}
	loadProbe := func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{
			Available: true, SampledAt: now,
			DeviceAvailable: true, DeviceSampledAt: now,
			DeviceAwait: time.Millisecond, DeviceQueueMilli: 500,
			L0Sublevels: 1, L0StopWritesThreshold: 64, MemTableCount: 1, MemTableStopWritesThreshold: 8,
		}
	}
	w := pruning.NewPostingPruneWorker(pruning.PostingPruneWorkerConfig{
		Boundary:       func() pruning.PostingPruneBoundary { return boundary },
		Chunk:          runtimePostingPruneChunk(bc, gate, loadProbe),
		DirectoryChunk: runtimeDirectoryPruneChunk(bc, gate, loadProbe),
		LoadCursorHint: func() []byte { return loadPostingPruneCursorHint(hintPath) },
		SaveCursorHint: func(cursor []byte) { savePostingPruneCursorHint(hintPath, cursor) },
		Limits:         rawdb.StateChangePostingPruneLimits{MaxScannedRows: 2, MaxScannedBytes: 1 << 20, MaxDeleteBytes: 256 << 10, MaxDuration: time.Second},
		LoadProbe:      loadProbe,
		HeavyWorkGate:  gate,
		Interval:       5 * time.Millisecond,
		MetricsNamespace: "test/posting_e2e/",
	})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Stop() }()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var remainingStale int
		if err := rawdb.ScanStateChangePostingFrames(context.Background(), db, func(row rawdb.StateChangePostingFrameCursor) error {
			if row.LastBlock <= 1 {
				remainingStale++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		_, statErr := os.Stat(hintPath)
		var orphanDirs int
		var liveDirPresent bool
		if err := rawdb.ScanStateChangeKeyDirectory(context.Background(), db, func(row rawdb.StateChangeDirectoryCursor) error {
			for i := byte(0); i < 4; i++ {
				if bytes.Contains(row.Key, []byte{0x41, 0x50 + i}) {
					orphanDirs++
				}
			}
			if bytes.Contains(row.Key, []byte("live-durable")) {
				liveDirPresent = true
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if remainingStale == 0 && os.IsNotExist(statErr) && liveDirPresent && orphanDirs == 0 {
			// Verify live block 2 posting is preserved alongside its directory entry.
			var livePostings int
			if err := rawdb.ScanStateChangePostingFrames(context.Background(), db, func(row rawdb.StateChangePostingFrameCursor) error {
				if row.LastBlock >= 2 {
					livePostings++
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if livePostings == 0 {
				t.Fatal("live postings vanished")
			}
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("timed out waiting for end-to-end posting + directory wrap-around sweep")
}
