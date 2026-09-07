package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/urfave/cli/v2"
	"google.golang.org/protobuf/proto"
)

func storedReplayTestClose(t *testing.T, closer io.Closer) {
	t.Helper()
	if err := closer.Close(); err != nil {
		t.Errorf("close %T: %v", closer, err)
	}
}

func storedReplayTestContext(t *testing.T, args ...string) *cli.Context {
	t.Helper()
	set := flag.NewFlagSet(t.Name(), flag.ContinueOnError)
	for _, f := range app.Flags {
		if err := f.Apply(set); err != nil {
			t.Fatalf("apply %s: %v", f.Names()[0], err)
		}
	}
	if err := set.Parse(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	return cli.NewContext(app, set, nil)
}

func TestValidateStoredReplayOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		bad  bool
	}{
		{name: "ordinary startup"},
		{name: "replay", args: []string{"--sync.replay-stored-to", "2", "--sync.replay-audit", "audit.jsonl"}},
		{name: "optional profile", args: []string{"--sync.replay-stored-to", "2", "--sync.replay-audit", "audit.jsonl", "--sync.replay-cpuprofile", "cpu.pprof"}},
		{name: "missing audit", args: []string{"--sync.replay-stored-to", "2"}, bad: true},
		{name: "empty audit", args: []string{"--sync.replay-stored-to", "2", "--sync.replay-audit", ""}, bad: true},
		{name: "orphan audit", args: []string{"--sync.replay-audit", "audit.jsonl"}, bad: true},
		{name: "orphan profile", args: []string{"--sync.replay-cpuprofile", "cpu.pprof"}, bad: true},
		{name: "restart conflict", args: []string{"--sync.restart-from", "0"}, bad: true},
		{name: "stop conflict", args: []string{"--sync.stop-at", "2"}, bad: true},
		{name: "bootstrap conflict", args: []string{"--snapshot.bootstrap"}, bad: true},
		{name: "reset conflict", args: []string{"--snapshot.reset"}, bad: true},
		{name: "producer conflict", args: []string{"--witness"}, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := tc.args
			if strings.HasSuffix(tc.name, "conflict") {
				args = append([]string{"--sync.replay-stored-to", "2", "--sync.replay-audit", "audit.jsonl"}, args...)
			}
			err := validateStoredReplayOptions(storedReplayTestContext(t, args...))
			if (err != nil) != tc.bad {
				t.Fatalf("validateStoredReplayOptions(%v) = %v, want error=%v", args, err, tc.bad)
			}
		})
	}
}

// All integration cases call gtron, so they exercise the production placement
// of the replay branch after cold readers and the real DPoS engine are bound.
func TestGtronStoredReplayAuditAndOfflineStartup(t *testing.T) {
	for _, history := range []bool{false, true} {
		t.Run("history="+strconv.FormatBool(history), func(t *testing.T) {
			fixture := seedStoredReplayTestDatadir(t, false)
			auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
			profilePath := filepath.Join(t.TempDir(), "cpu.pprof")
			args := fixture.args(auditPath, "2")
			args = append(args, "--history.enabled="+strconv.FormatBool(history), "--sync.replay-cpuprofile", profilePath)
			// A successful offline run cannot depend on opening any of the
			// listeners used by a normal node.
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer storedReplayTestClose(t, listener)
			port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
			for _, name := range []string{"p2p.port", "http.port", "jsonrpc.port", "grpc.port", "pprof.port"} {
				args = append(args, "--"+name, port)
			}
			if err := gtron(storedReplayTestContext(t, args...)); err != nil {
				t.Fatalf("offline replay: %v", err)
			}
			assertStoredReplayNoNodeKey(t, fixture.dir)

			records := readStoredReplayTestAudit(t, auditPath)
			if len(records) != 4 || records[0].Type != "start" || records[1].Type != "block" || records[2].Type != "block" || records[3].Type != "complete" {
				t.Fatalf("audit record sequence = %+v", records)
			}
			db, err := rawdb.NewPebbleDB(chainDataDir(fixture.dir), 8, 32)
			if err != nil {
				t.Fatalf("reopen replayed database: %v", err)
			}
			defer storedReplayTestClose(t, db)
			chainDB := rawdb.NewChainDB(db, rawdb.NoopAncient{})
			if head := rawdb.ReadHeadBlockHash(db); head != fixture.blocks[1].Hash() {
				t.Fatalf("durable head = %s, want %s", head.Hex(), fixture.blocks[1].Hash().Hex())
			}
			for i, block := range fixture.blocks {
				record := records[i+1]
				if digest, err := hex.DecodeString(record.ReceiptsSHA256); err != nil || len(digest) != sha256.Size {
					t.Fatalf("block %d has invalid receipt digest %q: %v", block.Number(), record.ReceiptsSHA256, err)
				}
				root := rawdb.ReadBlockStateRoot(chainDB, block.Hash())
				if record.Number != block.Number() || record.Hash != block.Hash().Hex() || record.TransactionCount != len(block.Transactions()) || record.InternalRoot != root.Hex() || root == (common.Hash{}) {
					t.Fatalf("block %d audit does not describe persisted execution: record=%+v root=%s", block.Number(), record, root.Hex())
				}
				rng, ok, err := rawdb.ReadStateTxRange(db, block.Number())
				if err != nil {
					t.Fatal(err)
				}
				if history {
					if !ok || record.TxRange == nil || *record.TxRange != *rng || rng.BlockHash != block.Hash() || rng.BlockNum != block.Number() {
						t.Fatalf("block %d transaction range: audit=%+v disk=%+v ok=%v", block.Number(), record.TxRange, rng, ok)
					}
				} else if record.TxRange != nil {
					t.Fatalf("history-disabled audit has tx_range: %+v", record.TxRange)
				}
			}
			complete := records[3]
			wantRoot := storedReplayReferenceRoot(t, fixture, history)
			if complete.Blocks != 2 || complete.Transactions != 1 || complete.From != 1 || complete.To != 2 || complete.FinalRoot != wantRoot.Hex() {
				t.Fatalf("completion=%+v, want blocks=2 txs=1 from=1 to=2 root=%s", complete, wantRoot.Hex())
			}
			if complete.TotalElapsedSeconds <= 0 || complete.ReplayElapsedSeconds <= 0 || complete.TotalElapsedSeconds < complete.ReplayElapsedSeconds {
				t.Fatalf("invalid elapsed times: %+v", complete)
			}
			profile, err := os.ReadFile(profilePath)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := gzip.NewReader(bytes.NewReader(profile))
			if err != nil {
				t.Fatalf("CPU profile is not gzip: %v", err)
			}
			defer storedReplayTestClose(t, reader)
			if data, err := io.ReadAll(reader); err != nil || len(data) == 0 {
				t.Fatalf("CPU profile is empty or incomplete: bytes=%d err=%v", len(data), err)
			}
		})
	}
}

func TestGtronStoredReplayRejectsBadSignature(t *testing.T) {
	fixture := seedStoredReplayTestDatadir(t, true)
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	err := gtron(storedReplayTestContext(t, fixture.args(auditPath, "2")...))
	if err == nil || !strings.Contains(err.Error(), dpos.ErrInvalidSignature.Error()) {
		t.Fatalf("replay bad witness signature error = %v", err)
	}
	assertStoredReplayNoNodeKey(t, fixture.dir)
	for _, record := range readStoredReplayTestAudit(t, auditPath) {
		if record.Type == "complete" {
			t.Fatalf("failed replay produced success record: %+v", record)
		}
	}
	db, err := rawdb.NewPebbleDB(chainDataDir(fixture.dir), 8, 32)
	if err != nil {
		t.Fatalf("reopen rejected replay database: %v", err)
	}
	defer storedReplayTestClose(t, db)
	if head := rawdb.ReadHeadBlockHash(db); head != fixture.genesisHash {
		t.Fatalf("bad signature advanced head to %s", head.Hex())
	}
}

func TestGtronStoredReplayRejectsUnauthenticatedCheckpointHistoryRange(t *testing.T) {
	for _, which := range []string{"missing", "wrong hash"} {
		t.Run(which, func(t *testing.T) {
			fixture := seedStoredReplayTestDatadir(t, false)
			seedAudit := filepath.Join(t.TempDir(), "seed-audit.jsonl")
			args := append(fixture.args(seedAudit, "1"), "--history.enabled")
			if err := gtron(storedReplayTestContext(t, args...)); err != nil {
				t.Fatalf("advance history-enabled checkpoint: %v", err)
			}
			db, err := rawdb.NewPebbleDB(chainDataDir(fixture.dir), 8, 32)
			if err != nil {
				t.Fatal(err)
			}
			rng, present, err := rawdb.ReadStateTxRange(db, 1)
			if err != nil || !present {
				storedReplayTestClose(t, db)
				t.Fatalf("checkpoint range precondition: present=%v err=%v", present, err)
			}
			if which == "missing" {
				err = rawdb.DeleteStateTxRange(db, 1)
			} else {
				wrongHash := rng.BlockHash
				wrongHash[0] ^= 0xff
				err = rawdb.WriteStateTxRange(db, 1, wrongHash, rng.BeginTxNum, rng.EndTxNum)
			}
			closeErr := db.Close()
			if err != nil || closeErr != nil {
				t.Fatalf("alter checkpoint range: write=%v close=%v", err, closeErr)
			}
			auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
			args = append(fixture.args(auditPath, "2"), "--history.enabled")
			err = gtron(storedReplayTestContext(t, args...))
			if err == nil || !strings.Contains(err.Error(), "checkpoint history range") {
				t.Fatalf("unauthenticated checkpoint range accepted or wrong failure: %v", err)
			}
			if _, err := os.Stat(auditPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed history preflight created an audit: %v", err)
			}
			db, err = rawdb.NewPebbleDB(chainDataDir(fixture.dir), 8, 32)
			if err != nil {
				t.Fatal(err)
			}
			defer storedReplayTestClose(t, db)
			if got := rawdb.ReadHeadBlockHash(db); got != fixture.blocks[0].Hash() {
				t.Fatalf("unauthenticated checkpoint advanced head to %s", got.Hex())
			}
		})
	}
}

func TestGtronStoredReplayRejectsExistingOutputAndNonAdvancingTarget(t *testing.T) {
	for _, which := range []string{"audit", "profile", "equal target", "lower target"} {
		t.Run(which, func(t *testing.T) {
			fixture := seedStoredReplayTestDatadir(t, false)
			auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
			args := fixture.args(auditPath, "2")
			wantHead := fixture.genesisHash
			var protectedPath string
			const sentinel = "keep existing measurement\n"
			switch which {
			case "audit":
				protectedPath = auditPath
			case "profile":
				protectedPath = filepath.Join(t.TempDir(), "cpu.pprof")
				args = append(args, "--sync.replay-cpuprofile", protectedPath)
			case "equal target", "lower target":
				seedAudit := filepath.Join(t.TempDir(), "seed-audit.jsonl")
				if err := gtron(storedReplayTestContext(t, fixture.args(seedAudit, "1")...)); err != nil {
					t.Fatalf("advance fixture head to 1: %v", err)
				}
				wantHead = fixture.blocks[0].Hash()
				target := "1"
				if which == "lower target" {
					target = "0"
				}
				args = fixture.args(auditPath, target)
			}
			if protectedPath != "" {
				if err := os.WriteFile(protectedPath, []byte(sentinel), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			err := gtron(storedReplayTestContext(t, args...))
			if err == nil {
				t.Fatal("invalid replay unexpectedly succeeded")
			}
			if strings.HasSuffix(which, "target") && !strings.Contains(err.Error(), "must exceed current head") {
				t.Fatalf("non-advancing target error = %v", err)
			}
			if protectedPath != "" {
				if data, err := os.ReadFile(protectedPath); err != nil || string(data) != sentinel {
					t.Fatalf("existing measurement overwritten: data=%q err=%v", data, err)
				}
			}
			assertStoredReplayNoNodeKey(t, fixture.dir)
			db, err := rawdb.NewPebbleDB(chainDataDir(fixture.dir), 8, 32)
			if err != nil {
				t.Fatalf("reopen rejected replay database: %v", err)
			}
			defer storedReplayTestClose(t, db)
			if head := rawdb.ReadHeadBlockHash(db); head != wantHead {
				t.Fatalf("invalid replay advanced head to %s", head.Hex())
			}
		})
	}
}

type storedReplayTestRecord struct {
	Type                 string              `json:"type"`
	Number               uint64              `json:"number"`
	Hash                 string              `json:"hash"`
	InternalRoot         string              `json:"internal_root"`
	ReceiptsSHA256       string              `json:"receipts_sha256"`
	TransactionCount     int                 `json:"transaction_count"`
	TxRange              *rawdb.StateTxRange `json:"tx_range"`
	Blocks               uint64              `json:"blocks"`
	Transactions         uint64              `json:"transactions"`
	From                 uint64              `json:"from"`
	To                   uint64              `json:"to"`
	FinalRoot            string              `json:"final_root"`
	TotalElapsedSeconds  float64             `json:"total_elapsed_seconds"`
	ReplayElapsedSeconds float64             `json:"replay_elapsed_seconds"`
}

func TestStoredReplayReceiptsDigestUsesNewHotExecution(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run("compact="+strconv.FormatBool(compact), func(t *testing.T) {
			block, infos := dbRebuildTxIndexBlock(t, 7, 1)
			hotInfo := proto.Clone(infos[0]).(*corepb.TransactionInfo)
			hotInfo.Fee = 12345
			hotInfo.ContractResult = [][]byte{{0xaa, 0xbb}}
			coldInfo := proto.Clone(hotInfo).(*corepb.TransactionInfo)
			coldInfo.Fee = 98765
			coldInfo.ContractResult = [][]byte{{0xcc}}
			coldBytes, err := proto.Marshal(&corepb.TransactionRet{
				BlockNumber: int64(block.Number()), Transactioninfo: []*corepb.TransactionInfo{coldInfo},
			})
			if err != nil {
				t.Fatal(err)
			}
			ancient := &storedReplayReceiptsAncient{number: block.Number(), payload: coldBytes}
			db := ethrawdb.NewMemoryDatabase()
			defer storedReplayTestClose(t, db)
			chain := rawdb.NewChainDB(db, ancient)
			if err := rawdb.WriteBlock(db, block); err != nil {
				t.Fatal(err)
			}
			write := rawdb.WriteTransactionInfosByBlock
			if compact {
				write = rawdb.WriteCompactTransactionInfosByBlock
			}
			if err := write(db, block.Number(), []*corepb.TransactionInfo{hotInfo}); err != nil {
				t.Fatal(err)
			}
			// Verify this fixture actually exposes the stale-receipt hazard:
			// ordinary chain reads prefer the immutable ancient result.
			stale, present, err := rawdb.ReadTransactionInfosByBlockStrict(chain, block.Number())
			if err != nil || !present || len(stale) != 1 || stale[0].Fee != coldInfo.Fee || ancient.calls != 1 {
				t.Fatalf("ancient-first precondition: infos=%+v present=%v calls=%d err=%v", stale, present, ancient.calls, err)
			}
			ancient.calls = 0
			got, err := storedReplayReceiptsDigest(chain, block)
			if err != nil {
				t.Fatal(err)
			}
			// Build the expected byte stream from the newly executed receipt,
			// independent of the accessor selected by the production helper.
			payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(hotInfo)
			if err != nil {
				t.Fatal(err)
			}
			framed := binary.AppendUvarint(nil, uint64(len(payload)))
			framed = append(framed, payload...)
			want := sha256.Sum256(framed)
			if got != hex.EncodeToString(want[:]) || ancient.calls != 0 {
				t.Fatalf("receipt digest=%s want hot=%x ancient reads=%d", got, want, ancient.calls)
			}
		})
	}
}

func TestStoredReplayReceiptsDigestRejectsIncompleteOrMismatchedOutput(t *testing.T) {
	for _, which := range []string{"missing row", "too few", "too many", "wrong ID"} {
		t.Run(which, func(t *testing.T) {
			block, infos := dbRebuildTxIndexBlock(t, 7, 1)
			db := ethrawdb.NewMemoryDatabase()
			defer storedReplayTestClose(t, db)
			chain := rawdb.NewChainDB(db, rawdb.NoopAncient{})
			if err := rawdb.WriteBlock(db, block); err != nil {
				t.Fatal(err)
			}
			wantError := "receipt count"
			switch which {
			case "too few":
				infos = nil
			case "too many":
				infos = append(infos, proto.Clone(infos[0]).(*corepb.TransactionInfo))
			case "wrong ID":
				infos[0].Id[0] ^= 0xff
				wantError = "does not match canonical tx"
			}
			if which != "missing row" {
				if err := rawdb.WriteTransactionInfosByBlock(db, block.Number(), infos); err != nil {
					t.Fatal(err)
				}
			}
			if digest, err := storedReplayReceiptsDigest(chain, block); err == nil || !strings.Contains(err.Error(), wantError) || digest != "" {
				t.Fatalf("digest=%q err=%v, want no digest and %q", digest, err, wantError)
			}
		})
	}
}

type storedReplayReceiptsAncient struct {
	rawdb.NoopAncient
	number  uint64
	payload []byte
	calls   int
}

func (a *storedReplayReceiptsAncient) Ancient(kind string, number uint64) ([]byte, error) {
	a.calls++
	if kind == rawdb.AncientTxInfosTable && number == a.number {
		return a.payload, nil
	}
	return nil, rawdb.ErrNotInAncient
}

func readStoredReplayTestAudit(t *testing.T, path string) []storedReplayTestRecord {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer storedReplayTestClose(t, file)
	var records []storedReplayTestRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record storedReplayTestRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode audit line %d: %v", len(records)+1, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return records
}

type storedReplayTestFixture struct {
	dir         string
	genesisPath string
	genesisHash common.Hash
	blocks      []*types.Block
}

func (f storedReplayTestFixture) args(auditPath, target string) []string {
	return []string{"--datadir", f.dir, "--genesis", f.genesisPath,
		"--sync.replay-stored-to", target, "--sync.replay-audit", auditPath,
		"--freezer.disable", "--db.cache", "8", "--db.memtable", "8",
		"--state.trie.cache", "0", "--state.code.cache", "0", "--state.commitment.cache", "0"}
}

func assertStoredReplayNoNodeKey(t *testing.T, dir string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, "nodekey")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("offline replay created a node identity: %v", err)
	}
}

func seedStoredReplayTestDatadir(t *testing.T, badSignature bool) storedReplayTestFixture {
	t.Helper()
	fixture := storedReplayTestFixture{dir: t.TempDir(), genesisPath: filepath.Join(t.TempDir(), "genesis.json")}
	keyBytes := make([]byte, 32)
	keyBytes[31] = 1
	key, err := crypto.BytesToPrivateKey(keyBytes)
	if err != nil {
		t.Fatal(err)
	}
	witness := crypto.PubkeyToAddress(&key.PublicKey)
	receiver := dbRebuildTraceAddressT(0xe0)
	encoded, err := json.Marshal(genesisFile{
		ChainID: 1999, P2PVersion: 1999, ParentHash: strings.Repeat("0", 64),
		Accounts:          []genesisFileAccount{{Address: witness.Hex(), Balance: "99000000000000000"}, {Address: receiver.Hex(), Balance: "1"}},
		Witnesses:         []genesisFileWitness{{Address: witness.Hex(), VoteCount: 1000, URL: "http://witness.test"}},
		DynamicProperties: map[string]int64{"maintenance_time_interval": 21600000, "next_maintenance_time": 1<<62 - 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.genesisPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	genesis, err := loadGenesisFile(fixture.genesisPath)
	if err != nil {
		t.Fatal(err)
	}
	db, err := rawdb.NewPebbleDB(chainDataDir(fixture.dir), 8, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer storedReplayTestClose(t, db)
	_, fixture.genesisHash, err = core.SetupGenesisBlock(db, genesis)
	if err != nil {
		t.Fatal(err)
	}
	parent := fixture.genesisHash
	for n := int64(1); n <= 2; n++ {
		block := dbBackfillTransferBlock(t, n, n*3000, parent, witness, receiver, 5_000_000)
		pb := block.Proto()
		if n == 1 {
			pb.Transactions = nil
		} else {
			tx := pb.Transactions[0]
			tx.RawData.RefBlockBytes = []byte{0, 0}
			tx.RawData.RefBlockHash = append([]byte(nil), fixture.genesisHash[8:16]...)
			txHash := types.NewTransactionFromPB(tx).Hash()
			signature, err := crypto.Sign(txHash[:], key)
			if err != nil {
				t.Fatal(err)
			}
			tx.Signature = [][]byte{signature}
		}
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
		if badSignature && n == 1 {
			pb.BlockHeader.WitnessSignature = nil
		}
		block = types.NewBlockFromPB(pb)
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		fixture.blocks = append(fixture.blocks, block)
		parent = block.Hash()
	}
	return fixture
}

// Compare with normal InsertBlock on a separate database, not another call to
// the replay helper. Both paths must produce the same commitment root.
func storedReplayReferenceRoot(t *testing.T, fixture storedReplayTestFixture, history bool) common.Hash {
	t.Helper()
	genesis, err := loadGenesisFile(fixture.genesisPath)
	if err != nil {
		t.Fatal(err)
	}
	genesis.Config.HistoryEnabled = history
	db := ethrawdb.NewMemoryDatabase()
	defer storedReplayTestClose(t, db)
	if _, _, err := core.SetupGenesisBlock(db, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := core.NewBlockChain(db, state.NewDatabase(db), genesis.Config)
	if err != nil {
		t.Fatal(err)
	}
	defer storedReplayTestClose(t, bc)
	bc.SetEngine(dpos.New(bc))
	for _, block := range fixture.blocks {
		if err := bc.InsertBlock(types.NewBlockFromPB(proto.Clone(block.Proto()).(*corepb.Block))); err != nil {
			t.Fatalf("reference InsertBlock(%d): %v", block.Number(), err)
		}
	}
	if err := bc.Close(); err != nil {
		t.Fatalf("flush reference chain: %v", err)
	}
	return bc.HeadStateRoot()
}
