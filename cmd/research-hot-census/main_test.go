package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type fixtureWriter struct{ db *pebble.DB }

func (w fixtureWriter) Put(k, v []byte) error { return w.db.Set(k, v, pebble.Sync) }
func (w fixtureWriter) Delete(k []byte) error { return w.db.Delete(k, pebble.Sync) }
func TestFixedCopyCensusDistinguishesCoverageAndStaging(t *testing.T) {
	r := filepath.Join(t.TempDir(), "maintenance", "fixture", "chaindata")
	if e := os.MkdirAll(r, 0755); e != nil {
		t.Fatal(e)
	}
	db, e := pebble.Open(r, &pebble.Options{})
	if e != nil {
		t.Fatal(e)
	}
	w := fixtureWriter{db}
	check := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	for id, n := range map[rawdb.StageID]uint64{rawdb.StageFinish: 100000, rawdb.StageChainFreezer: 30000, rawdb.StageFreezerTxIndexPrune: 30001} {
		check(rawdb.WriteStageProgress(w, id, n))
	}
	var solid common.Hash
	binary.BigEndian.PutUint64(solid[:8], 100000)
	rawdb.WriteHeadSolidBlockHash(w, solid)
	for _, n := range []uint64{30000, 30001, 70000, 99900} {
		block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(n)}}, Transactions: []*corepb.Transaction{{}, {}}})
		check(rawdb.WriteBlock(w, block))
	}
	for _, n := range []uint64{99999, 100001} {
		block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(n)}}})
		check(rawdb.WriteSyncStagedBlock(w, block))
	}
	check(rawdb.WriteStateKVLatest(w, common.Address{}, 0, kvdomains.AccountAssetV2, []byte("7"), []byte{0, 0, 0, 0, 0, 0, 0, 1}))
	check(rawdb.WriteTransactionLocation(w, make([]byte, 32), 30000, 3))
	check(db.Close())
	oldFlags, oldArgs := flag.CommandLine, os.Args
	defer func() { flag.CommandLine = oldFlags; os.Args = oldArgs }()
	flag.CommandLine = flag.NewFlagSet("fixture", flag.ExitOnError)
	out := filepath.Join(filepath.Dir(r), "output")
	os.Args = []string{"census", "-db", r, "-out", out, "-logical-mib-sec", "0"}
	main()
	b, e := os.ReadFile(filepath.Join(out, "hot-census.json"))
	check(e)
	var got report
	check(json.Unmarshal(b, &got))
	if !got.Complete || got.Head != 100000 || got.ColdExclusive != 30001 || got.TxPrunedExclusive != 30001 || got.EligibleExclusive != 99873 || !got.Skipped["account_kv_latest"] {
		t.Fatalf("bad boundaries %+v", got)
	}
	for name, rows := range map[string]uint64{"block_body/below_published_or_pruned_boundary": 1, "block_body/eligible_complete_unarchived": 1, "block_body/eligible_incomplete_segment": 1, "block_body/recent_retention": 1, "staged_body/staged_not_ahead_of_head": 1, "staged_body/ahead_of_head": 1, "transaction_index/below_published_or_pruned_boundary": 1} {
		if got.Windows[name] == nil || got.Windows[name].Rows != rows {
			t.Fatalf("%s got %+v", name, got.Windows[name])
		}
	}
	if got.Families["block_body"].Transactions != 8 {
		t.Fatal(got.Families["block_body"])
	}
	if unknownName([]byte{'b', 'h', '-', 0xff, 0x40}) != "other/bh-" {
		t.Fatal("hash names must not become per-hash map keys")
	}
}
