package main

import (
	"crypto/sha256"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type fixtureWriter struct{ db *pebble.DB }

func (w fixtureWriter) Put(k, v []byte) error { return w.db.Set(k, v, pebble.Sync) }
func (w fixtureWriter) Delete(k []byte) error { return w.db.Delete(k, pebble.Sync) }
func TestCopyCompactionKeepsSourceAndUnrelatedRows(t *testing.T) {
	r := filepath.Join(t.TempDir(), "maintenance")
	s := filepath.Join(r, "source", "chaindata")
	if e := os.MkdirAll(s, 0755); e != nil {
		t.Fatal(e)
	}
	db, e := pebble.Open(s, &pebble.Options{DisableAutomaticCompactions: true})
	if e != nil {
		t.Fatal(e)
	}
	w := fixtureWriter{db}
	var hash common.Hash
	hash[10] = 7
	rawdb.WriteHeadBlockHash(w, hash)
	block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1}, WitnessSignature: make([]byte, 2<<20)}})
	if e = rawdb.WriteSyncStagedBlock(w, block); e != nil {
		t.Fatal(e)
	}
	if e = db.Flush(); e != nil {
		t.Fatal(e)
	}
	if e = rawdb.DeleteSyncStagedBlock(w, 1); e != nil {
		t.Fatal(e)
	}
	if e = db.Flush(); e != nil {
		t.Fatal(e)
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	before := map[string][32]byte{}
	files, e := os.ReadDir(s)
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			b, e := os.ReadFile(filepath.Join(s, f.Name()))
			if e != nil {
				t.Fatal(e)
			}
			before[f.Name()] = sha256.Sum256(b)
		}
	}
	oldArgs, oldFlags := os.Args, flag.CommandLine
	defer func() { os.Args = oldArgs; flag.CommandLine = oldFlags }()
	out := filepath.Join(r, "compact")
	flag.CommandLine = flag.NewFlagSet("fixture", flag.ExitOnError)
	os.Args = []string{"compact", "-source", s, "-out", out}
	main()
	for n, sum := range before {
		b, e := os.ReadFile(filepath.Join(s, n))
		if e != nil || sha256.Sum256(b) != sum {
			t.Fatalf("source changed %s: %v", n, e)
		}
	}
	q, e := pebble.Open(filepath.Join(out, "chaindata"), &pebble.Options{ReadOnly: true})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	v, closer, e := q.Get([]byte("LastBlock"))
	if e != nil {
		t.Fatal(e)
	}
	defer closer.Close()
	if string(v) != string(hash[:]) {
		t.Fatal("unrelated head changed")
	}
}
