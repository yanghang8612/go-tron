package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

func TestRelativeLocationsAndCorruption(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		file := filepath.Join(t.TempDir(), "sample.gtxi")
		res, e := freezer.BuildTransactionIndexRun(file, freezer.TransactionIndexBuildOptions{PrefixBits: 8, StartBlock: 1000, EndBlock: 9192, Iterate: func(yield func(freezer.TransactionIndexEntry) error) error {
			for i := uint64(0); i < 10; i++ {
				var h [32]byte
				binary.BigEndian.PutUint64(h[:], i<<56)
				loc := marker | ((1000 + i*800) << 16) | (i * 7000)
				if mixed && i == 0 {
					loc = 1000
				}
				if e := yield(freezer.TransactionIndexEntry{Hash: h, Location: loc}); e != nil {
					return e
				}
			}
			return nil
		}})
		if e != nil {
			t.Fatal(e)
		}
		b, e := os.ReadFile(file)
		if e != nil {
			t.Fatal(e)
		}
		s, e := inspect(b, manifestRun{File: "sample.gtxi", Start: 1000, End: 9192, Rows: 10})
		if e != nil {
			t.Fatal(e)
		}
		if s.Rows != 10 || s.Bytes != res.FileBytes {
			t.Fatal("lost rows")
		}
		if !mixed && (s.RelativeWidth != 4 || s.CandidateBytes != s.Bytes) {
			t.Fatalf("bad compact result %+v", s)
		}
		if mixed && (s.Plain != 1 || s.RelativeWidth != 8 || s.CandidateBytes != s.Bytes) {
			t.Fatal("mixed run must keep original")
		}
		b[len(b)-1] ^= 1
		if _, e = inspect(b, manifestRun{File: "sample.gtxi", Start: 1000, End: 9192, Rows: 10}); e == nil {
			t.Fatal("corruption accepted")
		}
	}
}
