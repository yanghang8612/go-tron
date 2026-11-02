package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
)

func TestOfflineRunnerCohort(t *testing.T) {
	dir := t.TempDir()
	rows := filepath.Join(dir, "fixed.rows")
	db := memorydb.New()
	defer db.Close()
	expected := make(map[string][]byte)
	for i := 0; i < 3000; i++ {
		addr := make([]byte, 21)
		addr[0] = 0x41
		binary.BigEndian.PutUint64(addr[13:], uint64(i))
		account := fixture(int64(i))
		native, err := statecodec.Marshal(account)
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateKVLatest(db, common.SystemAccountAddress, 0, kvdomains.SystemReward, rawdb.CycleAccountVoteStateKey(17, addr), native); err != nil {
			t.Fatal(err)
		}
		if i < 10 {
			var scalar [8]byte
			binary.BigEndian.PutUint64(scalar[:], uint64(i))
			for _, key := range [][]byte{rawdb.BeginCycleStateKey(addr), rawdb.EndCycleStateKey(addr), rawdb.CycleVoteStateKey(17, addr), rawdb.CycleRewardStateKey(17, addr)} {
				if err := rawdb.WriteStateKVLatest(db, common.SystemAccountAddress, 0, kvdomains.SystemReward, key, scalar[:]); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	f, err := os.Create(rows)
	if err != nil {
		t.Fatal(err)
	}
	it := db.NewIterator(nil, nil)
	for it.Next() {
		key, value := it.Key(), it.Value()
		frame := binary.BigEndian.AppendUint32(nil, uint32(len(key)))
		frame = append(frame, key...)
		frame = binary.BigEndian.AppendUint32(frame, uint32(len(value)))
		frame = append(frame, value...)
		if _, err := f.Write(frame); err != nil {
			t.Fatal(err)
		}
		logical, err := rawdb.DecodeStateKVLatestValue(value)
		if err != nil {
			t.Fatal(err)
		}
		expected[string(key)] = logical
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	it.Release()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	savedArgs, savedFlag := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = savedArgs, savedFlag })
	out := filepath.Join(dir, "out")
	os.Args = []string{"test-runner", "-rows", rows, "-out", out, "-input-chunk-mib", "1", "-decode-iterations", "1"}
	flag.CommandLine = flag.NewFlagSet("test-runner", flag.ContinueOnError)
	if err := run(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(out, "results.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Rows    uint64      `json:"rows"`
		Votes   uint64      `json:"account_vote_rows"`
		Changes uint64      `json:"changed_rows"`
		Tables  []tableStat `json:"tables"`
	}
	if err := json.Unmarshal(b, &result); err != nil || result.Rows != 3040 || result.Votes != 3000 || result.Changes != 3000 || len(result.Tables) != 4 {
		t.Fatalf("wrong cohort counts: %+v err=%v", result, err)
	}
	files, err := filepath.Glob(filepath.Join(out, "*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, path := range files {
		f, err := vfs.Default.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		readable, err := sstable.NewSimpleReadable(f)
		if err != nil {
			t.Fatal(err)
		}
		r, err := sstable.NewReader(readable, sstable.ReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		iter, err := r.NewIter(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(path)
		pair := strings.Join(strings.Split(name, "-")[:2], "-")
		for key, lazy := iter.First(); key != nil; key, lazy = iter.Next() {
			value, _, err := lazy.Value(nil)
			if err != nil {
				t.Fatal(err)
			}
			logical, err := rawdb.DecodeStateKVLatestValue(value)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.HasPrefix(logical, rewardMagic) {
				logical, err = decodeCompactSnapshot(logical)
				if err != nil {
					t.Fatal(err)
				}
			}
			if !bytes.Equal(logical, expected[string(key.UserKey)]) {
				t.Fatalf("SST returned mismatching row: %x", key.UserKey)
			}
			counts[pair]++
		}
		if err := iter.Error(); err != nil {
			t.Fatal(err)
		}
		if err := iter.Close(); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range []string{"baseline-raw", "compact-raw", "baseline-snappy", "compact-snappy"} {
		if counts[pair] != 3040 {
			t.Fatalf("readback %s got %d rows", pair, counts[pair])
		}
	}
}
