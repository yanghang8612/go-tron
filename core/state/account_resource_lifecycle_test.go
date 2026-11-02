package state

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/anypb"
)

type frozenLifecycleMetadata struct {
	Root      tcommon.Hash
	Addresses []tcommon.Address
	Filtered  bool
}

func TestPrepareFrozenReadLifecycleImage(t *testing.T) {
	target := os.Getenv("GTRON_FROZEN_FIXTURE_OUT")
	if target == "" {
		t.Skip("manual fixed-image preparation only")
	}
	filtered := os.Getenv("GTRON_FROZEN_FIXTURE_LEGACY") != "1"
	image, root, addresses := frozenLifecycleImage(t, 1024, filtered)
	if err := os.CopyFS(filepath.Join(target, "image"), os.DirFS(image)); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.MarshalIndent(frozenLifecycleMetadata{root, addresses, filtered}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "metadata.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
}

// The cold fixture keeps unconsumed, valid active-permission unknown fields
// between the small rows of different owners. They make the image larger than
// the fixed 16 MiB engine cache without inflating the consumed account envelope
// or inventing invalid frozen rows. All arms use the same sealed physical image.
func frozenLifecycleImage(t testing.TB, owners int, filtered bool) (string, tcommon.Hash, []tcommon.Address) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "image")
	opts := pebbledb.DefaultOptions()
	opts.EnableL6Filter, opts.DisableAutomaticCompactions, opts.LBaseMaxBytes = filtered, true, 1
	disk, err := pebbledb.New(dir, 16, 16, "bench/frozen/seed/", false, opts)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(tcommon.Hash{}, NewDatabase(rawdb.WrapKeyValueStore(disk)))
	if err != nil {
		t.Fatal(err)
	}
	addresses := make([]tcommon.Address, owners)
	padding := make([]byte, 64<<10)
	x := uint64(0x123456789abcdef)
	for i := range padding {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		padding[i] = byte(x)
	}
	unknown := protowire.AppendTag(nil, 100, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, padding)
	for i := range addresses {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		h := sha256.Sum256(key[:])
		addr := tcommon.BytesToAddress(append([]byte{tcommon.AddressPrefixMainnet}, h[:20]...))
		addresses[i] = addr
		s.CreateAccount(addr, corepb.AccountType_Normal)
		s.AddBalance(addr, 100_000_000)
		active := &corepb.Permission{Type: corepb.Permission_Active, Id: 2, PermissionName: "padding", Threshold: 1}
		active.ProtoReflect().SetUnknown(unknown)
		s.SetPermissions(addr, types.MakeDefaultOwnerPermission(addr), nil, []*corepb.Permission{active})
		if i%4 == 0 {
			s.FreezeV1Bandwidth(addr, int64(10_000_000+i), 100_000)
		}
		if i%3 == 0 {
			s.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 2_000_000)
		}
	}
	root, err := s.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	comparer := *pebble.DefaultComparer
	comparer.Split = func(key []byte) int { return len(key) }
	if !filtered {
		// An L0 table's existing filter can survive a trivial move to L6.
		// Re-encode the exact physical key/value stream without any writer
		// filter, rather than falsely calling such moved tables "legacy".
		source, err := pebble.Open(dir, &pebble.Options{Comparer: &comparer, ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		dir = filepath.Join(t.TempDir(), "legacy-image")
		levels := make([]pebble.LevelOptions, 7)
		for i := range levels {
			levels[i].Compression = pebble.NoCompression
		}
		legacy, err := pebble.Open(dir, &pebble.Options{Comparer: &comparer, Levels: levels, LBaseMaxBytes: 1, DisableAutomaticCompactions: true, MemTableSize: 64 << 20})
		if err != nil {
			t.Fatal(err)
		}
		iterator, err := source.NewIter(nil)
		if err != nil {
			t.Fatal(err)
		}
		for iterator.First(); iterator.Valid(); iterator.Next() {
			if err := legacy.Set(iterator.Key(), iterator.Value(), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
		}
		if err := iterator.Error(); err != nil {
			t.Fatal(err)
		}
		if err := iterator.Close(); err != nil {
			t.Fatal(err)
		}
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
		if err := legacy.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := legacy.Compact(nil, []byte{255}, false); err != nil {
			t.Fatal(err)
		}
		if err := legacy.Close(); err != nil {
			t.Fatal(err)
		}
	}
	engine, err := pebble.Open(dir, &pebble.Options{Comparer: &comparer, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	tables, err := engine.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	if len(tables[6]) == 0 {
		t.Fatal("fixture did not reach actual L6")
	}
	var data, filters uint64
	for _, table := range tables[6] {
		data += table.Properties.DataSize
		filters += table.Properties.FilterSize
	}
	if filtered != (filters > 0) {
		t.Fatalf("L6 filter property mismatch: filtered=%v bytes=%d", filtered, filters)
	}
	t.Logf("sealed L6 fixture: owners=%d root=%s tables=%d data=%d filter=%d", owners, root.Hex(), len(tables[6]), data, filters)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, root, addresses
}

func frozenLifecycleBlock(t testing.TB, addresses []tcommon.Address, number uint64) *types.Block {
	t.Helper()
	txs := make([]*corepb.Transaction, len(addresses))
	for i, owner := range addresses {
		parameter, err := anypb.New(&contractpb.TransferContract{OwnerAddress: owner.Bytes(), ToAddress: addresses[(i+1)%len(addresses)].Bytes(), Amount: 1})
		if err != nil {
			t.Fatal(err)
		}
		txs[i] = &corepb.Transaction{RawData: &corepb.TransactionRaw{Contract: []*corepb.Transaction_Contract{{Type: corepb.Transaction_Contract_TransferContract, Parameter: parameter}}}}
	}
	return types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), WitnessAddress: addresses[0].Bytes()}}, Transactions: txs})
}

// Run each compiled arm in a separate process with the same -benchtime=16x.
// Includes the actual planner, prefetch, cold envelope hydration, permission
// validation, frozen lookup, mutation, commitment, scope Close and buffer flush.
// The warm control deliberately revisits one owner set; pre70 consumes no row1.
func BenchmarkAccountFrozenReadLifecycle(b *testing.B) {
	const owners, batch = 1024, 64
	filtered := os.Getenv("GTRON_FROZEN_FIXTURE_LEGACY") != "1"
	var image string
	var initialRoot tcommon.Hash
	var addresses []tcommon.Address
	if fixed := os.Getenv("GTRON_FROZEN_FIXTURE_DIR"); fixed != "" {
		encoded, err := os.ReadFile(filepath.Join(fixed, "metadata.json"))
		if err != nil {
			b.Fatal(err)
		}
		var metadata frozenLifecycleMetadata
		if err := json.Unmarshal(encoded, &metadata); err != nil {
			b.Fatal(err)
		}
		image, initialRoot, addresses, filtered = filepath.Join(fixed, "image"), metadata.Root, metadata.Addresses, metadata.Filtered
		if len(addresses) != owners {
			b.Fatal("fixed fixture owner count mismatch")
		}
	} else {
		image, initialRoot, addresses = frozenLifecycleImage(b, owners, filtered)
	}
	for _, role := range []string{"pre70", "post70", "positive-warm"} {
		b.Run(role, func(b *testing.B) {
			dir := filepath.Join(b.TempDir(), "copy")
			if err := os.CopyFS(dir, os.DirFS(image)); err != nil {
				b.Fatal(err)
			}
			namespace := "bench/frozen/" + b.Name() + "/"
			opts := pebbledb.DefaultOptions()
			opts.EnableL6Filter, opts.DisableAutomaticCompactions = filtered, true
			disk, err := pebbledb.New(dir, 16, 16, namespace, false, opts)
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				if err := disk.Close(); err != nil {
					b.Error(err)
				}
			}()
			history, err := pebbledb.New(filepath.Join(b.TempDir(), "history"), 16, 16, namespace+"history/", false, pebbledb.DefaultOptions())
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				if err := history.Close(); err != nil {
					b.Error(err)
				}
			}()
			sink := rawdb.NewDirectHistoryStore(disk, history)
			s, err := New(initialRoot, NewDatabase(rawdb.WrapKeyValueStore(sink)))
			if err != nil {
				b.Fatal(err)
			}
			buf := blockbuffer.New(sink)
			buf.SetBaseReadCacheSize(8 << 20)
			s.SetAccountKVIndexStore(buf)
			commits := s.NewCommitScope()
			defer commits.Discard()
			p := NewStateReadAhead(buf, StateReadAheadConfig{Workers: 1})
			defer p.Close()
			blocks := make([]*types.Block, b.N)
			sets := make([][]tcommon.Address, b.N)
			for i := range blocks {
				start := (i * batch) % owners
				if role == "positive-warm" {
					start = 0
				}
				sets[i] = addresses[start : start+batch]
				if role == "positive-warm" {
					sets[i] = make([]tcommon.Address, batch)
					for j := range sets[i] {
						sets[i][j] = addresses[j*4]
					}
				}
				blocks[i] = frozenLifecycleBlock(b, sets[i], uint64(i+1))
			}
			counter := func(suffix string) int64 {
				return metrics.GetOrRegisterCounter(namespace+"disk/physical/read/sst/"+suffix, nil).Snapshot().Count()
			}
			beforeCalls, beforeBytes := counter("calls"), counter("bytes")
			historyCounter := func(suffix string) int64 {
				return metrics.GetOrRegisterCounter(namespace+"history/disk/physical/read/sst/"+suffix, nil).Snapshot().Count()
			}
			historyCalls, historyBytes := historyCounter("calls"), historyCounter("bytes")
			var demandCalls, demandBytes int64
			var prefetchTime, demandTime, commitTime time.Duration
			var root tcommon.Hash
			var consumed int64
			b.ReportAllocs()
			b.ResetTimer()
			for i, block := range blocks {
				number := uint64(i + 1)
				buf.BeginBlock(tcommon.Hash{31: byte(number)}, number)
				start := time.Now()
				if !p.warmBlock(stateReadAheadJob{block: block, epoch: p.epoch.Load()}) {
					b.Fatal("prefetch failed")
				}
				prefetchTime += time.Since(start)
				closeScope, err := s.BeginCanonicalStorageReadScope(number)
				if err != nil || s.storageReadScope == nil {
					b.Fatalf("scope=%v err=%v", s.storageReadScope, err)
				}
				calls, bytes := counter("calls"), counter("bytes")
				start = time.Now()
				for _, addr := range sets[i] {
					if _, err := s.AccountPermissionByID(addr, 0); err != nil {
						b.Fatal(err)
					}
					var frozen int64
					if role == "pre70" {
						frozen, err = s.GetAccountFrozenBandwidthV1(addr)
					} else {
						frozen, err = s.GetAccountFrozenBandwidth(addr)
					}
					if err != nil {
						b.Fatal(err)
					}
					consumed += frozen
					s.SetNetUsage(addr, int64(i+1))
					s.AddBalance(addr, 1)
				}
				demandTime += time.Since(start)
				demandCalls += counter("calls") - calls
				demandBytes += counter("bytes") - bytes
				start = time.Now()
				if err := closeScope(); err != nil {
					b.Fatal(err)
				}
				root, _, err = s.CommitWithStatsOptionsInScope(commits, CommitOptions{BlockNumber: number})
				if err != nil {
					b.Fatal(err)
				}
				if err := commits.FlushLatest(); err != nil {
					b.Fatal(err)
				}
				buf.CommitBlock()
				if err := buf.Flush(sink); err != nil {
					b.Fatal(err)
				}
				commitTime += time.Since(start)
			}
			b.StopTimer()
			for _, item := range []struct {
				name  string
				value float64
			}{
				{"sst_calls/op", float64(counter("calls")-beforeCalls) / float64(b.N)}, {"sst_bytes/op", float64(counter("bytes")-beforeBytes) / float64(b.N)},
				{"history_sst_calls/op", float64(historyCounter("calls")-historyCalls) / float64(b.N)}, {"history_sst_bytes/op", float64(historyCounter("bytes")-historyBytes) / float64(b.N)},
				{"demand_sst_calls/op", float64(demandCalls) / float64(b.N)}, {"demand_sst_bytes/op", float64(demandBytes) / float64(b.N)},
				{"prefetch_ns/op", float64(prefetchTime.Nanoseconds()) / float64(b.N)}, {"demand_ns/op", float64(demandTime.Nanoseconds()) / float64(b.N)}, {"commit_ns/op", float64(commitTime.Nanoseconds()) / float64(b.N)},
			} {
				b.ReportMetric(item.value, item.name)
			}
			b.Logf("outcome root=%s consumed=%d blocks=%d owners/block=%d", root.Hex(), consumed, b.N, batch)
		})
	}
}
