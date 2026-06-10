package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

func TestStatePrefetcherWarmsRawLatestRows(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddr(0x44)
	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("account")); err != nil {
		t.Fatalf("WriteStateAccountLatest: %v", err)
	}
	if err := rawdb.WriteStateKVGeneration(db, owner, 7); err != nil {
		t.Fatalf("WriteStateKVGeneration: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.SystemReward, []byte("reward"), []byte("value")); err != nil {
		t.Fatalf("WriteStateKVLatest reward: %v", err)
	}

	slot := tcommon.Hash{0x01}
	meta := &contractpb.SmartContract{
		Version: 1,
		TrxHash: []byte{
			0xaa, 0xbb, 0xcc,
		},
	}
	metaBytes, err := proto.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal contract metadata: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.ContractMetadata, contractMetaKVKey, metaBytes); err != nil {
		t.Fatalf("WriteStateKVLatest metadata: %v", err)
	}
	storageRowKey := javaStorageRowKey(owner, slot, meta)
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.ContractStorage, storageRowKey.Bytes(), []byte("storage")); err != nil {
		t.Fatalf("WriteStateKVLatest storage: %v", err)
	}

	p := NewStatePrefetcher(db, StatePrefetcherConfig{Workers: 2, Queue: 8})
	p.Start()
	if accepted := p.Enqueue([]PrefetchKey{
		AccountPrefetchKey(owner),
		AccountKVPrefetchKey(owner, kvdomains.SystemReward, []byte("reward")),
		ContractStoragePrefetchKey(owner, slot),
	}); accepted != 3 {
		t.Fatalf("accepted = %d, want 3", accepted)
	}
	p.Stop()

	stats := p.Stats()
	if stats.Enqueued != 3 || stats.Processed != 3 || stats.Hits != 3 || stats.Misses != 0 || stats.Errors != 0 || stats.Dropped != 0 {
		t.Fatalf("stats = %+v, want 3 hits and no misses/errors/drops", stats)
	}
}

func TestStatePrefetcherDropsWhenQueueFull(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddr(0x45)
	p := NewStatePrefetcher(db, StatePrefetcherConfig{Workers: 1, Queue: 1})

	accepted := p.Enqueue([]PrefetchKey{
		AccountPrefetchKey(owner),
		AccountPrefetchKey(testAddr(0x46)),
	})
	p.Stop()

	stats := p.Stats()
	if accepted != 1 || stats.Enqueued != 1 || stats.Dropped != 1 || stats.Processed != 0 {
		t.Fatalf("accepted=%d stats=%+v, want one queued, one dropped, none processed before Start", accepted, stats)
	}
}

func TestStatePrefetcherRecordsErrors(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddr(0x47)
	p := NewStatePrefetcher(db, StatePrefetcherConfig{Workers: 1, Queue: 2})
	p.Start()
	p.Enqueue([]PrefetchKey{
		AccountKVPrefetchKey(owner, kvdomains.KVDomain(0xffff), []byte("bad-domain")),
	})
	p.Stop()

	stats := p.Stats()
	if stats.Processed != 1 || stats.Errors != 1 || stats.Hits != 0 {
		t.Fatalf("stats = %+v, want one processed error", stats)
	}
}

func TestStatePrefetcherStopIsIdempotentAndDropsAfterStop(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	p := NewStatePrefetcher(db, StatePrefetcherConfig{Workers: 1, Queue: 2})
	p.Start()
	p.Stop()
	p.Stop()

	if accepted := p.Enqueue([]PrefetchKey{AccountPrefetchKey(testAddr(0x48))}); accepted != 0 {
		t.Fatalf("accepted after Stop = %d, want 0", accepted)
	}
	if stats := p.Stats(); stats.Dropped != 1 {
		t.Fatalf("stats after enqueue stopped = %+v, want one drop", stats)
	}
}
