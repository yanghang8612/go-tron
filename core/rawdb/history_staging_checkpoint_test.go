package rawdb

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

type stagingCheckpointCountingStore struct {
	*historyStagingSyncFaultStore
	puts map[string]int
}

func (s *stagingCheckpointCountingStore) NewBatch() ethdb.Batch {
	return &stagingCheckpointCountingBatch{Batch: s.KeyValueStore.NewBatch(), puts: s.puts}
}

type stagingCheckpointCountingBatch struct {
	ethdb.Batch
	puts map[string]int
}

func (b *stagingCheckpointCountingBatch) Put(key, value []byte) error {
	if bytes.HasPrefix(key, historyStagingPayloadPrefix) {
		b.puts[string(key)]++
	}
	return b.Batch.Put(key, value)
}

func TestHistoryStagingCopyCheckpointResumesWithoutRewritingPrefix(t *testing.T) {
	ctx := context.Background()
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	proof := stagingProtocolProof(t, m, 1)
	first, _, _ := StateHistoryChunkBucketBounds(1)
	keys := stagingProtocolRows(t, m, first)
	claim, err := m.BeginClaim(ctx, proof, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	counted := &stagingCheckpointCountingStore{historyStagingSyncFaultStore: &historyStagingSyncFaultStore{KeyValueStore: m.stage}, puts: make(map[string]int)}
	m.stage = counted
	limits := stagingProtocolLimits()
	checkpoints := 0
	limits.Checkpoint = func(work uint64) error {
		checkpoints++
		// These APIs acquire route locks; a checkpoint under routeMu would
		// deadlock. The source must stay authoritative throughout copy.
		route, present, err := m.ReadRoute(1)
		if err != nil || !present || route.Owner != HistoryStagingOwnerSource {
			t.Fatalf("copy changed route at checkpoint: %+v %v", route, err)
		}
		return nil
	}
	receipt, err := m.CopyClaim(ctx, claim, limits)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoints < int(3*StateHistoryChunkBucketBlocks) {
		t.Fatalf("physical/logical passes omitted checkpoints: %d", checkpoints)
	}
	for key, n := range counted.puts {
		if n != 1 {
			t.Fatalf("copied prefix key %x written %d times", key, n)
		}
	}
	if len(counted.puts) < int(receipt.TxRangeRows+receipt.PayloadRows+receipt.ChunkRows) {
		t.Fatal("copy did not cover every physical family")
	}
	// A hook does not replace receipt, canonical proof or adoption validation.
	wrong := proof
	wrong.Epoch++
	if _, err := m.AdoptClaim(ctx, claim, wrong); err == nil {
		t.Fatal("changed adoption proof accepted")
	}
	for _, key := range keys {
		if present, err := m.hot.Has(key); err != nil || !present {
			t.Fatalf("failed proof removed source: %v", err)
		}
	}
	if _, err := m.AdoptClaim(ctx, claim, proof); err != nil {
		t.Fatal(err)
	}
	limits.Checkpoint = func(uint64) error { return errors.New("pause source clearing") }
	if err := m.ClearSource(ctx, claim, limits); err == nil {
		t.Fatal("clear ignored checkpoint failure")
	}
	for _, key := range keys {
		if present, err := m.hot.Has(key); err != nil || !present {
			t.Fatalf("paused clear removed source: %v", err)
		}
	}
	limits.Checkpoint = nil
	if err := m.ClearSource(ctx, claim, limits); err != nil {
		t.Fatal(err)
	}
	route, _, _ := m.ReadRoute(1)
	if !route.SourceCleared {
		t.Fatal("clear did not resume")
	}
}

func TestHistoryStagingCopyCheckpointCancellationKeepsSource(t *testing.T) {
	ctx := context.Background()
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	proof := stagingProtocolProof(t, m, 1)
	first, _, _ := StateHistoryChunkBucketBounds(1)
	keys := stagingProtocolRows(t, m, first)
	claim, err := m.BeginClaim(ctx, proof, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	limits := stagingProtocolLimits()
	calls := 0
	limits.Checkpoint = func(uint64) error {
		calls++
		if calls == int(2*StateHistoryChunkBucketBlocks)+100 {
			return context.Canceled
		}
		return nil
	}
	if _, err := m.CopyClaim(ctx, claim, limits); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v want cancellation", err)
	}
	route, _, err := m.ReadRoute(1)
	if err != nil || route.Owner != HistoryStagingOwnerSource {
		t.Fatalf("cancel changed owner: %+v %v", route, err)
	}
	if _, ready, err := m.ReadReceiptAt(1, 1); err != nil || ready {
		t.Fatalf("canceled copy published receipt: ready=%t err=%v", ready, err)
	}
	for _, key := range keys {
		if present, err := m.hot.Has(key); err != nil || !present {
			t.Fatalf("cancel removed source: %v", err)
		}
	}
	limits.Checkpoint = nil
	if _, err := m.CopyClaim(ctx, claim, limits); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryStagingClearRechecksSpaceAfterYield(t *testing.T) {
	ctx := context.Background()
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	proof := stagingProtocolProof(t, m, 1)
	first, _, _ := StateHistoryChunkBucketBounds(1)
	keys := stagingProtocolRows(t, m, first)
	claim, err := m.BeginClaim(ctx, proof, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	limits := stagingProtocolLimits()
	if _, err := m.CopyClaim(ctx, claim, limits); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AdoptClaim(ctx, claim, proof); err != nil {
		t.Fatal(err)
	}
	free := uint64(1 << 30)
	limits.FreeBytes = func() (uint64, error) { return free, nil }
	limits.Checkpoint = func(work uint64) error {
		if work == 0 {
			free = 0
		}
		return nil
	}
	if err := m.ClearSource(ctx, claim, limits); err == nil {
		t.Fatal("delete used pre-yield space observation")
	}
	for _, key := range keys {
		if present, err := m.hot.Has(key); err != nil || !present {
			t.Fatalf("out-of-space clear removed source: %v", err)
		}
	}
}

func TestHistoryStagingRouteHintScanIsBoundedAndSkipsClaimProofs(t *testing.T) {
	ctx := context.Background()
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	for first := uint64(0); first <= 600; first += 128 {
		if err := m.InitializeOfflineSourceRoutes(ctx, 1, first, min(first+127, 600)); err != nil {
			t.Fatal(err)
		}
	}
	// A scheduling hint must not decode an unrelated claim's large proof.
	if err := m.hot.Put(historyStagingBucketKey(historyStagingClaimPrefix, 1), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	var cursor []byte
	seen := uint64(0)
	for {
		routes, next, complete, err := m.ScanRoutes(ctx, cursor, 128)
		if err != nil {
			t.Fatal(err)
		}
		if len(routes) > 128 {
			t.Fatal("hint scan exceeded route budget")
		}
		for _, route := range routes {
			if route.Bucket != seen {
				t.Fatalf("bucket=%d want=%d", route.Bucket, seen)
			}
			seen++
		}
		if complete {
			break
		}
		cursor = next
	}
	if seen != 601 {
		t.Fatalf("scanned %d routes want601", seen)
	}
}
