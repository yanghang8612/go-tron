package snapshots

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func historyStagingProofFixture(t *testing.T) (string, *Manifest, []rawdb.HistoryStagingBlockProof) {
	t.Helper()
	dir := t.TempDir()
	blocks := make([]rawdb.HistoryStagingBlockProof, rawdb.StateHistoryChunkBucketBlocks)
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i := range blocks {
		number := uint64(i + 1024)
		hash := common.Hash{byte(number >> 8), byte(number)}
		blocks[i] = rawdb.HistoryStagingBlockProof{Number: number, Hash: hash,
			BeginTxNum: number, EndTxNum: number}
		ranges[i] = &rawdb.StateTxRange{BlockNum: number, BlockHash: hash,
			BeginTxNum: number, EndTxNum: number}
	}
	change := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "cold-prev")
	change.BlockHash = blocks[5].Hash
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: blocks[0].BeginTxNum, ToTxNum: blocks[len(blocks)-1].EndTxNum,
		Path: stateDomainChangeHistorySegmentPath(blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum)}
	history, index, accessor, err := writeHistorySegmentFiles(dir, ref,
		[]*rawdb.StateDomainChange{change}, ranges)
	if err != nil {
		t.Fatal(err)
	}
	manifest := NewManifestForChain(ref.FromTxNum, ref.ToTxNum,
		[]SegmentRef{history, index, accessor}, ChainIdentity{
			ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	if err := manifest.ValidateProduction(); err != nil {
		t.Fatal(err)
	}
	return dir, manifest, blocks
}

func historyStagingTwoBucketProofFixture(t *testing.T) (string, *Manifest, []rawdb.HistoryStagingBlockProof) {
	t.Helper()
	dir := t.TempDir()
	blocks := make([]rawdb.HistoryStagingBlockProof, 2*rawdb.StateHistoryChunkBucketBlocks)
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i := range blocks {
		number := uint64(i + 1024)
		hash := common.Hash{byte(number >> 8), byte(number)}
		blocks[i] = rawdb.HistoryStagingBlockProof{Number: number, Hash: hash,
			BeginTxNum: number, EndTxNum: number}
		ranges[i] = &rawdb.StateTxRange{BlockNum: number, BlockHash: hash,
			BeginTxNum: number, EndTxNum: number}
	}
	changes := make([]*rawdb.StateDomainChange, 0, 2)
	for _, offset := range []int{5, 1029} {
		change := binaryStateDomainChange(blocks[offset].Number, blocks[offset].BeginTxNum, 1, "cold-prev")
		change.BlockHash = blocks[offset].Hash
		changes = append(changes, change)
	}
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: blocks[0].BeginTxNum, ToTxNum: blocks[len(blocks)-1].EndTxNum,
		Path: stateDomainChangeHistorySegmentPath(blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum)}
	history, index, accessor, err := writeHistorySegmentFiles(dir, ref, changes, ranges)
	if err != nil {
		t.Fatal(err)
	}
	manifest := NewManifestForChain(ref.FromTxNum, ref.ToTxNum,
		[]SegmentRef{history, index, accessor}, ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	return dir, manifest, blocks
}

func TestHistoryStagingColdProverReusesOneAuthenticatedReaderAcrossBuckets(t *testing.T) {
	dir, manifest, blocks := historyStagingTwoBucketProofFixture(t)
	p, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	p.EnableReaderReuse()
	defer p.Close()
	mask := make([]bool, rawdb.StateHistoryChunkBucketBlocks)
	for i := range mask {
		mask[i] = true
	}
	first := blocks[:rawdb.StateHistoryChunkBucketBlocks]
	second := blocks[rawdb.StateHistoryChunkBucketBlocks:]
	for _, bucket := range [][]rawdb.HistoryStagingBlockProof{first, second} {
		spans, err := p.Build(context.Background(), bucket, mask)
		if err != nil || len(spans) != 1 || spans[0].RowCount != 1 {
			t.Fatalf("bucket proof = %+v, %v", spans, err)
		}
	}
	stats := p.Stats()
	if stats.HistoryOpens != 1 || stats.IndexOpens != 1 || stats.ReaderReuses != 1 ||
		stats.FullTrioAuthenticates != 1 {
		t.Fatalf("not one authenticated reader pair: %+v", stats)
	}
	// The original immutable bytes at a new inode are not silently accepted by
	// an already pinned reader, even when the manifest checksum still matches.
	path := filepath.Join(dir, manifest.Segments[0].Path)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	replacement := path + ".replacement"
	if err := os.WriteFile(replacement, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Build(context.Background(), second, mask); err == nil {
		t.Fatal("replaced trio was accepted by a reused reader")
	}
	if p.open != nil {
		t.Fatal("changed trio left a reader open")
	}
}

func TestHistoryStagingColdProverCachedReaderStillRejectsWrongSemanticRange(t *testing.T) {
	dir, manifest, blocks := historyStagingTwoBucketProofFixture(t)
	p, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	p.EnableReaderReuse()
	defer p.Close()
	mask := make([]bool, rawdb.StateHistoryChunkBucketBlocks)
	for i := range mask {
		mask[i] = true
	}
	first := blocks[:rawdb.StateHistoryChunkBucketBlocks]
	if _, err := p.Build(context.Background(), first, mask); err != nil {
		t.Fatal(err)
	}
	wrong := append([]rawdb.HistoryStagingBlockProof(nil), first...)
	wrong[5].Hash[0] ^= 1
	if _, err := p.Build(context.Background(), wrong, mask); err == nil {
		t.Fatal("wrong canonical hash passed through the cached reader")
	}
}

func TestHistoryStagingColdProverReferenceReaderMatchesPublicIterator(t *testing.T) {
	dir, _, blocks := historyStagingTwoBucketProofFixture(t)
	changes := make([]*rawdb.StateDomainChange, 0, len(blocks))
	for _, block := range blocks {
		change := binaryStateDomainChange(block.Number, block.BeginTxNum, 1, "cold-prev")
		change.BlockHash = block.Hash
		changes = append(changes, change)
	}
	oldRefs := writeV6StateDomainHistorySegmentForTest(t, dir,
		blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum, changes)
	newRefs, _, err := ReencodeHistoryReferenceTrioContext(context.Background(), dir,
		oldRefs)
	if err != nil {
		t.Fatal(err)
	}
	manifest := *NewManifestForChain(blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum,
		newRefs, ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	if err := manifest.ValidateProduction(); err != nil {
		t.Fatal(err)
	}
	p, err := NewHistoryStagingColdProver(dir, &manifest)
	if err != nil {
		t.Fatal(err)
	}
	p.EnableReaderReuse()
	defer p.Close()
	mask := make([]bool, rawdb.StateHistoryChunkBucketBlocks)
	for i := range mask {
		mask[i] = true
	}
	for _, bucket := range [][]rawdb.HistoryStagingBlockProof{
		blocks[:rawdb.StateHistoryChunkBucketBlocks],
		blocks[rawdb.StateHistoryChunkBucketBlocks:],
	} {
		spans, err := p.Build(context.Background(), bucket, mask)
		if err != nil || len(spans) != 1 {
			t.Fatalf("reference proof = %+v, %v", spans, err)
		}
		var records [][32]byte
		var previousTx, ordinal uint64
		var havePrevious bool
		cfg, ok := DefaultDomainRegistry().ConfigForRef(newRefs[0])
		if !ok {
			t.Fatal("reference history has no domain config")
		}
		err = cfg.IterateHistoryRange(dir, &manifest, newRefs[0],
			bucket[0].BeginTxNum, bucket[len(bucket)-1].EndTxNum,
			func(change *rawdb.StateDomainChange) (bool, error) {
				if !havePrevious || change.TxNum != previousTx {
					ordinal = 0
				} else {
					ordinal++
				}
				previousTx, havePrevious = change.TxNum, true
				records = append(records, historyStagingRecordDigest(change, ordinal))
				return true, nil
			})
		if err != nil {
			t.Fatal(err)
		}
		rangeDigest, semantic := historyStagingSemanticDigest(bucket, records)
		if spans[0].TxRangeDigest != rangeDigest || spans[0].SemanticHash != semantic ||
			spans[0].RowCount != uint64(len(records)) {
			t.Fatalf("reference proof differs from public iterator: %+v", spans[0])
		}
	}
	stats := p.Stats()
	if stats.HistoryOpens != 1 || stats.IndexOpens != 1 || stats.ReaderReuses != 1 {
		t.Fatalf("reference reader did not reuse one trio: %+v", stats)
	}
}

func TestHistoryStagingColdProverEvictsReaderWhenTrioChanges(t *testing.T) {
	dir, _, blocks := historyStagingTwoBucketProofFixture(t)
	var refs []SegmentRef
	for _, bucket := range [][]rawdb.HistoryStagingBlockProof{
		blocks[:rawdb.StateHistoryChunkBucketBlocks],
		blocks[rawdb.StateHistoryChunkBucketBlocks:],
	} {
		ranges := make([]*rawdb.StateTxRange, len(bucket))
		for i, block := range bucket {
			ranges[i] = &rawdb.StateTxRange{BlockNum: block.Number, BlockHash: block.Hash,
				BeginTxNum: block.BeginTxNum, EndTxNum: block.EndTxNum}
		}
		ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
			FromTxNum: bucket[0].BeginTxNum, ToTxNum: bucket[len(bucket)-1].EndTxNum,
			Path: stateDomainChangeHistorySegmentPath(bucket[0].BeginTxNum,
				bucket[len(bucket)-1].EndTxNum)}
		history, index, accessor, err := writeHistorySegmentFiles(dir, ref, nil, ranges)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, history, index, accessor)
	}
	manifest := NewManifestForChain(blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum,
		refs, ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	p, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	p.EnableReaderReuse()
	defer p.Close()
	mask := make([]bool, rawdb.StateHistoryChunkBucketBlocks)
	for i := range mask {
		mask[i] = true
	}
	for _, bucket := range [][]rawdb.HistoryStagingBlockProof{
		blocks[:rawdb.StateHistoryChunkBucketBlocks],
		blocks[rawdb.StateHistoryChunkBucketBlocks:],
		blocks[:rawdb.StateHistoryChunkBucketBlocks],
	} {
		if _, err := p.Build(context.Background(), bucket, mask); err != nil {
			t.Fatal(err)
		}
	}
	if got := p.Stats(); got.HistoryOpens != 3 || got.IndexOpens != 3 ||
		got.FullTrioAuthenticates != 2 {
		t.Fatalf("reader eviction and auth counts = %+v", got)
	}
	if err := p.Close(); err != nil || p.open != nil {
		t.Fatalf("evicted reader remained open: %v", err)
	}
}

func TestHistoryStagingTrioAuthenticationCancelledLeaderLetsWaiterRetry(t *testing.T) {
	dir, manifest, _ := historyStagingProofFixture(t)
	leader, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	waiter, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	refs, id, err := leader.trio(leader.refs[0])
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	waiting := make(chan struct{})
	var once sync.Once
	leader.testAuth = func(ctx context.Context, _ [3]SegmentRef) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	waiter.testWait = func() { once.Do(func() { close(waiting) }) }
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderResult := make(chan error, 1)
	waiterResult := make(chan error, 1)
	go func() { leaderResult <- leader.authenticate(leaderCtx, refs, id) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("leader never entered full trio authentication")
	}
	go func() { waiterResult <- waiter.authenticate(context.Background(), refs, id) }()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never observed the in-flight authentication")
	}
	cancelLeader()
	select {
	case err := <-leaderResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("leader = %v, want cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled leader remained blocked")
	}
	select {
	case err := <-waiterResult:
		if err != nil {
			t.Fatalf("live waiter did not retry its own full audit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter was not awakened by cancelled leader")
	}
	if waiter.Stats().FullTrioAuthenticates != 1 {
		t.Fatalf("waiter authentication work = %+v", waiter.Stats())
	}
}

func TestHistoryStagingTrioAuthenticationWaiterCancellation(t *testing.T) {
	dir, manifest, _ := historyStagingProofFixture(t)
	leader, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	waiter, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	refs, id, err := leader.trio(leader.refs[0])
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	waiting := make(chan struct{})
	leader.testAuth = func(ctx context.Context, trio [3]SegmentRef) error {
		close(started)
		<-release
		return VerifyHistorySegmentWithCompanionsContext(ctx, dir, manifest, trio[0])
	}
	waiter.testWait = func() { close(waiting) }
	leaderResult := make(chan error, 1)
	waiterResult := make(chan error, 1)
	go func() { leaderResult <- leader.authenticate(context.Background(), refs, id) }()
	<-started
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	go func() { waiterResult <- waiter.authenticate(waiterCtx, refs, id) }()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never observed the in-flight authentication")
	}
	cancelWaiter()
	select {
	case err := <-waiterResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter = %v, want cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled waiter remained blocked")
	}
	close(release)
	select {
	case err := <-leaderResult:
		if err != nil {
			t.Fatalf("leader full audit = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("leader did not complete")
	}
}

func TestHistoryStagingTargetColdEquivalenceChecksPrevAndZeroBlocks(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, block := range blocks {
		if err := rawdb.WriteStateTxRange(db, block.Number, block.Hash, block.BeginTxNum, block.EndTxNum); err != nil {
			t.Fatal(err)
		}
	}
	check := func() error {
		view, release, err := rawdb.AcquireStateHistoryReadView(db)
		if err != nil {
			return err
		}
		defer release()
		_, err = VerifyHistoryStagingTargetColdEquivalence(context.Background(), view, dir, manifest, blocks)
		return err
	}
	if err := check(); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("missing target changeset = %v, want semantic mismatch", err)
	}
	change := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "cold-prev")
	change.BlockHash = blocks[5].Hash
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{change}); err != nil {
		t.Fatal(err)
	}
	if err := check(); err != nil {
		t.Fatalf("matching target and 1023 zero-change blocks: %v", err)
	}
	changed := *change
	changed.Prev = []byte("different-prev")
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{&changed}); err != nil {
		t.Fatal(err)
	}
	if err := check(); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("changed Prev = %v, want semantic mismatch", err)
	}
}

func TestHistoryStagingMixedTargetColdEquivalence(t *testing.T) {
	dir := t.TempDir()
	blocks := make([]rawdb.HistoryStagingBlockProof, rawdb.StateHistoryChunkBucketBlocks)
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i := range blocks {
		number := uint64(i + 1024)
		hash := common.Hash{byte(number >> 8), byte(number)}
		blocks[i] = rawdb.HistoryStagingBlockProof{Number: number, Hash: hash,
			BeginTxNum: number, EndTxNum: number}
		ranges[i] = &rawdb.StateTxRange{BlockNum: number, BlockHash: hash,
			BeginTxNum: number, EndTxNum: number}
	}
	oldChange := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "old-cold")
	newChange := binaryStateDomainChange(blocks[600].Number, blocks[600].BeginTxNum, 1, "new-cold")
	oldChange.BlockHash, newChange.BlockHash = blocks[5].Hash, blocks[600].Hash
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: blocks[0].BeginTxNum, ToTxNum: blocks[len(blocks)-1].EndTxNum,
		Path: stateDomainChangeHistorySegmentPath(blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum)}
	history, index, accessor, err := writeHistorySegmentFiles(dir, ref,
		[]*rawdb.StateDomainChange{oldChange, newChange}, ranges)
	if err != nil {
		t.Fatal(err)
	}
	manifest := NewManifestForChain(ref.FromTxNum, ref.ToTxNum,
		[]SegmentRef{history, index, accessor}, ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	oldMask := make([]bool, len(blocks))
	for i := 0; i <= 10; i++ {
		oldMask[i] = true
	}
	oldSpans, err := BuildHistoryStagingColdSpans(context.Background(), dir, manifest, blocks, oldMask)
	if err != nil {
		t.Fatal(err)
	}
	old := rawdb.HistoryStagingColdBinding{Version: 1, Bucket: 1, Epoch: 1, BindingEpoch: 1,
		ManifestEpoch: manifest.Generation, Spans: oldSpans}
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, block := range blocks {
		if err := rawdb.WriteStateTxRange(db, block.Number, block.Hash, block.BeginTxNum, block.EndTxNum); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{newChange}); err != nil {
		t.Fatal(err)
	}
	check := func(binding rawdb.HistoryStagingColdBinding) ([]rawdb.HistoryStagingColdSpan, error) {
		view, release, err := rawdb.AcquireStateHistoryReadView(db)
		if err != nil {
			return nil, err
		}
		defer release()
		return VerifyHistoryStagingMixedTargetColdEquivalence(context.Background(), view, dir, manifest, blocks, binding)
	}
	full, err := check(old)
	if err != nil || len(full) != 1 || full[0].From != blocks[0].Number || full[0].To != blocks[len(blocks)-1].Number || full[0].RowCount != 2 {
		t.Fatalf("mixed extension = %+v, %v", full, err)
	}
	wrongOld := old
	wrongOld.Spans = append([]rawdb.HistoryStagingColdSpan(nil), old.Spans...)
	wrongOld.Spans[0].SemanticHash[0] ^= 1
	if _, err := check(wrongOld); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("wrong pruned prefix = %v, want semantic mismatch", err)
	}
	changed := *newChange
	changed.Prev = []byte("changed-Prev")
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{&changed}); err != nil {
		t.Fatal(err)
	}
	if _, err := check(old); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("changed hot suffix = %v, want semantic mismatch", err)
	}
	if err := rawdb.DeleteStateDomainChanges(db, blocks[600].Number); err != nil {
		t.Fatal(err)
	}
	if _, err := check(old); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("missing hot suffix = %v, want semantic mismatch", err)
	}
}

func TestHistoryStagingSemanticDigestBindsSameTxRepairOrder(t *testing.T) {
	dir, _, blocks := historyStagingProofFixture(t)
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i, block := range blocks {
		ranges[i] = &rawdb.StateTxRange{BlockNum: block.Number, BlockHash: block.Hash,
			BeginTxNum: block.BeginTxNum, EndTxNum: block.EndTxNum}
	}
	a := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "same-key")
	a.BlockHash = blocks[5].Hash
	b := *a
	b.Seq = 2
	b.Prev = []byte("second-prev")
	c := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 3, "other-key")
	c.BlockHash = blocks[5].Hash
	c.Domain = kvdomains.SystemDelegation
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: blocks[0].BeginTxNum, ToTxNum: blocks[len(blocks)-1].EndTxNum,
		Path: stateDomainChangeHistorySegmentPath(blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum)}
	history, index, accessor, err := writeHistorySegmentFiles(dir, ref,
		[]*rawdb.StateDomainChange{a, &b, c}, ranges)
	if err != nil {
		t.Fatal(err)
	}
	manifest := NewManifestForChain(ref.FromTxNum, ref.ToTxNum,
		[]SegmentRef{history, index, accessor}, ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, block := range blocks {
		if err := rawdb.WriteStateTxRange(db, block.Number, block.Hash, block.BeginTxNum, block.EndTxNum); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{a, &b, c}); err != nil {
		t.Fatal(err)
	}
	check := func() error {
		view, release, err := rawdb.AcquireStateHistoryReadView(db)
		if err != nil {
			return err
		}
		defer release()
		_, err = VerifyHistoryStagingTargetColdEquivalence(context.Background(), view, dir, manifest, blocks)
		return err
	}
	if err := check(); err != nil {
		t.Fatalf("same TxNum/Key ordered repairs: %v", err)
	}
	swappedA, swappedB := *a, b
	swappedA.Prev, swappedB.Prev = b.Prev, a.Prev
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{&swappedA, &swappedB, c}); err != nil {
		t.Fatal(err)
	}
	if err := check(); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("same TxNum/Key swapped repair order = %v, want mismatch", err)
	}
}

func TestHistoryStagingColdProofMissingSubrangeAndZeroChange(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	needed := make([]bool, len(blocks))
	needed[4], needed[5], needed[6] = true, true, true
	prover, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	spans, err := prover.Build(context.Background(), blocks, needed)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 1 || spans[0].From != blocks[4].Number ||
		spans[0].To != blocks[6].Number || spans[0].RowCount != 1 ||
		spans[0].SemanticHash == ([32]byte{}) || spans[0].ContentID == ([32]byte{}) {
		t.Fatalf("invalid certified cold span: %+v", spans)
	}
	spansAgain, err := prover.Build(context.Background(), blocks, needed)
	if err != nil || len(spansAgain) != 1 || spansAgain[0] != spans[0] {
		t.Fatalf("cached proof changed: %+v, %v", spansAgain, err)
	}
	binding := rawdb.HistoryStagingColdBinding{Bucket: 1, Spans: spans}
	if err := VerifyHistoryStagingColdBinding(context.Background(), dir, manifest, binding, blocks); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryStagingPinnedBindingCacheHitAndFileReplacement(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	needed := make([]bool, len(blocks))
	needed[4], needed[5], needed[6] = true, true, true
	spans, err := BuildHistoryStagingColdSpans(context.Background(), dir, manifest, blocks, needed)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion,
		Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: manifest.Generation, Spans: spans}
	if err := pinned.VerifyHistoryStagingPinnedBindingReceipt(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	loads := 0
	load := func() ([]rawdb.HistoryStagingBlockProof, error) { loads++; return blocks, nil }
	for i := 0; i < 2; i++ {
		if err := pinned.VerifyHistoryStagingPinnedBinding(context.Background(), binding, load); err != nil {
			t.Fatal(err)
		}
	}
	if loads != 1 {
		t.Fatalf("cached proof loaded canonical txranges %d times", loads)
	}
	// A later manifest can append an unrelated immutable trio without forcing
	// every older binding to rebind or redo semantic scans.
	nextRanges := make([]*rawdb.StateTxRange, len(blocks))
	for i := range nextRanges {
		number := uint64(2048 + i)
		nextRanges[i] = &rawdb.StateTxRange{BlockNum: number,
			BlockHash:  common.Hash{byte(number >> 8), byte(number)},
			BeginTxNum: number, EndTxNum: number}
	}
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: 2048, ToTxNum: 3071, Path: stateDomainChangeHistorySegmentPath(2048, 3071)}
	newHistory, newIndex, newAccessor, err := writeHistorySegmentFiles(dir, ref, nil, nextRanges)
	if err != nil {
		t.Fatal(err)
	}
	later := *manifest
	later.Generation++
	later.Segments = append(append([]SegmentRef(nil), manifest.Segments...), newHistory, newIndex, newAccessor)
	later.VisibleTxEnd = 3071
	laterPinned, err := OpenPinnedManager(dir, &later)
	if err != nil {
		t.Fatal(err)
	}
	if err := laterPinned.VerifyHistoryStagingPinnedBindingReceipt(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	if err := laterPinned.VerifyHistoryStagingPinnedBinding(context.Background(), binding, load); err != nil {
		t.Fatal(err)
	}
	if loads != 1 {
		t.Fatalf("unrelated trio append reloaded old canonical proofs %d times", loads)
	}
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentHistory {
			continue
		}
		path := filepath.Join(dir, ref.Path)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data[8] ^= 0xff
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		break
	}
	if err := pinned.VerifyHistoryStagingPinnedBinding(context.Background(), binding, load); err == nil {
		t.Fatal("cached semantic proof accepted replaced/corrupt trio")
	}
	if err := pinned.VerifyHistoryStagingPinnedBindingReceipt(context.Background(), binding); err == nil {
		t.Fatal("receipt checksum cache accepted replaced/corrupt trio")
	}
}

func TestHistoryStagingColdProofRejectsWrongCanonicalAndIncompleteMask(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	needed := make([]bool, len(blocks))
	needed[5] = true
	blocks[5].Hash = common.Hash{0xff}
	if _, err := BuildHistoryStagingColdSpans(context.Background(), dir, manifest, blocks, needed); err == nil {
		t.Fatal("mismatched canonical hash accepted")
	}
	blocks[5].Hash = common.Hash{byte(blocks[5].Number >> 8), byte(blocks[5].Number)}
	if _, err := BuildHistoryStagingColdSpans(context.Background(), dir, manifest, blocks, needed[:10]); err == nil {
		t.Fatal("short cold mask accepted")
	}
}

func TestHistoryStagingColdProofRejectsTamperedTrio(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	needed := make([]bool, len(blocks))
	needed[5] = true
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentHistory {
			continue
		}
		path := filepath.Join(dir, ref.Path)
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteAt([]byte{0xff}, 8); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		break
	}
	if _, err := BuildHistoryStagingColdSpans(context.Background(), dir, manifest, blocks, needed); err == nil {
		t.Fatal("tampered history file was accepted")
	}
}

func TestHistoryStagingColdRebindTwoTriosToOne(t *testing.T) {
	dir := t.TempDir()
	blocks := make([]rawdb.HistoryStagingBlockProof, rawdb.StateHistoryChunkBucketBlocks)
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i := range blocks {
		number := uint64(i + 1024)
		hash := common.Hash{byte(number >> 8), byte(number)}
		blocks[i] = rawdb.HistoryStagingBlockProof{Number: number, Hash: hash,
			BeginTxNum: number, EndTxNum: number}
		ranges[i] = &rawdb.StateTxRange{BlockNum: number, BlockHash: hash,
			BeginTxNum: number, EndTxNum: number}
	}
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "early"),
		binaryStateDomainChange(blocks[600].Number, blocks[600].BeginTxNum, 1, "late"),
	}
	for _, change := range changes {
		change.BlockHash = blocks[int(change.BlockNum-1024)].Hash
	}
	write := func(start, end int, changes []*rawdb.StateDomainChange) []SegmentRef {
		t.Helper()
		ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
			FromTxNum: blocks[start].BeginTxNum, ToTxNum: blocks[end-1].EndTxNum,
			Path: stateDomainChangeHistorySegmentPath(blocks[start].BeginTxNum, blocks[end-1].EndTxNum)}
		history, index, accessor, err := writeHistorySegmentFiles(dir, ref, changes, ranges[start:end])
		if err != nil {
			t.Fatal(err)
		}
		return []SegmentRef{history, index, accessor}
	}
	oldRefs := append(write(0, 512, changes[:1]), write(512, 1024, changes[1:])...)
	chain := ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"}
	oldManifest := NewManifestForChain(blocks[0].BeginTxNum, blocks[1023].EndTxNum, oldRefs, chain)
	mask := make([]bool, len(blocks))
	for i := range mask {
		mask[i] = true
	}
	oldSpans, err := BuildHistoryStagingColdSpans(context.Background(), dir, oldManifest, blocks, mask)
	if err != nil || len(oldSpans) != 2 {
		t.Fatalf("old spans = %+v, %v", oldSpans, err)
	}
	mergedRefs := write(0, 1024, changes)
	merged := NewManifestForChain(blocks[0].BeginTxNum, blocks[1023].EndTxNum, mergedRefs, chain)
	binding := rawdb.HistoryStagingColdBinding{Version: 1, Bucket: 1, Epoch: 1,
		BindingEpoch: 1, Spans: oldSpans}
	prover, err := NewHistoryStagingColdProver(dir, merged)
	if err != nil {
		t.Fatal(err)
	}
	prover.EnableReaderReuse()
	defer prover.Close()
	updated, err := rebindHistoryStagingColdBinding(context.Background(), prover, binding, blocks)
	if err != nil {
		t.Fatal(err)
	}
	if updated.BindingEpoch != 2 || len(updated.Spans) != 2 ||
		updated.Spans[0].ContentID != updated.Spans[1].ContentID ||
		updated.Spans[0].ContentID == oldSpans[0].ContentID {
		t.Fatalf("merged binding incorrect: %+v", updated)
	}
	if err := VerifyHistoryStagingColdBinding(context.Background(), dir, merged, updated, blocks); err != nil {
		t.Fatalf("merged binding kept certified subrange boundaries: %v", err)
	}
	if stats := prover.Stats(); stats.SpanRecordWalks != 2 || stats.HistoryOpens != 1 || stats.ReaderReuses != 1 {
		t.Fatalf("rebind must walk each subrange once and reuse one trio: %+v", stats)
	}
	// A single old span may be split by a new manifest. Compare complete old
	// semantics over both new spans, including the many zero-change blocks.
	whole, err := BuildHistoryStagingColdSpans(context.Background(), dir, merged, blocks, mask)
	if err != nil || len(whole) != 1 {
		t.Fatalf("whole span: %+v, %v", whole, err)
	}
	splitBinding := binding
	splitBinding.Spans = whole
	splitProver, err := NewHistoryStagingColdProver(dir, oldManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer splitProver.Close()
	split, err := rebindHistoryStagingColdBinding(context.Background(), splitProver, splitBinding, blocks)
	if err != nil || !reflect.DeepEqual(split.Spans, oldSpans) || splitProver.Stats().SpanRecordWalks != 2 {
		t.Fatalf("one-to-two rebind: %+v, stats=%+v, %v", split, splitProver.Stats(), err)
	}
	unchanged, err := RebindHistoryStagingColdBinding(context.Background(), dir, oldManifest, binding, blocks)
	if err != nil || !reflect.DeepEqual(unchanged.Spans, oldSpans) {
		t.Fatalf("same-trio rebind: %+v, %v", unchanged, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if canceled, err := RebindHistoryStagingColdBinding(ctx, dir, merged, binding, blocks); !errors.Is(err, context.Canceled) || canceled.BindingEpoch != 0 || len(canceled.Spans) != 0 {
		t.Fatalf("canceled rebind returned a candidate: %+v, %v", canceled, err)
	}
	for _, fault := range []string{"cancel-between-spans", "replace-earlier-trio"} {
		t.Run(fault, func(t *testing.T) {
			p, err := NewHistoryStagingColdProver(dir, oldManifest)
			if err != nil {
				t.Fatal(err)
			}
			p.EnableReaderReuse()
			defer p.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := false
			ctx = maintenance.WithWorkCheckpoint(ctx, func(uint64) error {
				if injected || p.stats.SpanRecordWalks != 2 {
					return ctx.Err()
				}
				if fault == "cancel-between-spans" {
					injected = true
					cancel()
					return ctx.Err()
				}
				if p.open == nil || p.open.refs[0].Path != oldRefs[3].Path {
					return nil
				}
				injected = true
				path := filepath.Join(dir, oldRefs[0].Path)
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if err := os.WriteFile(path+".replacement", data, 0o600); err != nil {
					return err
				}
				return os.Rename(path+".replacement", path)
			})
			candidate, err := rebindHistoryStagingColdBinding(ctx, p, splitBinding, blocks)
			if !injected || err == nil || candidate.BindingEpoch != 0 || len(candidate.Spans) != 0 {
				t.Fatalf("fault returned a candidate: injected=%v candidate=%+v err=%v", injected, candidate, err)
			}
			if fault == "cancel-between-spans" && !errors.Is(err, context.Canceled) {
				t.Fatalf("wrong cancellation: %v", err)
			}
			if err := p.Close(); err != nil || p.open != nil {
				t.Fatalf("failed rebind retained reader: %v", err)
			}
		})
	}
	changed := *changes[1]
	changed.Prev = []byte("different logical history")
	badRefs := write(0, 1024, []*rawdb.StateDomainChange{changes[0], &changed})
	badManifest := NewManifestForChain(blocks[0].BeginTxNum, blocks[1023].EndTxNum, badRefs, chain)
	if _, err := RebindHistoryStagingColdBinding(context.Background(), dir, badManifest, binding, blocks); err == nil {
		t.Fatal("content-changed merge passed semantic rebind")
	}
}
