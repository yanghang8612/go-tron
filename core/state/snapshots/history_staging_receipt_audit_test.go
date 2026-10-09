package snapshots

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestHistoryStagingReceiptAuditReusesOneTrioAcrossBuckets(t *testing.T) {
	dir, manifest, blocks := historyStagingTwoBucketProofFixture(t)
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := NewHistoryStagingReceiptAudit(pinned)
	if err != nil {
		t.Fatal(err)
	}
	refs := stagingReceiptRefs(t, dir, manifest)
	id, err := historyStagingTrioID(refs)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	audit.auth.testAuth = func(ctx context.Context, refs [3]SegmentRef) error {
		calls++
		return VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0])
	}
	for bucket := uint64(1); bucket <= 2; bucket++ {
		first := blocks[(bucket-1)*rawdb.StateHistoryChunkBucketBlocks : bucket*rawdb.StateHistoryChunkBucketBlocks]
		change := binaryStateDomainChange(first[5].Number, first[5].BeginTxNum, 1, "cold-prev")
		change.BlockHash = first[5].Hash
		txDigest, semantic := historyStagingSemanticDigest(first, [][32]byte{historyStagingRecordDigest(change, 0)})
		binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion,
			Bucket: bucket, Epoch: 1, BindingEpoch: 1, ManifestEpoch: manifest.Generation,
			Spans: []rawdb.HistoryStagingColdSpan{{From: first[0].Number, To: first[len(first)-1].Number,
				ContentID: id, SemanticHash: semantic, TxRangeDigest: txDigest, RowCount: 1}}}
		if err := audit.VerifyBinding(context.Background(), binding); err != nil {
			t.Fatalf("bucket %d: %v", bucket, err)
		}
		if bucket == 1 {
			// A long startup can evict this entry from the bounded process cache.
			// The pinned task must keep its first successful authentication.
			key := sha256.Sum256(append(append([]byte("gtron-history-staging-receipt-checksum-v1\x00"), dir...), id[:]...))
			historyStagingReceiptChecksumCache.Lock()
			_, cached := historyStagingReceiptChecksumCache.trios[key]
			delete(historyStagingReceiptChecksumCache.trios, key)
			historyStagingReceiptChecksumCache.Unlock()
			if !cached {
				t.Fatal("first bucket did not populate process receipt cache")
			}
		}
	}
	if calls != 1 || audit.AuthenticatedTrios() != 1 {
		t.Fatalf("shared trio physically audited %d times, retained %d unique trios", calls, audit.AuthenticatedTrios())
	}
	if err := audit.RecheckAll(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryStagingReceiptAuditRejectsCancellationBeforeReady(t *testing.T) {
	dir, manifest, _, binding := stagingReceiptFixture(t)
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := NewHistoryStagingReceiptAudit(pinned)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := audit.VerifyBinding(ctx, binding); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled receipt audit = %v", err)
	}
	if audit.AuthenticatedTrios() != 0 {
		t.Fatal("canceled receipt installed task-local authenticated trio")
	}
	if err := audit.RecheckAll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled final recheck = %v", err)
	}
}

func TestHistoryStagingReceiptAuditRechecksEarlierBucketAfterLaterAuthentication(t *testing.T) {
	dir := t.TempDir()
	var segments []SegmentRef
	var bindings []rawdb.HistoryStagingColdBinding
	for bucket := uint64(1); bucket <= 2; bucket++ {
		first := bucket * rawdb.StateHistoryChunkBucketBlocks
		ranges := make([]*rawdb.StateTxRange, rawdb.StateHistoryChunkBucketBlocks)
		blocks := make([]rawdb.HistoryStagingBlockProof, len(ranges))
		for i := range ranges {
			number := first + uint64(i)
			hash := common.Hash{byte(number >> 8), byte(number)}
			ranges[i] = &rawdb.StateTxRange{BlockNum: number, BlockHash: hash, BeginTxNum: number, EndTxNum: number}
			blocks[i] = rawdb.HistoryStagingBlockProof{Number: number, Hash: hash, BeginTxNum: number, EndTxNum: number}
		}
		ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
			FromTxNum: first, ToTxNum: first + rawdb.StateHistoryChunkBucketBlocks - 1,
			Path: stateDomainChangeHistorySegmentPath(first, first+rawdb.StateHistoryChunkBucketBlocks-1)}
		history, index, accessor, err := writeHistorySegmentFiles(dir, ref, nil, ranges)
		if err != nil {
			t.Fatal(err)
		}
		segments = append(segments, history, index, accessor)
		id, err := historyStagingTrioID([3]SegmentRef{history, index, accessor})
		if err != nil {
			t.Fatal(err)
		}
		txDigest, semantic := historyStagingSemanticDigest(blocks, nil)
		bindings = append(bindings, rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion,
			Bucket: bucket, Epoch: 1, BindingEpoch: 1, ManifestEpoch: 1,
			Spans: []rawdb.HistoryStagingColdSpan{{From: first, To: ref.ToTxNum, ContentID: id,
				SemanticHash: semantic, TxRangeDigest: txDigest}}})
	}
	manifest := NewManifestForChain(1024, 3071, segments, ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	for i := range bindings {
		bindings[i].ManifestEpoch = manifest.Generation
	}
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := NewHistoryStagingReceiptAudit(pinned)
	if err != nil {
		t.Fatal(err)
	}
	firstPath := filepath.Join(dir, segments[0].Path)
	calls := 0
	audit.auth.testAuth = func(ctx context.Context, refs [3]SegmentRef) error {
		calls++
		if err := VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0]); err != nil {
			return err
		}
		if calls != 2 {
			return nil
		}
		data, err := os.ReadFile(firstPath)
		if err != nil {
			return err
		}
		if err := os.WriteFile(firstPath+".replacement", data, 0o600); err != nil {
			return err
		}
		return os.Rename(firstPath+".replacement", firstPath)
	}
	for _, binding := range bindings {
		if err := audit.VerifyBinding(context.Background(), binding); err != nil {
			t.Fatalf("bucket %d: %v", binding.Bucket, err)
		}
	}
	if calls != 2 || audit.AuthenticatedTrios() != 2 {
		t.Fatalf("physical audits=%d retained=%d, want 2 each", calls, audit.AuthenticatedTrios())
	}
	if err := audit.RecheckAll(context.Background()); err == nil {
		t.Fatal("later bucket replaced previously authenticated trio without blocking readiness")
	}
}

func TestHistoryStagingReceiptAuditRechecksEveryAuthenticatedTrio(t *testing.T) {
	dir, manifest, _, binding := stagingReceiptFixture(t)
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := NewHistoryStagingReceiptAudit(pinned)
	if err != nil {
		t.Fatal(err)
	}
	if err := audit.VerifyBinding(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	if err := audit.VerifyBinding(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	if audit.AuthenticatedTrios() != 1 {
		t.Fatalf("expected one task-local authenticated trio, got %d", audit.AuthenticatedTrios())
	}
	if err := audit.RecheckAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var changed string
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentHistory && ref.Dataset == SegmentDatasetStateDomainChange {
			changed = filepath.Join(dir, ref.Path)
			break
		}
	}
	if changed == "" {
		t.Fatal("fixture has no cold history file")
	}
	data, err := os.ReadFile(changed)
	if err != nil {
		t.Fatal(err)
	}
	data[8] ^= 0xff
	if err := os.WriteFile(changed, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := audit.RecheckAll(context.Background()); err == nil {
		t.Fatal("changed authenticated trio passed final fingerprint check")
	}
	if err := audit.VerifyBinding(context.Background(), binding); err == nil {
		t.Fatal("changed trio passed task-local receipt reuse")
	}
}

func TestHistoryStagingReceiptAuditRequiresPinnedManager(t *testing.T) {
	if _, err := NewHistoryStagingReceiptAudit(nil); err == nil {
		t.Fatal("nil manager accepted")
	}
	var audit *HistoryStagingReceiptAudit
	if err := audit.VerifyBinding(context.Background(), rawdb.HistoryStagingColdBinding{}); err == nil {
		t.Fatal("nil audit accepted")
	}
}
