package snapshots

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func stagingReceiptRefs(t testing.TB, dir string, manifest *Manifest) [3]SegmentRef {
	t.Helper()
	p, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	refs, _, err := p.trio(p.refs[0])
	if err != nil {
		t.Fatal(err)
	}
	return refs
}

// Construct the known fixture's commitment independently of Build so these
// tests start with no full semantic authentication cached for this directory.
func stagingReceiptFixture(t *testing.T) (string, *Manifest, []rawdb.HistoryStagingBlockProof, rawdb.HistoryStagingColdBinding) {
	t.Helper()
	dir, manifest, blocks := historyStagingProofFixture(t)
	id, err := historyStagingTrioID(stagingReceiptRefs(t, dir, manifest))
	if err != nil {
		t.Fatal(err)
	}
	change := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "cold-prev")
	change.BlockHash = blocks[5].Hash
	txDigest, semantic := historyStagingSemanticDigest(blocks[4:7], [][32]byte{historyStagingRecordDigest(change, 0)})
	binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion,
		Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: manifest.Generation,
		Spans: []rawdb.HistoryStagingColdSpan{{From: blocks[4].Number, To: blocks[6].Number,
			ContentID: id, SemanticHash: semantic, TxRangeDigest: txDigest, RowCount: 1}}}
	return dir, manifest, blocks, binding
}

func TestHistoryStagingReceiptDoesNotPopulateFullProofCaches(t *testing.T) {
	dir, manifest, blocks, binding := stagingReceiptFixture(t)
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := pinned.VerifyHistoryStagingPinnedBindingReceipt(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	full, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	if len(full.verified) != 0 {
		t.Fatal("receipt installed a full proof in the worker")
	}
	if err := full.VerifyBinding(context.Background(), binding, blocks); err != nil {
		t.Fatal(err)
	}
	if full.Stats().FullTrioAuthenticates != 1 {
		t.Fatalf("receipt skipped complete trio authentication: %+v", full.Stats())
	}
	loads := 0
	if err := pinned.VerifyHistoryStagingPinnedBinding(context.Background(), binding, func() ([]rawdb.HistoryStagingBlockProof, error) {
		loads++
		return blocks, nil
	}); err != nil {
		t.Fatal(err)
	}
	if loads != 1 {
		t.Fatal("receipt populated RPC per-range proof cache")
	}
	// A nonzero but wrong persisted commitment is still rejected by RPC and
	// offline semantic proof, even after the physical receipt check succeeded.
	bad := binding
	bad.Spans = append([]rawdb.HistoryStagingColdSpan(nil), binding.Spans...)
	bad.Spans[0].SemanticHash[0] ^= 1
	if err := pinned.VerifyHistoryStagingPinnedBinding(context.Background(), bad, func() ([]rawdb.HistoryStagingBlockProof, error) { return blocks, nil }); err == nil {
		t.Fatal("RPC accepted incorrect semantic commitment")
	}
	if _, err := full.PrepareOfflineBinding(context.Background(), bad, blocks); err == nil {
		t.Fatal("offline certification accepted incorrect semantic commitment")
	}
}

func TestHistoryStagingFullProofDoesNotPopulateReceiptCache(t *testing.T) {
	dir, manifest, blocks, binding := stagingReceiptFixture(t)
	if err := VerifyHistoryStagingColdBinding(context.Background(), dir, manifest, binding, blocks); err != nil {
		t.Fatal(err)
	}
	calls := 0
	auth := historyStagingReceiptAuthenticator{dir: dir, manifest: manifest,
		testAuth: func(ctx context.Context, refs [3]SegmentRef) error {
			calls++
			return VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0])
		}}
	refs := stagingReceiptRefs(t, dir, manifest)
	for i := 0; i < 2; i++ {
		if err := auth.authenticate(context.Background(), refs, binding.Spans[0].ContentID); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("receipt should scan once independently of full cache, scanned %d times", calls)
	}
}

func TestHistoryStagingReceiptChecksumSuccessCannotHideMalformedSemantics(t *testing.T) {
	dir, manifest, blocks, binding := stagingReceiptFixture(t)
	// Give malformed index bytes a correct physical checksum and ContentID.
	// This deliberately lacks genuine prior durable semantic certification;
	// it tests that checksum success cannot contaminate a full proof cache.
	var ref *SegmentRef
	for i := range manifest.Segments {
		if manifest.Segments[i].Kind == SegmentInverted {
			ref = &manifest.Segments[i]
		}
	}
	path := filepath.Join(dir, ref.Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[0] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	ref.Size, ref.Checksum, err = stateDomainChangeBinaryFileMetadataContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	refs := stagingReceiptRefs(t, dir, manifest)
	binding.Spans[0].ContentID, err = historyStagingTrioID(refs)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := pinned.VerifyHistoryStagingPinnedBindingReceipt(context.Background(), binding); err != nil {
		t.Fatalf("physical checksum proof = %v", err)
	}
	full, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	if err := full.authenticate(context.Background(), refs, binding.Spans[0].ContentID); err == nil {
		t.Fatal("checksum receipt authorized a malformed index as fully authenticated")
	}
	if err := VerifyHistoryStagingColdBinding(context.Background(), dir, manifest, binding, blocks); err == nil {
		t.Fatal("offline full proof accepted malformed index")
	}
	if err := pinned.VerifyHistoryStagingPinnedBinding(context.Background(), binding, func() ([]rawdb.HistoryStagingBlockProof, error) { return blocks, nil }); err == nil {
		t.Fatal("RPC full proof accepted malformed index")
	}
}

func TestHistoryStagingReceiptRejectsChangedFiles(t *testing.T) {
	for _, kind := range []SegmentKind{SegmentHistory, SegmentInverted, SegmentAccessor} {
		for _, mutation := range []string{"tamper", "replace", "missing"} {
			t.Run(string(kind)+"/"+mutation, func(t *testing.T) {
				dir, manifest, _, binding := stagingReceiptFixture(t)
				pinned, err := OpenPinnedManager(dir, manifest)
				if err != nil {
					t.Fatal(err)
				}
				if err := pinned.VerifyHistoryStagingPinnedBindingReceipt(context.Background(), binding); err != nil {
					t.Fatal(err)
				}
				for _, ref := range manifest.Segments {
					if ref.Kind != kind {
						continue
					}
					path := filepath.Join(dir, ref.Path)
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					data[len(data)-1] ^= 1
					switch mutation {
					case "tamper":
						err = os.WriteFile(path, data, 0o600)
					case "replace":
						err = os.WriteFile(path+".replacement", data, info.Mode())
						if err == nil {
							err = os.Chtimes(path+".replacement", info.ModTime(), info.ModTime())
						}
						if err == nil {
							err = os.Rename(path+".replacement", path)
						}
					case "missing":
						err = os.Remove(path)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := pinned.VerifyHistoryStagingPinnedBindingReceipt(context.Background(), binding); err == nil {
					t.Fatal("receipt accepted changed or missing file after cache hit")
				}
			})
		}
	}
}

func TestHistoryStagingReceiptAuthenticationFailureIsNotCached(t *testing.T) {
	for _, failure := range []string{"error", "cancel", "replace"} {
		t.Run(failure, func(t *testing.T) {
			dir, manifest, _, binding := stagingReceiptFixture(t)
			refs := stagingReceiptRefs(t, dir, manifest)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			auth := historyStagingReceiptAuthenticator{dir: dir, manifest: manifest}
			auth.testAuth = func(ctx context.Context, refs [3]SegmentRef) error {
				calls++
				if err := VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0]); err != nil {
					return err
				}
				if calls == 1 {
					switch failure {
					case "error":
						return errors.New("injected read failure")
					case "cancel":
						cancel() // even a verifier returning nil after cancellation cannot cache success
					case "replace":
						path := filepath.Join(dir, refs[0].Path)
						data, err := os.ReadFile(path)
						if err != nil {
							return err
						}
						if err := os.WriteFile(path+".replacement", data, 0o600); err != nil {
							return err
						}
						return os.Rename(path+".replacement", path)
					}
				}
				return nil
			}
			if err := auth.authenticate(ctx, refs, binding.Spans[0].ContentID); err == nil {
				t.Fatal("failed/cancelled/changed authentication succeeded")
			}
			if err := auth.authenticate(context.Background(), refs, binding.Spans[0].ContentID); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatalf("failure cached as success, calls=%d", calls)
			}
		})
	}
}

func TestHistoryStagingReceiptAuthenticationCancelledFlight(t *testing.T) {
	for _, cancelOwner := range []bool{false, true} {
		t.Run(map[bool]string{false: "waiter", true: "owner"}[cancelOwner], func(t *testing.T) {
			dir, manifest, _, binding := stagingReceiptFixture(t)
			refs := stagingReceiptRefs(t, dir, manifest)
			started, waiting, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			ownerCtx, cancelOwnerCtx := context.WithCancel(context.Background())
			defer cancelOwnerCtx()
			waiterCtx, cancelWaiter := context.WithCancel(context.Background())
			defer cancelWaiter()
			owner := historyStagingReceiptAuthenticator{dir: dir, manifest: manifest, testAuth: func(ctx context.Context, refs [3]SegmentRef) error {
				close(started)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-release:
					return VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0])
				}
			}}
			var once sync.Once
			waiterCalls := 0
			waiter := historyStagingReceiptAuthenticator{dir: dir, manifest: manifest,
				testWait: func() { once.Do(func() { close(waiting) }) },
				testAuth: func(ctx context.Context, refs [3]SegmentRef) error {
					waiterCalls++
					return VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0])
				}}
			ownerResult, waiterResult := make(chan error, 1), make(chan error, 1)
			go func() { ownerResult <- owner.authenticate(ownerCtx, refs, binding.Spans[0].ContentID) }()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("owner did not start")
			}
			go func() { waiterResult <- waiter.authenticate(waiterCtx, refs, binding.Spans[0].ContentID) }()
			select {
			case <-waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("waiter did not wait")
			}
			if cancelOwner {
				cancelOwnerCtx()
			} else {
				cancelWaiter()
			}
			if cancelOwner {
				if err := <-ownerResult; !errors.Is(err, context.Canceled) {
					t.Fatalf("owner cancellation: %v", err)
				}
				if err := <-waiterResult; err != nil || waiterCalls != 1 {
					t.Fatalf("waiter did not retry failed flight: %v, calls=%d", err, waiterCalls)
				}
			} else {
				if err := <-waiterResult; !errors.Is(err, context.Canceled) {
					t.Fatalf("waiter cancellation: %v", err)
				}
				close(release)
				if err := <-ownerResult; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestHistoryStagingReceiptAndFullProofFlightsAreIndependent(t *testing.T) {
	for _, receiptOwner := range []bool{false, true} {
		t.Run(map[bool]string{false: "full_owner", true: "receipt_owner"}[receiptOwner], func(t *testing.T) {
			dir, manifest, _, binding := stagingReceiptFixture(t)
			refs := stagingReceiptRefs(t, dir, manifest)
			full, err := NewHistoryStagingColdProver(dir, manifest)
			if err != nil {
				t.Fatal(err)
			}
			receipt := &historyStagingReceiptAuthenticator{dir: dir, manifest: manifest}
			started, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			if receiptOwner {
				receipt.testAuth = func(ctx context.Context, refs [3]SegmentRef) error {
					close(started)
					<-release
					return VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0])
				}
			} else {
				full.testAuth = func(ctx context.Context, refs [3]SegmentRef) error {
					close(started)
					<-release
					return VerifyHistorySegmentWithCompanionsContext(ctx, dir, manifest, refs[0])
				}
			}
			ownerResult, independentResult := make(chan error, 1), make(chan error, 1)
			t.Cleanup(func() {
				select {
				case err := <-ownerResult:
					if err != nil {
						t.Errorf("owner authentication: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("owner authentication did not finish")
				}
			})
			go func() {
				if receiptOwner {
					ownerResult <- receipt.authenticate(context.Background(), refs, binding.Spans[0].ContentID)
				} else {
					ownerResult <- full.authenticate(context.Background(), refs, binding.Spans[0].ContentID)
				}
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("owner authentication did not start")
			}
			go func() {
				if receiptOwner {
					independentResult <- full.authenticate(context.Background(), refs, binding.Spans[0].ContentID)
				} else {
					independentResult <- receipt.authenticate(context.Background(), refs, binding.Spans[0].ContentID)
				}
			}()
			select {
			case err := <-independentResult:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("receipt and full proof incorrectly share an in-flight audit")
			}
		})
	}
}

func TestHistoryStagingReceiptCancellationDuringPhysicalChecksum(t *testing.T) {
	dir, manifest, _ := stagingDenseColdFixture(t, "v5", 8)
	refs := stagingReceiptRefs(t, dir, manifest)
	id, err := historyStagingTrioID(refs)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	auth := &historyStagingReceiptAuthenticator{dir: dir, manifest: manifest,
		testAuth: func(ctx context.Context, refs [3]SegmentRef) error {
			calls++
			if calls == 1 {
				// Interrupt real streaming SHA reads, after entering the strong
				// verifier, rather than cancelling only before authentication.
				ctx = historyCancelAfterChecks(t, 4)
			}
			return VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0])
		}}
	if err := auth.authenticate(context.Background(), refs, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("physical checksum cancellation: %v", err)
	}
	if err := auth.authenticate(context.Background(), refs, id); err != nil || calls != 2 {
		t.Fatalf("cancelled checksum cached or retry failed: calls=%d, err=%v", calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := auth.authenticate(ctx, refs, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached checksum ignored cancellation: %v", err)
	}
}

func TestHistoryStagingReceiptReusesChecksumAcrossBuckets(t *testing.T) {
	dir, manifest, blocks := historyStagingTwoBucketProofFixture(t)
	p, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	mask := make([]bool, rawdb.StateHistoryChunkBucketBlocks)
	for i := range mask {
		mask[i] = true
	}
	calls := 0
	auth := &historyStagingReceiptAuthenticator{dir: dir, manifest: manifest,
		testAuth: func(ctx context.Context, refs [3]SegmentRef) error {
			calls++
			return VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0])
		}}
	for offset := 0; offset < len(blocks); offset += len(mask) {
		bucket := blocks[offset : offset+len(mask)]
		spans, err := p.Build(context.Background(), bucket, mask)
		if err != nil {
			t.Fatal(err)
		}
		binding := rawdb.HistoryStagingColdBinding{Bucket: bucket[0].Number / rawdb.StateHistoryChunkBucketBlocks, Spans: spans}
		if err := p.verifyBindingReceiptFiles(context.Background(), binding, auth); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("shared trio rescanned across buckets: %d physical audits", calls)
	}
}

func TestHistoryStagingReceiptRechecksWholeBindingAcrossTrios(t *testing.T) {
	dir := t.TempDir()
	var segments []SegmentRef
	var spans []rawdb.HistoryStagingColdSpan
	for first := uint64(1024); first < 2048; first += 512 {
		ranges := make([]*rawdb.StateTxRange, 512)
		blocks := make([]rawdb.HistoryStagingBlockProof, 512)
		for i := range ranges {
			number := first + uint64(i)
			hash := common.Hash{byte(number >> 8), byte(number)}
			ranges[i] = &rawdb.StateTxRange{BlockNum: number, BlockHash: hash, BeginTxNum: number, EndTxNum: number}
			blocks[i] = rawdb.HistoryStagingBlockProof{Number: number, Hash: hash, BeginTxNum: number, EndTxNum: number}
		}
		ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
			FromTxNum: first, ToTxNum: first + 511, Path: stateDomainChangeHistorySegmentPath(first, first+511)}
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
		spans = append(spans, rawdb.HistoryStagingColdSpan{From: first, To: first + 511,
			ContentID: id, TxRangeDigest: txDigest, SemanticHash: semantic})
	}
	manifest := NewManifestForChain(1024, 2047, segments, ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	p, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	firstPath := filepath.Join(dir, segments[0].Path)
	calls := 0
	auth := &historyStagingReceiptAuthenticator{dir: dir, manifest: manifest,
		testAuth: func(ctx context.Context, refs [3]SegmentRef) error {
			calls++
			if err := VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, refs[0]); err != nil {
				return err
			}
			if calls == 2 {
				// Replace the already authenticated first trio while the second
				// succeeds. Per-trio before/after checks alone cannot detect this.
				data, err := os.ReadFile(firstPath)
				if err != nil {
					return err
				}
				if err := os.WriteFile(firstPath+".replacement", data, 0o600); err != nil {
					return err
				}
				return os.Rename(firstPath+".replacement", firstPath)
			}
			return nil
		}}
	if err := p.verifyBindingReceiptFiles(context.Background(), rawdb.HistoryStagingColdBinding{Bucket: 1, Spans: spans}, auth); err == nil {
		t.Fatal("whole binding accepted replacement of an earlier trio")
	}
	if calls != 2 {
		t.Fatalf("did not reach the second trio: calls=%d", calls)
	}
}

func TestHistoryStagingReceiptRejectsInvalidMetadata(t *testing.T) {
	mutations := map[string]func(*rawdb.HistoryStagingColdBinding){
		"version":        func(b *rawdb.HistoryStagingColdBinding) { b.Version++ },
		"epoch":          func(b *rawdb.HistoryStagingColdBinding) { b.Epoch = 0 },
		"binding epoch":  func(b *rawdb.HistoryStagingColdBinding) { b.BindingEpoch = 0 },
		"manifest epoch": func(b *rawdb.HistoryStagingColdBinding) { b.ManifestEpoch++ },
		"coverage":       func(b *rawdb.HistoryStagingColdBinding) { b.Spans[0].From = 1023 },
		"overlap":        func(b *rawdb.HistoryStagingColdBinding) { b.Spans = append(b.Spans, b.Spans[0]) },
		"content id":     func(b *rawdb.HistoryStagingColdBinding) { b.Spans[0].ContentID[0] ^= 1 },
		"semantic hash":  func(b *rawdb.HistoryStagingColdBinding) { b.Spans[0].SemanticHash = [32]byte{} },
		"txrange digest": func(b *rawdb.HistoryStagingColdBinding) { b.Spans[0].TxRangeDigest = [32]byte{} },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			dir, manifest, _, binding := stagingReceiptFixture(t)
			pinned, err := OpenPinnedManager(dir, manifest)
			if err != nil {
				t.Fatal(err)
			}
			mutate(&binding)
			if err := pinned.VerifyHistoryStagingPinnedBindingReceipt(context.Background(), binding); err == nil {
				t.Fatal("invalid durable receipt accepted")
			}
		})
	}
}

func TestHistoryStagingReceiptRequiresStrongChecksumForEveryFile(t *testing.T) {
	for _, kind := range []SegmentKind{SegmentHistory, SegmentInverted, SegmentAccessor} {
		for _, checksum := range []string{"", "sha256:bad", "size:42"} {
			t.Run(string(kind)+"/"+checksum, func(t *testing.T) {
				dir, manifest, _, binding := stagingReceiptFixture(t)
				refs := stagingReceiptRefs(t, dir, manifest)
				auth := &historyStagingReceiptAuthenticator{dir: dir, manifest: manifest}
				// First populate the checksum cache: malformed metadata must
				// still reject rather than trust an earlier success for the ID.
				if err := auth.authenticate(context.Background(), refs, binding.Spans[0].ContentID); err != nil {
					t.Fatal(err)
				}
				for i := range refs {
					if refs[i].Kind == kind {
						refs[i].Checksum = checksum
					}
				}
				if err := auth.authenticate(context.Background(), refs, binding.Spans[0].ContentID); err == nil {
					t.Fatal("weak checksum accepted after cache success")
				}
			})
		}
	}
}

func BenchmarkHistoryStagingStartupTrioAuthentication(b *testing.B) {
	for _, format := range []string{"v5", "v6", "reference"} {
		b.Run(format, func(b *testing.B) {
			dir, manifest, _ := stagingDenseColdFixture(b, format, 8)
			refs := stagingReceiptRefs(b, dir, manifest)
			var size int64
			for _, ref := range refs {
				size += int64(ref.Size)
			}
			for _, full := range []bool{false, true} {
				name := map[bool]string{false: "receipt_checksums", true: "full_semantics"}[full]
				b.Run(name, func(b *testing.B) {
					b.SetBytes(size)
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						verify := VerifyHistorySegmentCompanionChecksumsContext
						if full {
							verify = VerifyHistorySegmentWithCompanionsContext
						}
						if err := verify(context.Background(), dir, manifest, refs[0]); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
