package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func historyVerificationSHAFixture(t testing.TB, dir, name string, shift uint64) []SegmentRef {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	for n := uint64(1); n <= 512; n++ {
		var hash common.Hash
		binary.BigEndian.PutUint64(hash[24:], n)
		if err := rawdb.WriteStateTxRange(db, n, hash, n, n); err != nil {
			t.Fatal(err)
		}
		row := &rawdb.StateDomainChange{
			BlockNum: n, BlockHash: hash, TxNum: n, Seq: 1,
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, make([]byte, common.AccountIDLength)...)),
			Domain:     kvdomains.ContractStorage, Key: []byte{byte((n + shift) % 8)},
			PrevExists: true, Prev: bytes.Repeat([]byte{byte(n)}, 100),
		}
		if err := rawdb.WriteStateDomainChange(db, row); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 512, name+"/state-domain-change-1-512.seg")
	if err != nil {
		t.Fatal(err)
	}
	return refs
}

func historyVerificationSHARefs(t testing.TB, refs []SegmentRef) (history, index, accessor SegmentRef) {
	t.Helper()
	for _, ref := range refs {
		switch ref.Kind {
		case SegmentHistory:
			history = ref
		case SegmentInverted:
			index = ref
		case SegmentAccessor:
			accessor = ref
		}
	}
	if history.Path == "" || index.Path == "" || accessor.Path == "" {
		t.Fatal("fixture is not one complete history trio")
	}
	return
}

func TestHistoryV7JointVerificationPreservesAuthenticationAndLayout(t *testing.T) {
	for _, variant := range []string{"valid", "bad-sha", "bad-size", "rehash-bad-header", "rehash-posting-tail", "canceled"} {
		t.Run(variant, func(t *testing.T) {
			dir := t.TempDir()
			history, index, accessor := historyVerificationSHARefs(t, historyVerificationSHAFixture(t, dir, "source", 0))
			path := filepath.Join(dir, accessor.Path)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeStateDomainChangeBinaryAccessorV7Header(bytes.NewReader(raw), uint64(len(raw))); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			switch variant {
			case "bad-sha":
				raw[len(raw)-1] ^= 1
			case "bad-size":
				accessor.Size++
			case "rehash-bad-header":
				raw[116] = 1 // Reserved header byte; valid SHA must not bypass layout.
			case "rehash-posting-tail":
				raw = append(raw, 0)
				binary.BigEndian.PutUint64(raw[104:112], binary.BigEndian.Uint64(raw[104:112])+1)
				binary.BigEndian.PutUint32(raw[112:116], crc32.ChecksumIEEE(raw[:112]))
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(variant, "rehash-") {
				sum := sha256.Sum256(raw)
				accessor.Size, accessor.Checksum = uint64(len(raw)), "sha256:"+hex.EncodeToString(sum[:])
				if err := checkStateDomainChangeBinaryAccessorChecksumContext(ctx, dir, accessor); err != nil {
					t.Fatalf("rehashed fixture must pass physical authentication: %v", err)
				}
			}
			err = verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx, dir, history, index, accessor)
			if variant == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("invalid trio passed joint gate")
			} else if variant == "bad-sha" && !strings.Contains(err.Error(), "checksum") {
				t.Fatalf("expected physical checksum rejection: %v", err)
			} else if variant == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
		})
	}
}

func TestHistoryV7JointVerificationStillComparesEveryPosting(t *testing.T) {
	dir := t.TempDir()
	history, index, _ := historyVerificationSHARefs(t, historyVerificationSHAFixture(t, dir, "source", 0))
	_, _, otherAccessor := historyVerificationSHARefs(t, historyVerificationSHAFixture(t, dir, "different-postings", 1))
	// Same dictionary, tx range, row count and valid standalone structure. Only
	// the association of each key with a transaction differs from the history.
	if err := CheckStateDomainChangeAccessorSegment(dir, otherAccessor); err != nil {
		t.Fatal(err)
	}
	err := verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(context.Background(), dir, history, index, otherAccessor)
	if err == nil || !strings.Contains(err.Error(), "posting/history mismatch") {
		t.Fatalf("expected exact tuple rejection, got %v", err)
	}
}

type historyVerificationReadCounter struct {
	r     *bytes.Reader
	bytes int64
}

func (r *historyVerificationReadCounter) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.bytes += int64(n)
	return n, err
}

func (r *historyVerificationReadCounter) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.r.ReadAt(p, off)
	r.bytes += int64(n)
	return n, err
}

func TestHistoryV7RedundantSHAReadBytes(t *testing.T) {
	dir := t.TempDir()
	_, _, accessor := historyVerificationSHARefs(t, historyVerificationSHAFixture(t, dir, "source", 0))
	raw, err := os.ReadFile(filepath.Join(dir, accessor.Path))
	if err != nil {
		t.Fatal(err)
	}
	// Instrument the two constituent passes without introducing an unsafe
	// process-global filesystem hook. These are caller-read bytes, not device IO.
	measure := func(extraSHA bool) int64 {
		r := &historyVerificationReadCounter{r: bytes.NewReader(raw)}
		if extraSHA {
			h := sha256.New()
			if _, err := io.Copy(h, r); err != nil {
				t.Fatal(err)
			}
			if got := "sha256:" + hex.EncodeToString(h.Sum(nil)); got != accessor.Checksum {
				t.Fatalf("measured SHA %s, want %s", got, accessor.Checksum)
			}
		}
		if err := checkStateDomainChangeBinaryAccessorV7(r, uint64(len(raw))); err != nil {
			t.Fatal(err)
		}
		return r.bytes
	}
	layoutReads, oldReads := measure(false), measure(true)
	if layoutReads == 0 || oldReads-layoutReads != int64(accessor.Size) {
		t.Fatalf("read bytes old=%d layout=%d, want exactly %d fewer", oldReads, layoutReads, accessor.Size)
	}
}

// baseline repeats the physical accessor hash that the old joint gate ran in
// its standalone accessor check. Valid-input workload is otherwise identical;
// placement of that extra pass is before the joint gate, not a copy of old code.
// This is test-only: there is no production flag weakening authentication.
func benchmarkHistoryV7JointVerification(ctx context.Context, dir string, history, index, accessor SegmentRef, baseline bool) error {
	if baseline {
		if err := checkStateDomainChangeBinaryAccessorChecksumContext(ctx, dir, accessor); err != nil {
			return err
		}
	}
	return verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx, dir, history, index, accessor)
}

func BenchmarkHistoryV7JointVerificationSHA(b *testing.B) {
	dir := b.TempDir()
	history, index, accessor := historyVerificationSHARefs(b, historyVerificationSHAFixture(b, dir, "source", 0))
	benchmarkHistoryV7JointVerificationModes(b, dir, history, index, accessor)
}

// Set GTRON_HISTORY_VERIFY_FIXTURE to a private, immutable fixture with a
// manifest. Files are hard-linked into a separate temp directory so verification
// ETL never writes to the fixture and no chain database is opened. Multiple trios
// are deliberately rejected: this benchmark compares one exact workload.
func BenchmarkHistoryV7JointVerificationSHAProduction(b *testing.B) {
	fixture := os.Getenv("GTRON_HISTORY_VERIFY_FIXTURE")
	if fixture == "" {
		b.Skip("GTRON_HISTORY_VERIFY_FIXTURE is not set")
	}
	manifest, err := LoadManifest(fixture)
	if err != nil {
		b.Fatal(err)
	}
	if len(manifest.Segments) != 3 {
		b.Fatal("fixture requires exactly one complete trio")
	}
	history, index, accessor := historyVerificationSHARefs(b, manifest.Segments)
	dir := b.TempDir()
	for _, ref := range manifest.Segments {
		target := filepath.Join(dir, ref.Path)
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			b.Fatal(err)
		}
		if err := os.Link(filepath.Join(fixture, ref.Path), target); err != nil {
			b.Fatal(err)
		}
	}
	benchmarkHistoryV7JointVerificationModes(b, dir, history, index, accessor)
}

func benchmarkHistoryV7JointVerificationModes(b *testing.B, dir string, history, index, accessor SegmentRef) {
	r, h, _, err := openStateDomainChangeBinaryAccessorReader(dir, accessor)
	if err != nil {
		b.Fatal(err)
	}
	if err := r.Close(); err != nil {
		b.Fatal(err)
	}
	if h.version != stateDomainChangeBinaryVersionV7 {
		b.Fatal("benchmark requires a V7 accessor")
	}
	for _, tc := range []struct {
		name     string
		baseline bool
	}{{"baseline_extra_sha", true}, {"single_sha", false}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(accessor.Size), "accessor-B")
			for i := 0; i < b.N; i++ {
				if err := benchmarkHistoryV7JointVerification(context.Background(), dir, history, index, accessor, tc.baseline); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
