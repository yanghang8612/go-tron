package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type v9CountedReader struct {
	*bytes.Reader
	calls int
}

func (r *v9CountedReader) ReadAt(p []byte, off int64) (int, error) {
	r.calls++
	return r.Reader.ReadAt(p, off)
}

func TestHistoryOffsetPagesBoundariesAndCorruption(t *testing.T) {
	for _, count := range []uint64{0, 1, 127, 128, 129, 4097} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			b, err := newHistoryAccessorOffsetBuilder(t.TempDir(), "test")
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			for i := uint64(0); i < count; i++ {
				if err := b.Add(i, 100+i*37); err != nil {
					t.Fatal(err)
				}
			}
			var out bytes.Buffer
			if err := b.AppendTo(&out, count); err != nil {
				t.Fatal(err)
			}
			h := stateDomainChangeBinaryAccessorV6Header{version: stateDomainChangeBinaryAccessorVersionV9, recordCount: count}
			r := &v9CountedReader{Reader: bytes.NewReader(out.Bytes())}
			if err := decodeHistoryOffsetFooter(r, uint64(out.Len()), 0, &h); err != nil {
				t.Fatal(err)
			}
			if err := checkHistoryAccessorOffsets(r, h); err != nil {
				t.Fatal(err)
			}
			lookup := newHistoryAccessorOffsetResolver(r, h)
			for i := uint64(0); i < count; i++ {
				before := r.calls
				off, err := lookup.Offset(uint32(i))
				if err != nil || off != 100+i*37 {
					t.Fatal(i, off, err)
				}
				wantReads := 0
				if i%128 == 0 {
					wantReads = 2
				}
				if r.calls-before != wantReads {
					t.Fatalf("ordinal %d reads=%d want=%d", i, r.calls-before, wantReads)
				}
			}
			if _, err := lookup.Offset(uint32(count)); err == nil {
				t.Fatal("accepted out-of-range ordinal")
			}
			if count > 0 {
				bad := append([]byte(nil), out.Bytes()...)
				bad[0] ^= 1
				if err := checkHistoryAccessorOffsets(bytes.NewReader(bad), h); err == nil {
					t.Fatal("accepted corrupt payload")
				}
				bad = append([]byte(nil), out.Bytes()...)
				bad[h.offsetDirectory] ^= 1
				if err := checkHistoryAccessorOffsets(bytes.NewReader(bad), h); err == nil {
					t.Fatal("accepted corrupt directory")
				}
			}
			for _, cut := range []int{1, 8, 31} {
				if err := decodeHistoryOffsetFooter(bytes.NewReader(out.Bytes()[:out.Len()-cut]), uint64(out.Len()-cut), 0, &h); err == nil {
					t.Fatal("accepted truncated footer")
				}
			}
		})
	}
}

func TestStateDomainChangeV9MixedPostingRoundTrip(t *testing.T) {
	data, expected := buildAccessorVersionFixture(t, stateDomainChangeBinaryAccessorVersionV9, 4200)
	if err := checkStateDomainChangeBinaryAccessorV7(bytes.NewReader(data), uint64(len(data))); err != nil {
		t.Fatal(err)
	}
	cursor, err := newStateDomainChangeBinaryAccessorV7PostingCursor(bytes.NewReader(data), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for id, key := range expected {
		for _, want := range key.rows {
			gotID, got, ok, err := cursor.Next()
			if err != nil || !ok || gotID != uint32(id) || got != want {
				t.Fatal(id, gotID, got, want, err)
			}
		}
	}
	if _, _, ok, err := cursor.Next(); err != nil || ok {
		t.Fatal(ok, err)
	}
	// A V9 frame keeps exactly the TxNum/ordinal values and omits offset bytes.
	posts := expected[3].rows[:128]
	old, err := encodeStateDomainChangeBinaryAccessorV7PostingList(10, posts)
	if err != nil {
		t.Fatal(err)
	}
	packed, err := encodeStateDomainChangeBinaryAccessorPostingListVersion(10, posts, stateDomainChangeBinaryAccessorVersionV9)
	if err != nil || len(packed) >= len(old) {
		t.Fatal(len(old), len(packed), err)
	}
}

func TestStateDomainChangeV9StrictCoverageRejectsForgedLocator(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	for tx := uint64(1); tx <= 140; tx++ {
		var hash common.Hash
		hash[31] = byte(tx)
		if err := rawdb.WriteStateTxRange(db, tx, hash, tx, tx); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{BlockNum: tx, BlockHash: hash, TxNum: tx, Seq: 1, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0: common.AddressPrefixMainnet, 20: byte(tx % 3)}, PrevExists: true, Prev: []byte{byte(tx)}}); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 140, "history/state-domain-change-v9-forged.seg")
	if err != nil {
		t.Fatal(err)
	}
	var history, index, accessor SegmentRef
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
	verify := func() error {
		return verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(context.Background(), dir, history, index, accessor)
	}
	if err := verify(); err != nil {
		t.Fatal("valid V9", err)
	}
	path := filepath.Join(dir, accessor.Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(bytes.NewReader(data), uint64(len(data)))
	if err != nil || !isHistoryOffsetAccessor(h.version) {
		t.Fatal(h, err)
	}
	entry := data[h.offsetDirectory : h.offsetDirectory+12]
	off, length, err := historyOffsetEntryBounds(h, entry)
	if err != nil {
		t.Fatal(err)
	}
	payload := data[h.offsetDataBase+off : h.offsetDataBase+off+length]
	first, err := stateDomainChangeBinaryAccessorV5Offset(payload[:6])
	if err != nil {
		t.Fatal(err)
	}
	if err := putStateDomainChangeBinaryAccessorV5Offset(payload[:6], first+1); err != nil {
		t.Fatal(err)
	}
	// Recomputed page CRC and physical SHA must not bypass the authoritative
	// sequential comparison against history offsets.
	putCRC := historyOffsetCRC(0, entry, payload)
	entry[8], entry[9], entry[10], entry[11] = byte(putCRC>>24), byte(putCRC>>16), byte(putCRC>>8), byte(putCRC)
	if err := checkStateDomainChangeBinaryAccessorV7(bytes.NewReader(data), uint64(len(data))); err != nil {
		t.Fatal("forgery must be structurally valid", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	accessor.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	accessor.Size = uint64(len(data))
	if err := verify(); err == nil {
		t.Fatal("accepted locator not matching history")
	}
}

func TestStateDomainChangeV9StrictCoverageRejectsForgedInline(t *testing.T) {
	testHistoryInlineStrictCoverageRejectsForgery(t, stateDomainChangeBinaryAccessorVersionV9)
}
