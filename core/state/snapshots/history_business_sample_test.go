package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestBusinessSampleWindowsDeterministicDisjoint(t *testing.T) {
	for _, count := range []uint64{0, 1, 7, 32, 1000, 142950901} {
		o := HistoryBusinessSampleOptions{Seed: 999, Windows: 16, TxEntriesPerWindow: 8}
		a, b := businessSampleWindows(count, o), businessSampleWindows(count, o)
		if !reflect.DeepEqual(a, b) {
			t.Fatal("seed is not reproducible")
		}
		for i, w := range a {
			if w[1] == 0 || w[0]+w[1] > count || i > 0 && w[0] < a[i-1][0]+a[i-1][1] {
				t.Fatalf("count=%d windows=%v", count, a)
			}
		}
	}
}

type businessHeaderOnlyReader struct {
	calls int
	data  [21]byte
}

func (r *businessHeaderOnlyReader) ReadAt(p []byte, off int64) (int, error) {
	r.calls++
	if off != 0 || len(p) != 21 {
		return 0, io.ErrUnexpectedEOF
	}
	return copy(p, r.data[:]), nil
}
func TestBusinessSampleGiganticValueDoesNotReadBody(t *testing.T) {
	r := new(businessHeaderOnlyReader)
	const valueBytes = uint64(1) << 30
	binary.BigEndian.PutUint32(r.data[:4], uint32(valueBytes+17))
	binary.BigEndian.PutUint32(r.data[4:8], 19)
	binary.BigEndian.PutUint64(r.data[8:16], 111)
	r.data[16] = 1
	binary.BigEndian.PutUint32(r.data[17:], uint32(valueBytes))
	k, p, n, next, e := readBusinessSampleRecordHeader(r, 0, 21+valueBytes, 111)
	if e != nil || k != 19 || !p || n != valueBytes || next != 21+valueBytes || r.calls != 1 {
		t.Fatalf("k=%d p=%t n=%d next=%d reads=%d err=%v", k, p, n, next, r.calls, e)
	}
	if _, _, _, _, e := readBusinessSampleRecordHeader(r, 0, 20+valueBytes, 111); e == nil {
		t.Fatal("truncated giant accepted")
	}
	if _, _, _, _, e := readBusinessSampleRecordHeader(r, 0, 21+valueBytes, 112); e == nil {
		t.Fatal("wrong tx accepted")
	}
	r.data[16] = 0
	if _, _, _, _, e := readBusinessSampleRecordHeader(r, 0, 21+valueBytes, 111); e == nil {
		t.Fatal("absent with value accepted")
	}
}

func businessSampleFixture(t *testing.T) (string, SegmentRef, SegmentRef, SegmentRef, uint64) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	owner := common.SystemAccountAddress
	giant := make([]byte, 2<<20)
	rand.New(rand.NewSource(10)).Read(giant)
	var valueBytes uint64
	for i := uint64(1); i <= 32; i++ {
		hash := common.Hash{byte(i)}
		if e := rawdb.WriteStateTxRange(db, i, hash, i, i); e != nil {
			t.Fatal(e)
		}
		c := &rawdb.StateDomainChange{BlockNum: i, BlockHash: hash, TxNum: i, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.SystemDelegation, Key: rawdb.DrAccountIndexLegacyStateKey(common.Address{0x41, 1}.Bytes()), PrevExists: true, Prev: bytes.Repeat([]byte{byte(i)}, 35)}
		if i == 16 {
			c.Prev = giant
		}
		if i == 1 {
			c.PrevExists = false
			c.Prev = nil
		}
		if i == 2 {
			c.Prev = nil
		}
		valueBytes += uint64(len(c.Prev))
		if e := rawdb.WriteStateDomainChange(db, c); e != nil {
			t.Fatal(e)
		}
	}
	dir := t.TempDir()
	refs, e := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 32, "history/state-domain-change-business.seg")
	if e != nil {
		t.Fatal(e)
	}
	var h, i, a SegmentRef
	for _, ref := range refs {
		switch ref.Kind {
		case SegmentHistory:
			h = ref
		case SegmentInverted:
			i = ref
		case SegmentAccessor:
			a = ref
		}
	}
	return dir, h, i, a, valueBytes
}

func TestBusinessSampleMatchesFullLogicalOracleBothContainers(t *testing.T) {
	for _, format := range []string{"1", "2"} {
		t.Run(format, func(t *testing.T) {
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", format)
			dir, h, i, a, want := businessSampleFixture(t)
			o := HistoryBusinessSampleOptions{Seed: 1, Windows: 1, TxEntriesPerWindow: 32, MaxRecords: 100, MaxRecordsPerWindow: 100}
			r, e := SampleHistoryBusiness(context.Background(), dir, h, i, a, o)
			if e != nil {
				t.Fatal(e)
			}
			s := r.Categories["kv-latest/SystemDelegation/drax-0-aggregate"]
			if !r.FileStatsStable || r.Records != 32 || len(r.Windows) != 1 || !r.Windows[0].Complete || s == nil || s.PreviousBytes != want || s.Present != 31 || s.EmptyPresent != 1 || s.MaxPreviousBytes != 2<<20 {
				t.Fatalf("report=%+v stats=%+v", r, s)
			}
			if r.ChargedReadBytes > 1<<20 {
				t.Fatalf("header-only sample read %d bytes for one giant", r.ChargedReadBytes)
			}
			if len(r.TopKeys) != 1 || r.TopKeys[0].Records != 32 || r.TopKeys[0].PreviousBytes != want {
				t.Fatal(r.TopKeys)
			}
			// The public production reader is the independent decoded-value oracle.
			cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
			var sum, n uint64
			e = cfg.IterateHistoryRange(dir, NewManifest(1, 32, []SegmentRef{h, i, a}), h, 1, 32, func(c *rawdb.StateDomainChange) (bool, error) { sum += uint64(len(c.Prev)); n++; return true, nil })
			if e != nil || sum != s.PreviousBytes || n != r.Records {
				t.Fatalf("oracle n=%d sum=%d err=%v", n, sum, e)
			}
			o.MaxRecords = 3
			r, e = SampleHistoryBusiness(context.Background(), dir, h, i, a, o)
			if e != nil || r.Records != 3 || r.Windows[0].Complete || r.Windows[0].StopReason == "" {
				t.Fatalf("uncensored cap: %+v %v", r, e)
			}
			o.MaxReadBytes = 7
			r, e = SampleHistoryBusiness(context.Background(), dir, h, i, a, o)
			if e == nil || r.ChargedReadBytes != 0 {
				t.Fatalf("read cap exceeded: %+v %v", r, e)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, e := SampleHistoryBusiness(ctx, dir, h, i, a, o); e == nil {
				t.Fatal("cancel ignored")
			}
		})
	}
}

func TestBusinessSampleCompressedMetadataRejectedBeforeAllocation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "oversized.seg")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	var b [48]byte
	copy(b[:8], compressedBlockMagic)
	binary.BigEndian.PutUint32(b[8:12], 1)
	binary.BigEndian.PutUint32(b[12:16], 128<<10)
	binary.BigEndian.PutUint64(b[24:32], 1000000)
	if _, e = f.Write(b[:]); e != nil {
		t.Fatal(e)
	}
	f.Close()
	bgt := &businessSampleBudget{ctx: context.Background(), limit: 1 << 20}
	o, _ := (HistoryBusinessSampleOptions{MaxMetadataBytes: 1024}).defaults()
	if f, e := openBusinessSampleFile(dir, SegmentRef{Path: "oversized.seg"}, bgt, o, true); e == nil {
		f.Close()
		t.Fatal("huge table accepted")
	}
	if bgt.used > 56 {
		t.Fatal("read metadata despite rejection", bgt.used)
	}
}

func TestBusinessSampleCategoryShapes(t *testing.T) {
	a, b := common.Address{0x41, 1}, common.Address{0x41, 2}
	for _, tc := range []struct {
		k    []byte
		want string
	}{{rawdb.DelegationIndexStateKey(a), "dri-address-list"}, {rawdb.DrAccountIndexLegacyStateKey(a[:]), "drax-0-aggregate"}, {rawdb.DrAccountIndexStateKey(rawdb.DrAccIdxV2To, a[:], b[:]), "drax-4-directional"}, {rawdb.DelegatedResourceV2StateKey(a, b, false), "dr-v2-bucket-1"}, {[]byte("drax-"), "drax-malformed"}} {
		c := &rawdb.StateDomainChange{FlatDomain: rawdb.StateFlatDomainKVLatest, Domain: kvdomains.SystemDelegation, Key: tc.k}
		if got := StateHistoryBusinessCategory(c); got != "kv-latest/SystemDelegation/"+tc.want {
			t.Fatalf("%x => %s", tc.k, got)
		}
	}
}

func TestBusinessSampleReadBudgetIncludesEvictedCompressedChunks(t *testing.T) {
	enc, e := zstd.NewWriter(nil)
	if e != nil {
		t.Fatal(e)
	}
	defer enc.Close()
	dec, e := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecodeAllCapLimit(true))
	if e != nil {
		t.Fatal(e)
	}
	file, e := os.CreateTemp(t.TempDir(), "chunks")
	if e != nil {
		t.Fatal(e)
	}
	var table []cbBlock
	var physical, want []byte
	for i := 0; i < 4; i++ {
		data := bytes.Repeat([]byte{byte(i)}, 4)
		zipped := enc.EncodeAll(data, nil)
		table = append(table, cbBlock{uncompressedStart: uint64(i * 4), compressedStart: uint64(len(physical)), compressedLen: uint64(len(zipped)), records: 1})
		physical = append(physical, zipped...)
		want = append(want, data...)
	}
	if _, e = file.Write(physical); e != nil {
		t.Fatal(e)
	}
	bgt := &businessSampleBudget{ctx: context.Background(), limit: uint64(len(physical) - 1)}
	cr := &compressedBlockReader{f: file, dec: dec, blockSize: 4, uncSize: 16, fileSize: uint64(len(physical)), table: table, cacheLimit: 2, cache: []cbCacheEntry{{idx: 2, bytes: bytes.Repeat([]byte{2}, 4)}, {idx: 1, bytes: bytes.Repeat([]byte{1}, 4)}}}
	r := &businessSampleFile{file: file, compressed: cr, decoder: dec, logical: 16, budget: bgt}
	defer r.Close()
	var data [16]byte
	if _, e = r.ReadAt(data[:], 0); e == nil || bgt.used != 0 {
		t.Fatal("read miss budget not checked before I/O", e, bgt.used)
	}
	bgt.limit = uint64(len(physical))
	if _, e = r.ReadAt(data[:], 0); e != nil || !bytes.Equal(data[:], want) || bgt.used != uint64(len(physical)) {
		t.Fatal("MRU budget/result mismatch", e, bgt.used, data)
	}
}
