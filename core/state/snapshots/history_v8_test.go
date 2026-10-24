package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

type v8ExpectedKey struct {
	key  []byte
	rows []stateDomainChangeBinaryAccessorV6Posting
}

func buildAccessorVersionFixture(t testing.TB, version uint32, keyCount int) ([]byte, []v8ExpectedKey) {
	t.Helper()
	dir := t.TempDir()
	b, err := newStateDomainChangeV6Build(etl.Options{TempDir: t.TempDir()}, dir, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	expected := make([]v8ExpectedKey, keyCount)
	for i := range expected {
		expected[i].key = []byte(fmt.Sprintf("key/%08d", i))
		if err := b.CollectLogicalKey(expected[i].key); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.FinishDictionary(); err != nil {
		t.Fatal(err)
	}
	var ordinal uint32
	for i := range expected {
		count := 1
		switch i {
		case 1:
			count = 2
		case 2:
			count = 128
		case 3:
			count = 129
		}
		for j := 0; j < count; j++ {
			// 129 same-TxNum records cross a frame boundary; offsets/ordinals differ.
			p := stateDomainChangeBinaryAccessorV6Posting{txNum: 10 + uint64(i), offset: 100 + uint64(ordinal)*37, recordIndex: ordinal}
			ordinal++
			expected[i].rows = append(expected[i].rows, p)
			if err := b.CollectPosting(uint32(i), p.txNum, p.offset, uint64(p.recordIndex)); err != nil {
				t.Fatal(err)
			}
		}
	}
	ref, _, err := b.buildAccessorContextVersion(context.Background(), dir, SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, Path: "history/fixture.kv", FromTxNum: 10, ToTxNum: 10 + uint64(keyCount)}, uint64(ordinal), version)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	return data, expected
}
func TestStateDomainChangeV8InlineSparseRoundTrip(t *testing.T) {
	data, expected := buildAccessorVersionFixture(t, stateDomainChangeBinaryAccessorVersionV8, 4101)
	r := bytes.NewReader(data)
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(r, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if h.version != 8 || h.sparseLen == 0 || h.postingLen == 0 {
		t.Fatal("missing mixed V8 layout", h)
	}
	if err := checkStateDomainChangeBinaryAccessorV7(r, uint64(len(data))); err != nil {
		t.Fatal(err)
	}
	cursor, err := newStateDomainChangeBinaryAccessorV7PostingCursor(r, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range expected {
		gotKey, err := stateDomainChangeBinaryAccessorV6KeyByID(r, uint64(len(data)), uint32(i))
		if err != nil || !bytes.Equal(gotKey, e.key) {
			t.Fatal(i, "dictionary", err)
		}
		_, record, ok, err := stateDomainChangeBinaryAccessorV7Lookup(r, uint64(len(data)), e.key)
		if err != nil || !ok || record.inline != (len(e.rows) == 1) {
			t.Fatal(i, "lookup", record, err)
		}
		frames, err := stateDomainChangeBinaryAccessorV7PostingFrames(r, h, record)
		if err != nil {
			t.Fatal(err)
		}
		var rows []stateDomainChangeBinaryAccessorV6Posting
		for _, frame := range frames {
			a, err := stateDomainChangeBinaryAccessorV7ReadFrame(r, h, record, frame)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, a...)
		}
		if len(rows) != len(e.rows) {
			t.Fatal(i, len(rows), len(e.rows))
		}
		for j, want := range e.rows {
			if rows[j] != want {
				t.Fatal(i, j, rows[j], want)
			}
			id, p, ok, err := cursor.Next()
			if err != nil || !ok || id != uint32(i) || p != want {
				t.Fatal("cursor", i, j, id, p, ok, err)
			}
		}
	}
	_, _, ok, err := cursor.Next()
	if err != nil || ok {
		t.Fatal("cursor end", ok, err)
	}
	for _, key := range [][]byte{[]byte("aaa"), []byte("key/00000000x"), []byte("zzz")} {
		_, _, ok, err := stateDomainChangeBinaryAccessorV7Lookup(r, uint64(len(data)), key)
		if err != nil || ok {
			t.Fatal("negative", string(key), ok, err)
		}
	}
	legacy, _ := buildAccessorVersionFixture(t, stateDomainChangeBinaryVersionV7, 4101)
	lh, err := decodeStateDomainChangeBinaryAccessorV7Header(bytes.NewReader(legacy), uint64(len(legacy)))
	if err != nil || lh.dictionaryDigest != h.dictionaryDigest {
		t.Fatal("dictionary changed", err)
	}
	t.Logf("V7=%d V8=%d sparse=%d", len(legacy), len(data), h.sparseLen)
}
func TestStateDomainChangeV8DirectoryBoundedLookupReads(t *testing.T) {
	v7, expected := buildAccessorVersionFixture(t, stateDomainChangeBinaryVersionV7, 9000)
	v8, _ := buildAccessorVersionFixture(t, stateDomainChangeBinaryAccessorVersionV8, 9000)
	key := expected[5432].key
	a := &countingStateDomainReaderAt{reader: bytes.NewReader(v7)}
	b := &countingStateDomainReaderAt{reader: bytes.NewReader(v8)}
	for _, r := range []*countingStateDomainReaderAt{a, b} {
		_, _, ok, err := stateDomainChangeBinaryAccessorV7Lookup(r, uint64(r.reader.(*bytes.Reader).Len()), key)
		if err != nil || !ok {
			t.Fatal(err)
		}
	}
	if b.reads > 5 || a.reads <= b.reads {
		t.Fatalf("directory read calls V7=%d V8=%d", a.reads, b.reads)
	}
	t.Logf("directory lookup raw ReadAt: V7=%d V8=%d", a.reads, b.reads)
}
func TestStateDomainChangeV8RejectsCorruptDirectoryAndInline(t *testing.T) {
	data, _ := buildAccessorVersionFixture(t, stateDomainChangeBinaryAccessorVersionV8, 4200)
	h, err := decodeStateDomainChangeBinaryAccessorV7Header(bytes.NewReader(data), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []uint64{116, h.headerSize + h.blockDirLen - h.sparseLen + 4, h.headerSize + h.blockCount*8 + 3} {
		b := append([]byte(nil), data...)
		b[at] ^= 1
		if err := checkStateDomainChangeBinaryAccessorV7(bytes.NewReader(b), uint64(len(b))); err == nil {
			t.Fatal("corruption accepted", at)
		}
	}
	// Invalid token, transaction delta and record ordinal must fail even under a valid key CRC.
	for _, raw := range [][]byte{{1}, {0, 255, 255, 255, 255, 15, 1, 0}, {0, 0, 1, 255, 255, 255, 255, 15}} {
		_, err := readStateDomainChangeAccessorV8Meta(bytes.NewReader(raw), h)
		if err == nil {
			t.Fatal("invalid inline metadata accepted", raw)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err = stateDomainChangeBinaryAccessorV7Lookup(contextReaderAt{ctx: ctx, r: bytes.NewReader(data)}, uint64(len(data)), []byte("key/00000001"))
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation", err)
	}
	b := append([]byte(nil), data...)
	b[116:120][0] ^= 1
	binary.BigEndian.PutUint32(b[112:116], stateDomainChangeAccessorHeaderCRC(b, 8))
	if _, err := decodeStateDomainChangeBinaryAccessorV7Header(bytes.NewReader(b), uint64(len(b))); err == nil {
		t.Fatal("oversized sparse header accepted")
	}
	groups, err := readHistoryAccessorSparseDirectory(bytes.NewReader(data), h)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) < 2 {
		t.Fatal("fixture")
	}
	sparse := append([]byte(nil), data[h.headerSize+h.blockDirLen-h.sparseLen:h.headerSize+h.blockDirLen]...)
	sparse[len(sparse)-5] ^= 1
	binary.BigEndian.PutUint32(sparse[len(sparse)-4:], crc32.ChecksumIEEE(sparse[:len(sparse)-4]))
	copy(b, data)
	copy(b[h.headerSize+h.blockDirLen-h.sparseLen:], sparse)
	if err := checkStateDomainChangeBinaryAccessorV7(bytes.NewReader(b), uint64(len(b))); err == nil {
		t.Fatal("forged sparse coverage accepted")
	}
}

// A freshly recomputed physical checksum cannot authenticate a forged record
// position. Strict snapshot validation must still compare inline tuples against
// the authoritative history stream.
func TestStateDomainChangeV8StrictCoverageRejectsForgedInline(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	for tx := uint64(1); tx <= 3; tx++ {
		var hash common.Hash
		hash[31] = byte(tx)
		if err := rawdb.WriteStateTxRange(db, tx, hash, tx, tx); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{BlockNum: tx, BlockHash: hash, TxNum: tx, Seq: 1, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0: common.AddressPrefixMainnet, 20: byte(tx)}, PrevExists: true, Prev: []byte{byte(tx)}}); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 3, "history/state-domain-change-forged-inline.seg")
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
	check := func() error {
		return verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(context.Background(), dir, history, index, accessor)
	}
	if err := check(); err != nil {
		t.Fatal("valid V8 companions", err)
	}
	file := filepath.Join(dir, accessor.Path)
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	r := bytes.NewReader(data)
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(r, uint64(len(data)))
	if err != nil || h.version != 8 || h.blockCount != 1 {
		t.Fatal("V8 fixture", h, err)
	}
	b, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r, h, 0)
	if err != nil {
		t.Fatal(err)
	}
	records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, uint64(len(data)), h, b, 0)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]stateDomainChangeBinaryAccessorV6Key, len(records))
	for i, record := range records {
		if !record.inline {
			t.Fatal("not singleton")
		}
		keys[i] = stateDomainChangeBinaryAccessorV6Key{key: record.key, postingCount: 1, inlinePosting: record.inlinePosting}
	}
	keys[0].inlinePosting.recordIndex, keys[1].inlinePosting.recordIndex = keys[1].inlinePosting.recordIndex, keys[0].inlinePosting.recordIndex
	raw, err := encodeStateDomainChangeBinaryAccessorV8KeyBlock(keys, h.fromTxNum)
	if err != nil || uint32(len(raw)) != b.rawLen || b.dataLen&stateDomainChangeBinaryAccessorV6StoredRaw == 0 || b.dataLen&^stateDomainChangeBinaryAccessorV6StoredRaw != b.rawLen {
		t.Fatal("uncompressed fixture changed length", err)
	}
	copy(data[b.dataOff:], raw)
	entry := binary.BigEndian.Uint64(data[h.headerSize:])
	tail := data[entry+2+uint64(len(b.firstKey)) : entry+2+uint64(len(b.firstKey))+28]
	binary.BigEndian.PutUint32(tail[16:20], crc32.ChecksumIEEE(raw))
	binary.BigEndian.PutUint32(tail[24:28], crc32.Update(crc32.ChecksumIEEE(b.firstKey), crc32.IEEETable, tail[:24]))
	if err := checkStateDomainChangeBinaryAccessorV7(bytes.NewReader(data), uint64(len(data))); err != nil {
		t.Fatal("forged tuple should remain structurally well formed", err)
	}
	if err := os.WriteFile(file, data, 0600); err != nil {
		t.Fatal(err)
	}
	accessor.Size = uint64(len(data))
	accessor.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	if err := check(); err == nil {
		t.Fatal("strict companion gate accepted forged inline ordinal with valid CRC/SHA")
	}
}
