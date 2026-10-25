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
	"sort"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

func ownerFixtureKeys(count int, repeated bool) [][]byte {
	owners := make([][20]byte, count)
	for i := range owners {
		sum := sha256.Sum256([]byte(fmt.Sprintf("owner/%d", i)))
		copy(owners[i][:], sum[:20])
	}
	sort.Slice(owners, func(i, j int) bool { return bytes.Compare(owners[i][:], owners[j][:]) < 0 })
	var keys [][]byte
	for _, owner := range owners {
		keys = append(keys, append([]byte{1}, owner[:]...))
	}
	if repeated {
		for _, owner := range owners {
			key := make([]byte, 39)
			key[0] = 2
			copy(key[1:21], owner[:])
			binary.BigEndian.PutUint64(key[21:29], 1)
			binary.BigEndian.PutUint16(key[29:31], 515)
			copy(key[31:], []byte("1002000"))
			keys = append(keys, key)
		}
	}
	return keys
}

func buildOwnerAccessorFixture(t testing.TB, version uint32, count int, repeated bool) ([]byte, []v8ExpectedKey) {
	t.Helper()
	dir := t.TempDir()
	b, err := newStateDomainChangeV6Build(etl.Options{TempDir: t.TempDir()}, dir, "owners")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	keys := ownerFixtureKeys(count, repeated)
	expected := make([]v8ExpectedKey, len(keys))
	for i, key := range keys {
		expected[i].key = key
		if err := b.CollectLogicalKey(key); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.FinishDictionary(); err != nil {
		t.Fatal(err)
	}
	var ordinal uint32
	for i := range expected {
		count := 1
		if i%47 == 0 {
			count = 129
		}
		for j := 0; j < count; j++ {
			p := stateDomainChangeBinaryAccessorV6Posting{txNum: 10 + uint64(i), offset: 100 + uint64(ordinal)*37, recordIndex: ordinal}
			ordinal++
			expected[i].rows = append(expected[i].rows, p)
			if err := b.CollectPosting(uint32(i), p.txNum, p.offset, uint64(p.recordIndex)); err != nil {
				t.Fatal(err)
			}
		}
	}
	ref, _, err := b.buildAccessorContextVersion(context.Background(), dir, SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, Path: "history/fixture.kv", FromTxNum: 10, ToTxNum: 10 + uint64(len(keys))}, uint64(ordinal), version)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	return data, expected
}

func TestHistoryOwnerRoundTripAndFallback(t *testing.T) {
	for _, version := range []uint32{7, 8, 9, 10, 11} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			data, expected := buildOwnerAccessorFixture(t, version, 4200, true)
			r := bytes.NewReader(data)
			h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(r, uint64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			if h.version != version {
				t.Fatalf("wanted V%d got %d", version, h.version)
			}
			if err := checkStateDomainChangeBinaryAccessorV7(r, uint64(len(data))); err != nil {
				t.Fatal(err)
			}
			cursor, err := newStateDomainChangeBinaryAccessorV7PostingCursor(r, uint64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			for i, e := range expected {
				// Exercise every reconstructed dictionary ID and the batched page cursor.
				key, err := stateDomainChangeBinaryAccessorV6KeyByID(r, uint64(len(data)), uint32(i))
				if err != nil || !bytes.Equal(key, e.key) {
					t.Fatal(i, "key identity", err)
				}
				if i%127 == 0 {
					_, rec, ok, err := stateDomainChangeBinaryAccessorV7Lookup(r, uint64(len(data)), e.key)
					if err != nil || !ok || rec.keyID != uint32(i) {
						t.Fatal(i, "lookup", ok, err)
					}
				}
				for _, want := range e.rows {
					id, p, ok, err := cursor.Next()
					if err != nil || !ok || id != uint32(i) || p != want {
						t.Fatal(i, id, p, want, ok, err)
					}
				}
			}
			if _, _, ok, err := cursor.Next(); err != nil || ok {
				t.Fatal("cursor end", ok, err)
			}
			if version == 10 {
				t.Logf("V10 keys=%d ownerPages=%d bytes=%d", h.keyCount, ceilDiv(h.owners.count, 128), len(data))
			}
		})
	}
	for _, count := range []int{1, 127, 129, 4200} {
		a, _ := buildOwnerAccessorFixture(t, 9, count, false)
		b, _ := buildOwnerAccessorFixture(t, 10, count, false)
		if !bytes.Equal(a, b) {
			t.Fatal("nonrepeated owners must retain exact V9 when dictionary is not profitable", count, len(a), len(b))
		}
	}
	a, _ := buildAccessorVersionFixture(t, 9, 4200)
	b, _ := buildAccessorVersionFixture(t, 10, 4200)
	if !bytes.Equal(a, b) {
		t.Fatal("generic keys fallback changed bytes")
	}
}

func TestHistoryOwnerBoundsCorruptionCancellation(t *testing.T) {
	data, _ := buildOwnerAccessorFixture(t, 10, 512, true)
	r := bytes.NewReader(data)
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(r, uint64(len(data)))
	if err != nil || h.owners == nil {
		t.Fatal(h, err)
	}
	for _, at := range []uint64{h.owners.prefixSize, h.owners.prefixSize + h.owners.dataLen, uint64(len(data)) - 1} {
		bad := append([]byte(nil), data...)
		bad[at] ^= 1
		if err := checkStateDomainChangeBinaryAccessorV7(bytes.NewReader(bad), uint64(len(bad))); err == nil {
			t.Fatal("corruption accepted", at)
		}
	}
	for _, count := range []uint64{0, 1<<20 + 1, ^uint64(0)} {
		bad := append([]byte(nil), data...)
		footer := bad[len(bad)-64:]
		binary.BigEndian.PutUint64(footer[8:16], count)
		binary.BigEndian.PutUint32(footer[60:], crc32.ChecksumIEEE(footer[:60]))
		if _, err := decodeStateDomainChangeBinaryAccessorKeyHeader(bytes.NewReader(bad), uint64(len(bad))); err == nil {
			t.Fatal("invalid owner count", count)
		}
	}
	for _, cut := range []int{1, 12, 63, 64} {
		bad := data[:len(data)-cut]
		if _, err := decodeStateDomainChangeBinaryAccessorKeyHeader(bytes.NewReader(bad), uint64(len(bad))); err == nil {
			t.Fatal("truncated dictionary", cut)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := makeHistoryOwnerTableContext(ctx, [][20]byte{{}}); !errors.Is(err, context.Canceled) {
		t.Fatal("table cancellation", err)
	}
	_, _, _, err = stateDomainChangeBinaryAccessorV7Lookup(contextReaderAt{ctx: ctx, r: r}, uint64(len(data)), []byte{1})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("lookup cancellation", err)
	}
	entry := make([]byte, 18)
	binary.BigEndian.PutUint32(entry[6:10], ^uint32(0))
	binary.BigEndian.PutUint32(entry[10:14], ^uint32(0))
	if _, _, _, err := historyOwnerPageBounds(h.owners, 0, entry); err == nil {
		t.Fatal("oversized owner payload")
	}
	// Even a forged valid CRC cannot bypass the per-key-page four-owner-page bound.
	recs := make([]stateDomainChangeBinaryAccessorV6Record, 5)
	for i := range recs {
		recs[i].key = make([]byte, 5)
		binary.BigEndian.PutUint32(recs[i].key[1:], uint32(i*128))
	}
	fake := h
	copyHeader := *h.owners
	copyHeader.count = 640
	fake.owners = &copyHeader
	counted := &v9CountedReader{Reader: r}
	if err := resolveHistoryOwnerKeys(counted, fake, recs); err == nil || counted.calls != 0 {
		t.Fatal("unbounded owner reads", counted.calls, err)
	}
}

func TestHistoryOwnerSparsePagesRemainPlain(t *testing.T) {
	owners := make([][20]byte, 8192)
	for i := range owners {
		binary.BigEndian.PutUint32(owners[i][16:], uint32(i))
	}
	table, err := makeHistoryOwnerTable(owners)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]stateDomainChangeBinaryAccessorV6Key, 128)
	for i := range keys {
		keys[i] = stateDomainChangeBinaryAccessorV6Key{key: append([]byte{1}, owners[i*64][:]...), postingCount: 1, inlinePosting: stateDomainChangeBinaryAccessorV6Posting{txNum: 10, offset: 100 + uint64(i)*37, recordIndex: uint32(i)}}
	}
	largest := 0
	raw, _, _, err := encodeHistoryOwnerKeyBlock(keys, 10, table, &largest)
	if err != nil || raw[0] != 0 {
		t.Fatal("sparse page must retain canonical keys", err)
	}
}

func TestHistoryOwnerStrictCoverageRejectsForgedDictionary(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	hash := common.Hash{31: 1}
	if err := rawdb.WriteStateTxRange(db, 1, hash, 1, 1); err != nil {
		t.Fatal(err)
	}
	keys := ownerFixtureKeys(512, true)
	for i, key := range keys {
		owner := common.Address{0: common.AddressPrefixMainnet}
		copy(owner[1:], key[1:21])
		change := &rawdb.StateDomainChange{BlockNum: 1, BlockHash: hash, TxNum: 1, Seq: uint64(i + 1), FlatDomain: rawdb.StateFlatDomain(key[0]), Owner: owner, PrevExists: true, Prev: []byte{1}}
		if key[0] == 2 {
			change.Generation = 1
			change.Domain = 515
			change.Key = key[31:]
		}
		if err := rawdb.WriteStateDomainChange(db, change); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 1, "history/state-domain-change-forged-owner.seg")
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
		t.Fatal("valid companions", err)
	}
	path := filepath.Join(dir, accessor.Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(bytes.NewReader(data), uint64(len(data)))
	if err != nil || h.owners == nil {
		t.Fatal(h, err)
	}
	entry := data[h.owners.prefixSize+h.owners.dataLen : h.owners.prefixSize+h.owners.dataLen+18]
	off, n, flags, err := historyOwnerPageBounds(h.owners, 0, entry)
	if err != nil || flags&stateDomainChangeBinaryAccessorV6StoredRaw == 0 {
		t.Fatal("raw first dictionary page fixture", err)
	}
	payload := data[h.owners.prefixSize+off : h.owners.prefixSize+off+n]
	// Owner #1 is not a key-page first key. Perturb its last suffix byte while
	// preserving sorted order, then forge both physical checksums and key digest.
	pos := 21
	prefix := int(payload[pos])
	pos++
	payload[pos+20-prefix-1] ^= 1
	binary.BigEndian.PutUint32(entry[14:], historyOwnerPageCRC(0, entry, payload))
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(bytes.NewReader(data), h)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.New()
	for bi, b := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(bytes.NewReader(data), uint64(len(data)), h, b, uint32(bi*128))
		if err != nil {
			t.Fatal("forged expanded keys", err)
		}
		for _, rec := range records {
			var length [4]byte
			binary.BigEndian.PutUint32(length[:], uint32(len(rec.key)))
			digest.Write(length[:])
			digest.Write(rec.key)
		}
	}
	copy(data[72:104], digest.Sum(nil))
	binary.BigEndian.PutUint32(data[112:116], stateDomainChangeAccessorHeaderCRC(data, 10))
	if err := checkStateDomainChangeBinaryAccessorV7(bytes.NewReader(data), uint64(len(data))); err != nil {
		t.Fatal("forgery must be structurally well formed", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	accessor.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	accessor.Size = uint64(len(data))
	if err := check(); err == nil {
		t.Fatal("accepted owner mapping not matching authoritative history")
	}
}

// A key page spanning two distant owners must not read the gap between their
// directory entries. Preserve adjacent-entry batching without arbitrary overfetch.
func TestHistoryOwnerDistantDirectoryEntriesStayBounded(t *testing.T) {
	owners := make([][20]byte, 1<<18)
	for i := range owners {
		binary.BigEndian.PutUint64(owners[i][12:], uint64(i))
	}
	table, err := makeHistoryOwnerTable(owners)
	if err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	raw.Write(make([]byte, 152))
	if err := table.appendTo(&raw, 152); err != nil {
		t.Fatal(err)
	}
	h, err := decodeHistoryOwnerFooter(bytes.NewReader(raw.Bytes()), uint64(raw.Len()))
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]stateDomainChangeBinaryAccessorV6Record, 2)
	for i, id := range []uint32{0, 1<<18 - 1} {
		rows[i].key = make([]byte, 5)
		rows[i].key[0] = 1
		binary.BigEndian.PutUint32(rows[i].key[1:], id)
	}
	r := &ownerDirectoryCountReader{Reader: bytes.NewReader(raw.Bytes()), start: h.prefixSize + h.dataLen, end: h.fileSize - 64}
	if err := resolveHistoryOwnerKeys(r, stateDomainChangeBinaryAccessorV6Header{owners: h}, rows); err != nil {
		t.Fatal(err)
	}
	if r.bytes != 36 || r.calls != 2 {
		t.Fatal("sparse directory read amplification", r.bytes, r.calls)
	}
	if !bytes.Equal(rows[0].key[1:], owners[0][:]) || !bytes.Equal(rows[1].key[1:], owners[len(owners)-1][:]) {
		t.Fatal("reconstruction")
	}
}

type ownerDirectoryCountReader struct {
	*bytes.Reader
	start, end   uint64
	bytes, calls int
}

func (r *ownerDirectoryCountReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= 0 && uint64(off) >= r.start && uint64(off) < r.end {
		r.bytes += len(p)
		r.calls++
	}
	return r.Reader.ReadAt(p, off)
}
