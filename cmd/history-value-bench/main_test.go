package main

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestCopiedTrioReadOnlyAndActualCodecRoundTrips(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	rng := rand.New(rand.NewSource(19))
	bases := make([][]byte, 32)
	for i := range bases {
		bases[i] = make([]byte, 8192)
		_, _ = rng.Read(bases[i])
	}
	for tx := uint64(1); tx <= 256; tx++ {
		value := bytes.Clone(bases[tx%32])
		value[50] ^= byte(tx / 32)
		value[4000] ^= byte(tx / 32)
		c := &rawdb.StateDomainChange{BlockNum: tx, BlockHash: common.Hash{byte(tx)}, TxNum: tx, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 0x17}, Domain: kvdomains.ContractMetadata, Key: []byte{byte(tx % 32)}, PrevExists: true, Prev: value}
		if tx%61 == 0 {
			c.Prev = nil
			c.PrevExists = false
		}
		if tx%67 == 0 {
			c.Prev = []byte{}
		}
		if err := rawdb.WriteStateTxRange(db, tx, c.BlockHash, tx, tx); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{c}); err != nil {
			t.Fatal(err)
		}
	}
	cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(db, dir, 1, 256, 1, 256, cfg.HistoryPath(1, 256))
	if err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for _, ref := range refs {
		data, e := os.ReadFile(filepath.Join(dir, ref.Path))
		if e != nil {
			t.Fatal(e)
		}
		before[ref.Path] = data
	}
	r, err := run(dir, refs[0].Path, 1, 256, 16<<20, 32)
	if err != nil {
		t.Fatal(err)
	}
	if !r.ReaderMatchesRaw || !r.InputUnchanged || r.Records != 256 || r.Keys != 32 || len(r.Codecs) != 4 {
		t.Fatalf("incomplete result: %+v", r)
	}
	for _, c := range r.Codecs {
		if !c.ByteExact || c.DecodedSHA256 != r.RawSHA256 {
			t.Fatalf("codec did not validate: %+v", c)
		}
	}
	if r.Codecs[2].ContainerBytes >= r.Codecs[0].ContainerBytes/2 {
		t.Fatalf("interior-copy fixture did not demonstrate actual compression savings: %+v", r.Codecs)
	}
	for path, data := range before {
		after, e := os.ReadFile(filepath.Join(dir, path))
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(data, after) {
			t.Fatalf("modified source %s", path)
		}
	}
}

func TestCodecAdversarialBinaryValues(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	var rows []record
	prefix := []byte("opaque-prefix")
	raw := bytes.Clone(prefix)
	values := [][]byte{nil, {}, {0}, bytes.Repeat([]byte{0}, 1024), bytes.Repeat([]byte{255}, 513), []byte("same")}
	for i := 0; i < 300; i++ {
		v := values[i%len(values)]
		if i%7 == 0 {
			v = make([]byte, i*13)
			_, _ = rng.Read(v)
		}
		header := make([]byte, 21)
		binary.BigEndian.PutUint32(header[:4], uint32(17+len(v)))
		binary.BigEndian.PutUint32(header[4:8], uint32(i%3))
		binary.BigEndian.PutUint64(header[8:16], uint64(i))
		header[16] = byte(i % 2)
		binary.BigEndian.PutUint32(header[17:21], uint32(len(v)))
		rows = append(rows, record{header, v, uint32(i % 3), uint64(i)})
		raw = append(raw, header...)
		raw = append(raw, v...)
	}
	for _, codec := range []string{"same-key-prefix-suffix", "same-key-copy-literal", "segment-exact-dedup"} {
		for _, checkpoint := range []int{1, 2, 32} {
			encoded := encode(prefix, rows, codec, checkpoint, &codecResult{})
			decoded, err := decode(encoded, len(raw))
			if err != nil {
				t.Fatalf("%s: %v", codec, err)
			}
			if !bytes.Equal(decoded, raw) {
				t.Fatal("lost binary bytes or existence flag")
			}
			if _, err := decode(encoded, len(raw)-1); err == nil {
				t.Fatal("accepted decoded overflow")
			}
		}
	}
}

func TestContainerRejectsMalformedOrOversizedData(t *testing.T) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	raw := bytes.Repeat([]byte("a"), chunkSize+9)
	packed := pack(raw, enc)
	decoded, err := unpack(packed, uint64(len(raw)))
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("round trip: %v", err)
	}
	if _, err := unpack(packed, uint64(len(raw)-1)); err == nil {
		t.Fatal("accepted logical overflow")
	}
	bad := bytes.Clone(packed)
	binary.BigEndian.PutUint64(bad[56:64], 1)
	if _, err := unpack(bad, uint64(len(raw))); err == nil {
		t.Fatal("accepted overlapping table")
	}
	if _, err := unpack(append(packed, 0), uint64(len(raw))); err == nil {
		t.Fatal("accepted trailing bytes")
	}
	if _, err := applyCopyDelta([]byte("x"), []byte{1, 2, 1}, 1); err == nil {
		t.Fatal("accepted out-of-bounds copy")
	}
}
