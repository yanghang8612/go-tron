package rawdb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"reflect"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

func chunkHistoryRows(size, versions int) []*StateDomainChange {
	rng := rand.New(rand.NewSource(20260907))
	base := make([]byte, size)
	_, _ = rng.Read(base)
	rows := make([]*StateDomainChange, versions)
	for i := range rows {
		// Repeated random address-like contents with a small insertion and a
		// changed header; alignment shifts so fixed-size dedup cannot win.
		prev := append([]byte{byte(i), 0x41}, base[:size/2]...)
		prev = append(prev, bytes.Repeat([]byte{byte(i)}, 21*i)...)
		prev = append(prev, base[size/2:]...)
		rows[i] = &StateDomainChange{BlockNum: 42, Seq: uint64(i + 1), TxNum: 500 + uint64(i), FlatDomain: StateFlatDomainKVLatest,
			Owner: common.Address{0x41, 3}, Generation: 7, Domain: kvdomains.SystemDelegation, Key: []byte("drax-0-fixture"), PrevExists: true, Prev: prev}
	}
	return rows
}

func TestStateChangeChunksProductionReadersAndUnwind(t *testing.T) {
	prior := stateChangeBlockChunkEncoding.Swap(true)
	t.Cleanup(func() { stateChangeBlockChunkEncoding.Store(prior) })
	rows := chunkHistoryRows(2<<20, 4)
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteStateDomainChangeBlockRows(db, rows); err != nil {
		t.Fatal(err)
	}
	stored, err := db.Get(stateChangeSetKey(42, 0))
	if err != nil {
		t.Fatal(err)
	}
	if stored[len(stateDomainChangeBlockEnvelopeMagic)] != stateDomainChangeBlockChunksVersion {
		t.Fatal("production writer did not use chunk pack")
	}
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	if len(stored)*100 > len(raw)*35 {
		t.Fatalf("insufficient fixture savings: stored %d raw %d", len(stored), len(raw))
	}
	decoded, err := decodeStateDomainChangeBlockStorage(stored)
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("raw RLP changed: %v", err)
	}
	owned, err := decodePersistedStateDomainChangeBlock(stored, 42)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows, owned) {
		t.Fatal("owning decode changed rows")
	}
	i := 0
	_, err = iteratePersistedStateDomainChangeBlockBorrowed(stored, 42, func(row *StateDomainChange) (bool, error) {
		if !reflect.DeepEqual(row, rows[i]) {
			t.Fatalf("borrowed row %d mismatch", i)
		}
		i++
		return true, nil
	})
	if err != nil || i != len(rows) {
		t.Fatalf("borrowed: %d %v", i, err)
	}
	for _, row := range rows {
		got, ok, err := ReadStateDomainChange(db, 42, row.Seq)
		if err != nil || !ok || !bytes.Equal(got.Prev, row.Prev) {
			t.Fatalf("point seq %d: %v %v", row.Seq, ok, err)
		}
	}
	first := rows[0]
	if err := WriteStateKVLatest(db, first.Owner, first.Generation, first.Domain, first.Key, []byte("tip")); err != nil {
		t.Fatal(err)
	}
	if _, err := CollectStateUnwind(db, 42, 41); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadStateKVLatest(db, first.Owner, first.Generation, first.Domain, first.Key)
	if err != nil || !ok || !bytes.Equal(got, first.Prev) {
		t.Fatalf("unwind: %v %v", ok, err)
	}
	t.Logf("synthetic 4-version block: raw=%d stored=%d saved=%.2f%%", len(raw), len(stored), 100*(1-float64(len(stored))/float64(len(raw))))
}

func TestStateChangeChunksGatePreservesOrdinaryPath(t *testing.T) {
	rows := chunkHistoryRows(2<<20, 2)
	for _, variant := range []string{"single", "generation", "owner", "domain", "key", "absent"} {
		a, b := *rows[0], *rows[1]
		changes := []*StateDomainChange{&a, &b}
		switch variant {
		case "single":
			changes = changes[:1]
		case "generation":
			b.Generation++
		case "owner":
			b.Owner[1]++
		case "domain":
			b.Domain = kvdomains.SystemReward
		case "key":
			b.Key = []byte("other")
		case "absent":
			b.PrevExists = false
		}
		if stateChangeBlockHasLargeVersions(changes) {
			t.Fatalf("gate crossed %s", variant)
		}
	}
	if !stateChangeBlockHasLargeVersions(rows) {
		t.Fatal("repeated large values rejected")
	}
}

func TestStateChangeChunksRejectMalformedAndCorruption(t *testing.T) {
	full := bytes.Repeat([]byte{9}, historychunk.MinSize)
	header := func(size uint64) []byte {
		b := binary.AppendUvarint(nil, size)
		return binary.LittleEndian.AppendUint32(b, crc32.ChecksumIEEE(full))
	}
	anchor := append(binary.AppendUvarint([]byte{0}, uint64(len(full))), full...)
	ref := binary.AppendUvarint(binary.AppendUvarint([]byte{2}, uint64(len(full))), 0)
	bad := [][]byte{
		{}, {0xff}, header(stateDomainChangeBlockMaxDecodedBytes + 1),
		append(header(uint64(len(full))), ref...), // forward reference
		append(header(1), 0, 0),                   // zero size
		append(header(1), 3, 1),                   // unknown kind
		append(header(1), 1, 1, 1, 0xff),          // corrupt snappy
		append(header(uint64(len(full))), anchor[:len(anchor)-1]...),
		append(append(header(uint64(len(full))), anchor...), 0), // trailing
	}
	// Third chunk tries to reference the second (which itself references 0).
	chain := append(header(uint64(3*len(full))), anchor...)
	chain = append(chain, ref...)
	chain = append(chain, binary.AppendUvarint(binary.AppendUvarint([]byte{2}, uint64(len(full))), 1)...)
	bad = append(bad, chain)
	for i, payload := range bad {
		if _, err := decodeStateChangeChunks(nil, payload); err == nil {
			t.Fatalf("accepted malformed %d", i)
		}
	}
	rows := chunkHistoryRows(1<<20, 3)
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	encoded, ok := encodeStateChangeChunks(raw)
	if !ok {
		t.Fatal("fixture did not compress")
	}
	for _, offset := range []int{len(stateDomainChangeBlockEnvelopeMagic) + 5, len(encoded) / 2, len(encoded) - 1} {
		mutated := bytes.Clone(encoded)
		mutated[offset] ^= 0x80
		if _, err := decodeStateDomainChangeBlockStorage(mutated); err == nil {
			t.Fatalf("accepted corruption at %d", offset)
		}
	}
}

func BenchmarkStateChangeChunkBlock(b *testing.B) {
	rows := chunkHistoryRows(2<<20, 4)
	raw := encodeBorrowedStateDomainChangeTestBlock(b, rows)
	chunked, ok := encodeStateChangeChunks(raw)
	if !ok {
		b.Fatal("fixture not compressed")
	}
	baseline, _ := encodeStateDomainChangeBlockStorage(raw)
	for _, name := range []string{"snappy-encode", "chunks-encode", "snappy-decode", "chunks-decode"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			stored := baseline
			if name == "chunks-encode" || name == "chunks-decode" {
				stored = chunked
			}
			b.ReportMetric(float64(len(stored)), "stored-B")
			for i := 0; i < b.N; i++ {
				switch name {
				case "snappy-encode":
					encodeStateDomainChangeBlockStorage(raw)
				case "chunks-encode":
					encodeStateChangeChunks(raw)
				default:
					if _, err := decodeStateDomainChangeBlockStorage(stored); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
