package rawdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestStateTxRangeRoundTrip(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	hash := common.Hash{0xaa}
	if _, ok, err := ReadStateTxRange(db, 7); err != nil || ok {
		t.Fatalf("pre-read = ok:%v err:%v", ok, err)
	}
	if err := WriteStateTxRange(db, 7, hash, 7, 7); err != nil {
		t.Fatalf("write tx range: %v", err)
	}
	got, ok, err := ReadStateTxRange(db, 7)
	if err != nil || !ok {
		t.Fatalf("read tx range = ok:%v err:%v", ok, err)
	}
	if got.BlockNum != 7 || got.BlockHash != hash || got.BeginTxNum != 7 || got.EndTxNum != 7 {
		t.Fatalf("range = %+v", got)
	}
}

func TestNextStateTxRangeUsesCompactGlobalSequence(t *testing.T) {
	begin, end, err := NextStateTxRange(41, 3)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if begin != 42 || end != 45 {
		t.Fatalf("range = [%d,%d], want [42,45]", begin, end)
	}
	txNum, err := StateTxNumAt(begin, 2)
	if err != nil {
		t.Fatalf("tx num at: %v", err)
	}
	if txNum != 44 {
		t.Fatalf("tx num = %d, want 44", txNum)
	}
	if _, err := StateTxNumAt(^uint64(0), 1); err == nil {
		t.Fatal("expected overflowing ordinal to fail")
	}
	if _, _, err := NextStateTxRange(^uint64(0), 0); err == nil {
		t.Fatal("expected overflowing parent end to fail")
	}
}

func TestStateTxNumAtBlockEndUsesStoredRangeAndLegacyFallback(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	got, err := StateTxNumAtBlockEnd(db, 7)
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Fatalf("fallback tx num = %d, want legacy block number 7", got)
	}
	begin, end, err := NextStateTxRange(41, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteStateTxRange(db, 7, common.Hash{0x07}, begin, end); err != nil {
		t.Fatal(err)
	}
	got, err = StateTxNumAtBlockEnd(db, 7)
	if err != nil {
		t.Fatal(err)
	}
	if got != end {
		t.Fatalf("stored end tx num = %d, want %d", got, end)
	}
}

func TestStateDomainChangeRoundTripAndIteration(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	change1 := &StateDomainChange{
		BlockNum:   9,
		BlockHash:  common.Hash{0x09},
		TxNum:      9,
		Seq:        1,
		FlatDomain: StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 3,
		Domain:     kvdomains.SystemReward,
		Key:        []byte("reward/1"),
		PrevExists: true,
		Prev:       []byte("old"),
		NextExists: true,
		Next:       []byte("new"),
	}
	change2 := &StateDomainChange{
		BlockNum:   9,
		BlockHash:  common.Hash{0x09},
		TxNum:      9,
		Seq:        2,
		FlatDomain: StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 3,
		Domain:     kvdomains.SystemReward,
		Key:        []byte("reward/2"),
		PrevExists: true,
		Prev:       []byte("gone"),
	}
	if err := WriteStateDomainChange(db, change1); err != nil {
		t.Fatalf("write change1: %v", err)
	}
	if err := WriteStateDomainChange(db, change2); err != nil {
		t.Fatalf("write change2: %v", err)
	}

	got, ok, err := ReadStateDomainChange(db, 9, 1)
	if err != nil || !ok {
		t.Fatalf("read change = ok:%v err:%v", ok, err)
	}
	if got.FlatDomain != StateFlatDomainKVLatest || got.Domain != kvdomains.SystemReward || !bytes.Equal(got.Prev, []byte("old")) || got.NextExists || got.Next != nil {
		t.Fatalf("change = %+v", got)
	}
	if got.BlockNum != change1.BlockNum || got.Seq != change1.Seq || got.BlockHash != (common.Hash{}) {
		t.Fatalf("derived row context = block:%d seq:%d hash:%x", got.BlockNum, got.Seq, got.BlockHash)
	}
	got.Prev[0] = 'x'
	reread, _, _ := ReadStateDomainChange(db, 9, 1)
	if bytes.Equal(reread.Prev, got.Prev) {
		t.Fatal("ReadStateDomainChange returned aliased bytes")
	}

	var seqs []uint64
	if err := IterateStateDomainChanges(db, 9, func(change *StateDomainChange) (bool, error) {
		seqs = append(seqs, change.Seq)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate changes: %v", err)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("seqs = %v", seqs)
	}

	var blocks []uint64
	if err := IterateStateDomainChangeBlocks(db, owner, 3, kvdomains.SystemReward, []byte("reward/1"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate inverse: %v", err)
	}
	if len(blocks) != 1 || blocks[0] != 9 {
		t.Fatalf("inverse blocks = %v", blocks)
	}
}

func TestStateDomainChangeBlockPackRoundTripAndLogicalBytes(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x19}
	blocksBefore := stateChangeBlockPackBlocksCounter.Snapshot().Count()
	rowsBefore := stateChangeBlockPackRowsCounter.Snapshot().Count()
	encodedBefore := stateChangeBlockPackEncodedBytesCounter.Snapshot().Count()
	logicalBefore := stateChangeBlockPackLogicalBytesCounter.Snapshot().Count()
	writesAvoidedBefore := stateChangeBlockPackWritesAvoidedCounter.Snapshot().Count()
	keysAvoidedBefore := stateChangeBlockPackKeyBytesAvoidedCounter.Snapshot().Count()
	uncompressedBefore := stateChangeBlockPackUncompressedCounter.Snapshot().Count()
	compressionSavedBefore := stateChangeBlockPackCompressionSavedCounter.Snapshot().Count()
	compressedBefore := stateChangeBlockPackCompressedCounter.Snapshot().Count()
	rawBefore := stateChangeBlockPackRawCounter.Snapshot().Count()
	changes := make([]*StateDomainChange, 128)
	individualBytes := 0
	for i := range changes {
		changes[i] = &StateDomainChange{
			BlockNum:   19,
			TxNum:      100 + uint64(i/4),
			Seq:        uint64(i + 1),
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 2,
			Domain:     kvdomains.SystemReward,
			Key:        []byte(fmt.Sprintf("reward/%03d", i)),
			PrevExists: true,
			Prev:       []byte("previous-value"),
			NextExists: true,
			Next:       []byte("current-value"),
		}
		encoded, err := encodePersistedStateDomainChange(changes[i])
		if err != nil {
			t.Fatal(err)
		}
		individualBytes += len(stateChangeSetKey(19, uint64(i+1))) + len(encoded)
	}
	if err := WriteStateDomainChangeBlockRows(db, changes); err != nil {
		t.Fatal(err)
	}
	packed, err := db.Get(stateChangeSetKey(19, 0))
	if err != nil {
		t.Fatal(err)
	}
	uncompressed, err := decodeStateDomainChangeBlockStorage(packed)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(uncompressed, packed) {
		t.Fatal("compressible block pack was stored raw")
	}
	packedBytes := len(stateChangeSetKey(19, 0)) + len(packed)
	if packedBytes*100 >= individualBytes*80 {
		t.Fatalf("packed logical bytes = %d, individual = %d, want >20%% reduction", packedBytes, individualBytes)
	}
	t.Logf("block pack logical bytes: individual=%d packed=%d reduction=%.2f%%", individualBytes, packedBytes, 100*(1-float64(packedBytes)/float64(individualBytes)))
	metricChecks := []struct {
		name      string
		got, want int64
	}{
		{"blocks", stateChangeBlockPackBlocksCounter.Snapshot().Count() - blocksBefore, 1},
		{"rows", stateChangeBlockPackRowsCounter.Snapshot().Count() - rowsBefore, int64(len(changes))},
		{"encoded bytes", stateChangeBlockPackEncodedBytesCounter.Snapshot().Count() - encodedBefore, int64(len(packed))},
		{"logical bytes", stateChangeBlockPackLogicalBytesCounter.Snapshot().Count() - logicalBefore, int64(packedBytes)},
		{"writes avoided", stateChangeBlockPackWritesAvoidedCounter.Snapshot().Count() - writesAvoidedBefore, int64(len(changes) - 1)},
		{"key bytes avoided", stateChangeBlockPackKeyBytesAvoidedCounter.Snapshot().Count() - keysAvoidedBefore, int64((len(changes) - 1) * len(stateChangeSetKey(19, 0)))},
		{"uncompressed bytes", stateChangeBlockPackUncompressedCounter.Snapshot().Count() - uncompressedBefore, int64(len(uncompressed))},
		{"compression saved", stateChangeBlockPackCompressionSavedCounter.Snapshot().Count() - compressionSavedBefore, int64(len(uncompressed) - len(packed))},
		{"compressed blocks", stateChangeBlockPackCompressedCounter.Snapshot().Count() - compressedBefore, 1},
		{"raw blocks", stateChangeBlockPackRawCounter.Snapshot().Count() - rawBefore, 0},
	}
	for _, check := range metricChecks {
		if check.got != check.want {
			t.Fatalf("%s metric delta = %d, want %d", check.name, check.got, check.want)
		}
	}
	if ok, err := db.Has(stateChangeSetKey(19, 1)); err != nil || ok {
		t.Fatalf("positive-sequence row present after block pack: ok=%v err=%v", ok, err)
	}

	got, ok, err := ReadStateDomainChange(db, 19, 64)
	if err != nil || !ok {
		t.Fatalf("read packed change = ok:%v err:%v", ok, err)
	}
	if got.Seq != 64 || got.TxNum != changes[63].TxNum || !bytes.Equal(got.Key, changes[63].Key) || !bytes.Equal(got.Prev, changes[63].Prev) || got.NextExists {
		t.Fatalf("packed change = %+v", got)
	}
	got.Prev[0] = 'x'
	reread, ok, err := ReadStateDomainChange(db, 19, 64)
	if err != nil || !ok || bytes.Equal(got.Prev, reread.Prev) {
		t.Fatalf("packed reread aliases decoded bytes: ok=%v err=%v", ok, err)
	}
	if _, ok, err := ReadStateDomainChange(db, 19, 129); err != nil || ok {
		t.Fatalf("out-of-range packed read = ok:%v err:%v", ok, err)
	}
	originalKey := append([]byte(nil), changes[63].Key...)
	if err := WriteStateDomainChangeInverseIndex(db, changes[63]); err != nil {
		t.Fatal(err)
	}
	override := *changes[63]
	override.Key = []byte("reward/repaired")
	override.Prev = []byte("repair-previous")
	if err := WriteStateDomainChange(db, &override); err != nil {
		t.Fatal(err)
	}
	got, ok, err = ReadStateDomainChange(db, 19, 64)
	if err != nil || !ok || !bytes.Equal(got.Key, override.Key) || !bytes.Equal(got.Prev, override.Prev) {
		t.Fatalf("positive row did not override packed sequence: got=%+v ok=%v err=%v", got, ok, err)
	}

	var seqs []uint64
	if err := IterateStateDomainChanges(db, 19, func(change *StateDomainChange) (bool, error) {
		seqs = append(seqs, change.Seq)
		if change.Seq == override.Seq && !bytes.Equal(change.Key, override.Key) {
			t.Fatalf("iteration did not apply packed-row override: %q", change.Key)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seqs) != len(changes) || seqs[0] != 1 || seqs[len(seqs)-1] != 128 {
		t.Fatalf("packed sequences = %v", seqs)
	}
	if err := DeleteStateDomainChanges(db, 19); err != nil {
		t.Fatal(err)
	}
	if ok, err := db.Has(stateChangeSetKey(19, 0)); err != nil || ok {
		t.Fatalf("packed row remains after delete: ok=%v err=%v", ok, err)
	}
	for _, key := range [][]byte{originalKey, override.Key} {
		var blocks []uint64
		if err := IterateStateDomainChangeBlocks(db, owner, 2, kvdomains.SystemReward, key, func(blockNum uint64) (bool, error) {
			blocks = append(blocks, blockNum)
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(blocks) != 0 {
			t.Fatalf("inverse blocks remain for %q after packed delete: %v", key, blocks)
		}
	}

	// The just-deployed v1 block pack had no compression envelope. Keep it
	// readable so a restart can span the P4.42/P4.43 boundary.
	rawDB := ethrawdb.NewMemoryDatabase()
	if err := rawDB.Put(stateChangeSetKey(19, 0), uncompressed); err != nil {
		t.Fatal(err)
	}
	rawRow, ok, err := ReadStateDomainChange(rawDB, 19, 64)
	if err != nil || !ok || !bytes.Equal(rawRow.Key, changes[63].Key) {
		t.Fatalf("read uncompressed block pack = %+v ok=%v err=%v", rawRow, ok, err)
	}
}

func TestStateDomainChangeBlockCompressedCorruption(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	bad := append(append([]byte(nil), stateDomainChangeBlockEnvelopeMagic[:]...), stateDomainChangeBlockSnappyVersion, 0xff)
	if err := db.Put(stateChangeSetKey(23, 0), bad); err != nil {
		t.Fatal(err)
	}
	if err := IterateStateDomainChanges(db, 23, func(*StateDomainChange) (bool, error) { return true, nil }); err == nil || !strings.Contains(err.Error(), "compressed state domain change block") {
		t.Fatalf("compressed corruption error = %v", err)
	}
	unknown := append(append([]byte(nil), stateDomainChangeBlockEnvelopeMagic[:]...), 0xff)
	if _, err := decodeStateDomainChangeBlockStorage(unknown); err == nil || !strings.Contains(err.Error(), "compression version") {
		t.Fatalf("unknown compression version error = %v", err)
	}
	var size [10]byte
	n := binary.PutUvarint(size[:], stateDomainChangeBlockMaxDecodedBytes+1)
	bomb := append(append(append([]byte(nil), stateDomainChangeBlockEnvelopeMagic[:]...), stateDomainChangeBlockSnappyVersion), size[:n]...)
	if _, err := decodeStateDomainChangeBlockStorage(bomb); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("oversize compressed block error = %v", err)
	}
}

func TestStateDomainChangeBlockCompressionGate(t *testing.T) {
	small := bytes.Repeat([]byte{0x11}, stateDomainChangeBlockCompressionMinBytes-1)
	if got, compressed := encodeStateDomainChangeBlockStorage(small); compressed || !bytes.Equal(got, small) {
		t.Fatal("sub-threshold state domain change block was compressed")
	}
	rng := rand.New(rand.NewSource(42))
	incompressible := make([]byte, 64<<10)
	_, _ = rng.Read(incompressible)
	if got, compressed := encodeStateDomainChangeBlockStorage(incompressible); compressed || !bytes.Equal(got, incompressible) {
		t.Fatalf("incompressible state domain change block was retained: compressed=%v bytes=%d", compressed, len(got))
	}
}

type discardStateDomainChangeWriter struct{}

func (discardStateDomainChangeWriter) Put([]byte, []byte) error { return nil }
func (discardStateDomainChangeWriter) Delete([]byte) error      { return nil }

func BenchmarkWriteStateDomainChangeBlockRows(b *testing.B) {
	const rows = 512
	changes := make([]*StateDomainChange, rows)
	owner := common.Address{0x41, 0x21}
	for i := range changes {
		prev := common.Keccak256([]byte(fmt.Sprintf("storage-prev/%04d", i)))
		changes[i] = &StateDomainChange{
			BlockNum:   21,
			TxNum:      1000 + uint64(i/8),
			Seq:        uint64(i + 1),
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 3,
			Domain:     kvdomains.ContractStorage,
			Key:        []byte(fmt.Sprintf("storage/%04d", i)),
			PrevExists: true,
			Prev:       append([]byte(nil), prev.Bytes()...),
		}
	}
	w := discardStateDomainChangeWriter{}
	probeDB := ethrawdb.NewMemoryDatabase()
	if err := WriteStateDomainChangeBlockRows(probeDB, changes); err != nil {
		b.Fatal(err)
	}
	probeStored, err := probeDB.Get(stateChangeSetKey(21, 0))
	if err != nil {
		b.Fatal(err)
	}
	probeRaw, err := decodeStateDomainChangeBlockStorage(probeStored)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("individual", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, change := range changes {
				if err := WriteStateDomainChangeRow(w, change); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("block-pack", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(100*float64(len(probeStored))/float64(len(probeRaw)), "stored/raw_%")
		for i := 0; i < b.N; i++ {
			if err := WriteStateDomainChangeBlockRows(w, changes); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkIterateStateTxRangesTailWindow(b *testing.B) {
	const (
		total  = uint64(100_000)
		window = uint64(5_000)
	)
	db, err := NewPebbleDB(b.TempDir(), 64, 64)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	batch := db.NewBatchWithSize(16 << 20)
	for blockNum := uint64(0); blockNum < total; blockNum++ {
		if err := WriteStateTxRange(batch, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			b.Fatal(err)
		}
		if batch.ValueSize() >= 16<<20 {
			if err := batch.Write(); err != nil {
				b.Fatal(err)
			}
			batch.Reset()
		}
	}
	if err := batch.Write(); err != nil {
		b.Fatal(err)
	}
	batch.Reset()
	fromBlock := total - window
	toBlock := total - 1

	b.Run("prefix-scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows := uint64(0)
			if err := IterateStateTxRanges(db, func(row *StateTxRange) (bool, error) {
				if row.BlockNum < fromBlock {
					return true, nil
				}
				if row.BlockNum > toBlock {
					return false, nil
				}
				rows++
				return true, nil
			}); err != nil {
				b.Fatal(err)
			}
			if rows != window {
				b.Fatalf("rows = %d, want %d", rows, window)
			}
		}
	})
	b.Run("bounded-seek", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(total)/float64(window), "logical_scan_reduction_x")
		for i := 0; i < b.N; i++ {
			rows := uint64(0)
			if err := IterateStateTxRangesByBlockRange(db, fromBlock, toBlock, func(*StateTxRange) (bool, error) {
				rows++
				return true, nil
			}); err != nil {
				b.Fatal(err)
			}
			if rows != window {
				b.Fatalf("rows = %d, want %d", rows, window)
			}
		}
	})
}

var benchmarkDecodedStateDomainChanges []*StateDomainChange

func BenchmarkDecodeStateDomainChangeBlockRows(b *testing.B) {
	const rows = 512
	changes := make([]*StateDomainChange, rows)
	owner := common.Address{0x41, 0x31}
	for i := range changes {
		prev := common.Keccak256([]byte(fmt.Sprintf("decode-prev/%04d", i)))
		changes[i] = &StateDomainChange{
			BlockNum: 31, TxNum: 2000 + uint64(i/8), Seq: uint64(i + 1),
			FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 3,
			Domain: kvdomains.ContractStorage, Key: []byte(fmt.Sprintf("storage/%04d", i)),
			PrevExists: true, Prev: append([]byte(nil), prev.Bytes()...),
		}
	}
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteStateDomainChangeBlockRows(db, changes); err != nil {
		b.Fatal(err)
	}
	compressed, err := db.Get(stateChangeSetKey(31, 0))
	if err != nil {
		b.Fatal(err)
	}
	raw, err := decodeStateDomainChangeBlockStorage(compressed)
	if err != nil || bytes.Equal(raw, compressed) {
		b.Fatalf("prepare compressed benchmark: raw=%d compressed=%d err=%v", len(raw), len(compressed), err)
	}
	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(100, "stored/raw_%")
		for i := 0; i < b.N; i++ {
			benchmarkDecodedStateDomainChanges, err = decodePersistedStateDomainChangeBlock(raw, 31)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("snappy", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(100*float64(len(compressed))/float64(len(raw)), "stored/raw_%")
		for i := 0; i < b.N; i++ {
			benchmarkDecodedStateDomainChanges, err = decodePersistedStateDomainChangeBlock(compressed, 31)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestWriteStateDomainChangeSamplesEncodingComponents(t *testing.T) {
	sequenceBefore := stateChangeEncodingSampleSequence.Swap(0)
	defer stateChangeEncodingSampleSequence.Store(sequenceBefore)
	db := ethrawdb.NewMemoryDatabase()
	change := &StateDomainChange{
		BlockNum:   11,
		BlockHash:  common.Hash{0x11},
		TxNum:      17,
		Seq:        3,
		FlatDomain: StateFlatDomainKVLatest,
		Owner:      common.Address{0x41, 0x11},
		Generation: 2,
		Domain:     kvdomains.ContractStorage,
		Key:        []byte("slot/key"),
		PrevExists: true,
		Prev:       []byte("previous-value"),
		NextExists: true,
		Next:       []byte("next-value"),
	}
	rowsBefore := stateChangeEncodingSampleRowsCounter.Snapshot().Count()
	encodedBefore := stateChangeEncodingSampleEncodedCounter.Snapshot().Count()
	keyBefore := stateChangeEncodingSampleKeyCounter.Snapshot().Count()
	prevBefore := stateChangeEncodingSamplePrevCounter.Snapshot().Count()
	nextBefore := stateChangeEncodingSampleNextCounter.Snapshot().Count()
	omittedNextBefore := stateChangeEncodingSampleOmittedNextCounter.Snapshot().Count()
	fixedBefore := stateChangeEncodingSampleFixedCounter.Snapshot().Count()
	prevRowsBefore := stateChangeEncodingSamplePrevRows.Snapshot().Count()
	nextRowsBefore := stateChangeEncodingSampleNextRows.Snapshot().Count()
	omittedNextRowsBefore := stateChangeEncodingSampleOmittedNextRows.Snapshot().Count()

	if err := WriteStateDomainChangeRow(db, change); err != nil {
		t.Fatal(err)
	}
	encoded, err := db.Get(stateChangeSetKey(change.BlockNum, change.Seq))
	if err != nil {
		t.Fatal(err)
	}
	legacyEncoded, err := rlp.EncodeToBytes(change)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= len(legacyEncoded) {
		t.Fatalf("previous-only row = %d bytes, want smaller than legacy %d", len(encoded), len(legacyEncoded))
	}
	priorEncoded, err := rlp.EncodeToBytes(&legacyPersistedStateDomainChange{
		BlockNum: change.BlockNum, BlockHash: change.BlockHash, TxNum: change.TxNum, Seq: change.Seq,
		FlatDomain: change.FlatDomain, Owner: change.Owner, Generation: change.Generation,
		Domain: change.Domain, Key: change.Key, PrevExists: change.PrevExists, Prev: change.Prev,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= len(priorEncoded) {
		t.Fatalf("context-hoisted row = %d bytes, want smaller than prior previous-only row %d", len(encoded), len(priorEncoded))
	}
	fixed := len(encoded) - len(change.Key) - len(change.Prev)
	checks := []struct {
		name   string
		before int64
		after  int64
		want   int64
	}{
		{name: "rows", before: rowsBefore, after: stateChangeEncodingSampleRowsCounter.Snapshot().Count(), want: 1},
		{name: "encoded", before: encodedBefore, after: stateChangeEncodingSampleEncodedCounter.Snapshot().Count(), want: int64(len(encoded))},
		{name: "key", before: keyBefore, after: stateChangeEncodingSampleKeyCounter.Snapshot().Count(), want: int64(len(change.Key))},
		{name: "prev", before: prevBefore, after: stateChangeEncodingSamplePrevCounter.Snapshot().Count(), want: int64(len(change.Prev))},
		{name: "next", before: nextBefore, after: stateChangeEncodingSampleNextCounter.Snapshot().Count(), want: 0},
		{name: "omitted next", before: omittedNextBefore, after: stateChangeEncodingSampleOmittedNextCounter.Snapshot().Count(), want: int64(len(change.Next))},
		{name: "fixed", before: fixedBefore, after: stateChangeEncodingSampleFixedCounter.Snapshot().Count(), want: int64(fixed)},
		{name: "prev rows", before: prevRowsBefore, after: stateChangeEncodingSamplePrevRows.Snapshot().Count(), want: 1},
		{name: "next rows", before: nextRowsBefore, after: stateChangeEncodingSampleNextRows.Snapshot().Count(), want: 0},
		{name: "omitted next rows", before: omittedNextRowsBefore, after: stateChangeEncodingSampleOmittedNextRows.Snapshot().Count(), want: 1},
	}
	for _, check := range checks {
		if got := check.after - check.before; got != check.want {
			t.Errorf("%s delta = %d, want %d", check.name, got, check.want)
		}
	}
}

func TestReadStateDomainChangeAcceptsLegacyNextImageRow(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	legacy := &StateDomainChange{
		BlockNum: 12, BlockHash: common.Hash{0x12}, TxNum: 20, Seq: 2,
		FlatDomain: StateFlatDomainKVLatest, Owner: common.Address{0x41, 0x12},
		Generation: 3, Domain: kvdomains.ContractStorage, Key: []byte("slot"),
		PrevExists: true, Prev: []byte("before"), NextExists: true, Next: []byte("after"),
	}
	encoded, err := rlp.EncodeToBytes(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateChangeSetKey(legacy.BlockNum, legacy.Seq), encoded); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadStateDomainChange(db, legacy.BlockNum, legacy.Seq)
	if err != nil || !ok || !bytes.Equal(got.Prev, legacy.Prev) || !bytes.Equal(got.Next, legacy.Next) {
		t.Fatalf("legacy row = %+v/%v/%v", got, ok, err)
	}
}

func TestReadStateDomainChangeAcceptsLegacyPreviousOnlyRow(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	legacy := &legacyPersistedStateDomainChange{
		BlockNum: 13, BlockHash: common.Hash{0x13}, TxNum: 21, Seq: 3,
		FlatDomain: StateFlatDomainKVLatest, Owner: common.Address{0x41, 0x13},
		Generation: 4, Domain: kvdomains.ContractStorage, Key: []byte("slot"),
		PrevExists: true, Prev: []byte("before"),
	}
	encoded, err := rlp.EncodeToBytes(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateChangeSetKey(legacy.BlockNum, legacy.Seq), encoded); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadStateDomainChange(db, legacy.BlockNum, legacy.Seq)
	if err != nil || !ok || got.BlockHash != legacy.BlockHash || !bytes.Equal(got.Prev, legacy.Prev) {
		t.Fatalf("legacy previous-only row = %+v/%v/%v", got, ok, err)
	}
}

func TestStateDomainChangeRowAndInverseIndexPublishSeparately(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x22}
	change := &StateDomainChange{
		BlockNum:   10,
		BlockHash:  common.Hash{0x10},
		TxNum:      42,
		Seq:        1,
		FlatDomain: StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 4,
		Domain:     kvdomains.SystemReward,
		Key:        []byte("reward/split"),
		PrevExists: true,
		Prev:       []byte("old"),
		NextExists: true,
		Next:       []byte("new"),
	}
	if err := WriteStateDomainChangeRow(db, change); err != nil {
		t.Fatalf("write row: %v", err)
	}
	if got, ok, err := ReadStateDomainChange(db, 10, 1); err != nil || !ok || !bytes.Equal(got.Prev, []byte("old")) || got.NextExists {
		t.Fatalf("read row = %+v ok:%v err:%v", got, ok, err)
	}
	var blocks []uint64
	if err := IterateStateDomainChangeBlocks(db, owner, 4, kvdomains.SystemReward, []byte("reward/split"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate before index: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("row-only publish created inverse blocks %v", blocks)
	}
	if err := WriteStateDomainChangeInverseIndex(db, change); err != nil {
		t.Fatalf("write inverse index: %v", err)
	}
	if err := IterateStateDomainChangeBlocks(db, owner, 4, kvdomains.SystemReward, []byte("reward/split"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate after index: %v", err)
	}
	if len(blocks) != 1 || blocks[0] != 10 {
		t.Fatalf("inverse blocks = %v, want [10]", blocks)
	}
}

func TestIterateStateDomainChangeBlocksByKeyDispatchesFlatDomains(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x23}
	changes := []*StateDomainChange{
		{
			BlockNum:   11,
			TxNum:      11,
			Seq:        1,
			FlatDomain: StateFlatDomainAccountLatest,
			Owner:      owner,
			NextExists: true,
			Next:       []byte("account"),
		},
		{
			BlockNum:   12,
			TxNum:      12,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 5,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/generic"),
			NextExists: true,
			Next:       []byte("kv"),
		},
		{
			BlockNum:   13,
			TxNum:      13,
			Seq:        1,
			FlatDomain: StateFlatDomainKVGeneration,
			Owner:      owner,
			NextExists: true,
			Next:       EncodeStateKVGenerationValue(5),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change %d: %v", change.BlockNum, err)
		}
	}
	tests := []struct {
		name       string
		flatDomain StateFlatDomain
		generation uint64
		domain     kvdomains.KVDomain
		key        []byte
		want       uint64
	}{
		{name: "account", flatDomain: StateFlatDomainAccountLatest, want: 11},
		{name: "kv", flatDomain: StateFlatDomainKVLatest, generation: 5, domain: kvdomains.SystemReward, key: []byte("reward/generic"), want: 12},
		{name: "generation", flatDomain: StateFlatDomainKVGeneration, want: 13},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var blocks []uint64
			if err := IterateStateDomainChangeBlocksByKey(db, tt.flatDomain, owner, tt.generation, tt.domain, tt.key, func(blockNum uint64) (bool, error) {
				blocks = append(blocks, blockNum)
				return true, nil
			}); err != nil {
				t.Fatalf("iterate: %v", err)
			}
			if len(blocks) != 1 || blocks[0] != tt.want {
				t.Fatalf("blocks = %v, want [%d]", blocks, tt.want)
			}
		})
	}
}

func TestIterateStateDomainChangesByKeyFiltersTxWindowAndKey(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x24}
	other := common.Address{0x41, 0x25}
	for _, row := range []StateTxRange{
		{BlockNum: 20, BlockHash: common.Hash{0x20}, BeginTxNum: 20, EndTxNum: 20},
		{BlockNum: 21, BlockHash: common.Hash{0x21}, BeginTxNum: 21, EndTxNum: 21},
		{BlockNum: 22, BlockHash: common.Hash{0x22}, BeginTxNum: 22, EndTxNum: 22},
	} {
		if err := WriteStateTxRange(db, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
			t.Fatalf("write range %d: %v", row.BlockNum, err)
		}
	}
	changes := []*StateDomainChange{
		{
			BlockNum:   20,
			TxNum:      20,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/a"),
			NextExists: true,
			Next:       []byte("too-old"),
		},
		{
			BlockNum:   21,
			TxNum:      21,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/a"),
			NextExists: true,
			Next:       []byte("match"),
		},
		{
			BlockNum:   21,
			TxNum:      21,
			Seq:        2,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      other,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/a"),
			NextExists: true,
			Next:       []byte("other-owner"),
		},
		{
			BlockNum:   22,
			TxNum:      22,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/b"),
			NextExists: true,
			Next:       []byte("other-key"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change %+v: %v", change, err)
		}
	}
	var got []*StateDomainChange
	if err := IterateStateDomainChangesByKey(db, 20, 21, StateFlatDomainKVLatest, owner, 1, kvdomains.SystemReward, []byte("reward/a"), func(change *StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate changes: %v", err)
	}
	if len(got) != 1 || got[0].BlockNum != 21 || !bytes.Equal(got[0].Key, []byte("reward/a")) {
		t.Fatalf("changes = %+v, want only block 21 match", got)
	}
}

func TestIterateStateDomainChangesByKeyBlockRangeSeeksPastOldHistory(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x27}
	for _, blockNum := range []uint64{1, 100, 101} {
		if err := WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatalf("write range %d: %v", blockNum, err)
		}
		if err := WriteStateDomainChange(db, &StateDomainChange{
			BlockNum:   blockNum,
			TxNum:      blockNum,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/every-block"),
			NextExists: true,
			Next:       []byte{byte(blockNum)},
		}); err != nil {
			t.Fatalf("write change %d: %v", blockNum, err)
		}
	}
	// A bounded iterator must neither point-read history before its lower
	// block bound nor continue beyond its upper bound. It also does not need
	// StateTxRange for the in-range block: the block bounds already imply that
	// every candidate change is inside the requested end-of-block tx window.
	// Malformed rows on all three blocks make any accidental range read fail
	// deterministically.
	if err := db.Put(stateTxRangeKey(1), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateTxRangeKey(100), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateTxRangeKey(101), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	var got []*StateDomainChange
	if err := IterateStateDomainChangesByKeyBlockRange(db, 99, 100, 99, 100, StateFlatDomainKVLatest, owner, 1, kvdomains.SystemReward, []byte("reward/every-block"), func(change *StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate bounded changes: %v", err)
	}
	if len(got) != 1 || got[0].BlockNum != 100 {
		t.Fatalf("bounded changes = %+v, want only block 100", got)
	}
}

func TestReadStateKVAsOfTxNumStopsAtFirstSubsequentChange(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x28}
	key := []byte("reward/frequent")
	for _, blockNum := range []uint64{2, 3} {
		if err := WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatalf("write range %d: %v", blockNum, err)
		}
		if err := WriteStateDomainChange(db, &StateDomainChange{
			BlockNum:   blockNum,
			TxNum:      blockNum,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte{byte(blockNum - 2)},
			NextExists: true,
			Next:       []byte{byte(blockNum - 1)},
		}); err != nil {
			t.Fatalf("write change %d: %v", blockNum, err)
		}
	}
	// The second block remains in the inverse index but its changeset row is
	// unreadable. A point-in-time read at tx 1 only needs block 2's Prev and
	// must stop before touching block 3.
	if err := db.Put(stateChangeSetKey(3, 1), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	value, ok, err := ReadStateKVAsOfTxNum(db, owner, 1, kvdomains.SystemReward, key, 1, 3)
	if err != nil {
		t.Fatalf("read as of first change: %v", err)
	}
	if !ok || !bytes.Equal(value, []byte{0}) {
		t.Fatalf("value = %x ok=%v, want 00/true", value, ok)
	}
}

func TestReadFirstStateDomainChangeByKeyBlockRangeUsesPrefixSeek(t *testing.T) {
	base := ethrawdb.NewMemoryDatabase()
	db := &prefixSeekingHistoryDB{Database: base}
	owner := common.Address{0x41, 0x29}
	key := []byte("reward/seek")
	for _, blockNum := range []uint64{100, 101} {
		if err := WriteStateDomainChange(db, &StateDomainChange{
			BlockNum: blockNum, TxNum: blockNum, Seq: 1,
			FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 1,
			Domain: kvdomains.SystemReward, Key: key,
			PrevExists: true, Prev: []byte{byte(blockNum)}, NextExists: true, Next: []byte{byte(blockNum + 1)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	change, err := ReadFirstStateDomainChangeByKeyBlockRange(db, 99, 101, 99, 101, StateFlatDomainKVLatest, owner, 1, kvdomains.SystemReward, key)
	if err != nil {
		t.Fatal(err)
	}
	if change == nil || change.BlockNum != 100 {
		t.Fatalf("first change = %+v, want block 100", change)
	}
	if db.seekCalls != 1 || db.inverseIteratorCalls != 0 {
		t.Fatalf("seek calls = %d inverse iterator calls = %d, want 1/0", db.seekCalls, db.inverseIteratorCalls)
	}
}

func TestReadFirstStateDomainChangeByKeyBlockRangeUsesDurableSeekForStagedIndex(t *testing.T) {
	base := ethrawdb.NewMemoryDatabase()
	db := &prefixSeekingHistoryDB{Database: base}
	owner := common.Address{0x41, 0x2b}
	key := []byte("reward/durable-seek")
	if err := WriteStateDomainChange(db, &StateDomainChange{
		BlockNum: 100, TxNum: 100, Seq: 1,
		FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 1,
		Domain: kvdomains.SystemReward, Key: key,
		PrevExists: true, Prev: []byte("before"), NextExists: true, Next: []byte("after"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := WriteStageProgressWithHash(db, StageStateHistoryIndex, 100, common.Hash{100}); err != nil {
		t.Fatal(err)
	}
	change, err := ReadFirstStateDomainChangeByKeyBlockRange(db, 99, 100, 99, 100, StateFlatDomainKVLatest, owner, 1, kvdomains.SystemReward, key)
	if err != nil {
		t.Fatal(err)
	}
	if change == nil || change.BlockNum != 100 || !bytes.Equal(change.Prev, []byte("before")) {
		t.Fatalf("first change = %+v, want durable block 100", change)
	}
	if db.durableSeekCalls != 1 || db.seekCalls != 0 || db.inverseIteratorCalls != 0 {
		t.Fatalf("durable/logical/inverse iterator calls = %d/%d/%d, want 1/0/0", db.durableSeekCalls, db.seekCalls, db.inverseIteratorCalls)
	}
}

func TestStateDomainChangeBlockRangeReadsScanUnindexedStageTail(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x2a}
	key := []byte("reward/staged-tail")
	for _, blockNum := range []uint64{1, 2} {
		if err := WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
		change := &StateDomainChange{
			BlockNum: blockNum, TxNum: blockNum, Seq: 1,
			FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 1,
			Domain: kvdomains.SystemReward, Key: key,
			PrevExists: true, Prev: []byte{byte(blockNum - 1)},
		}
		if blockNum == 1 {
			if err := WriteStateDomainChange(db, change); err != nil {
				t.Fatal(err)
			}
		} else if err := WriteStateDomainChangeRow(db, change); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteStageProgressWithHash(db, StageStateHistoryIndex, 1, common.Hash{1}); err != nil {
		t.Fatal(err)
	}

	var exact []uint64
	if err := IterateStateDomainChangesByKeyBlockRange(db, 0, 2, 0, 2, StateFlatDomainKVLatest, owner, 1, kvdomains.SystemReward, key, func(change *StateDomainChange) (bool, error) {
		exact = append(exact, change.BlockNum)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(exact, []uint64{1, 2}) {
		t.Fatalf("exact indexed+tail blocks = %v, want [1 2]", exact)
	}
	first, err := ReadFirstStateDomainChangeByKeyBlockRange(db, 1, 2, 1, 2, StateFlatDomainKVLatest, owner, 1, kvdomains.SystemReward, key)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || first.BlockNum != 2 {
		t.Fatalf("first unindexed tail change = %+v, want block 2", first)
	}
	var prefixed []uint64
	if err := IterateStateDomainChangesByPrefixBlockRange(db, 0, 2, 0, 2, owner, 1, kvdomains.SystemReward, []byte("reward/staged"), func(change *StateDomainChange) (bool, error) {
		prefixed = append(prefixed, change.BlockNum)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(prefixed, []uint64{1, 2}) {
		t.Fatalf("prefix indexed+tail blocks = %v, want [1 2]", prefixed)
	}
}

func TestReadFirstStateKVChangesByKeysScansUnindexedTailOnce(t *testing.T) {
	base := ethrawdb.NewMemoryDatabase()
	db := &prefixSeekingHistoryDB{Database: base}
	owner := common.Address{0x41, 0x2c}
	keys := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	changes := make([]*StateDomainChange, 0, len(keys))
	for i, key := range keys {
		changes = append(changes, &StateDomainChange{
			BlockNum: 2, TxNum: 2, Seq: uint64(i + 1),
			FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 1,
			Domain: kvdomains.SystemDynamicProperty, Key: key,
			PrevExists: true, Prev: []byte("old-" + string(key)),
		})
	}
	if err := WriteStateDomainChangeBlockRows(db, changes); err != nil {
		t.Fatal(err)
	}
	if err := WriteStageProgressWithHash(db, StageStateHistoryIndex, 1, common.Hash{1}); err != nil {
		t.Fatal(err)
	}

	first, err := ReadFirstStateKVChangesByKeysBlockRange(db, 1, 2, 1, 2, owner, 1, kvdomains.SystemDynamicProperty, keys)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		change := first[string(key)]
		if change == nil || change.BlockNum != 2 || !bytes.Equal(change.Prev, []byte("old-"+string(key))) {
			t.Fatalf("first[%q] = %+v", key, change)
		}
	}
	if db.changeRangeIteratorCalls != 1 || db.changeBlockIteratorCalls != 0 {
		t.Fatalf("tail range/block iterator calls = %d/%d, want 1/0 for %d keys", db.changeRangeIteratorCalls, db.changeBlockIteratorCalls, len(keys))
	}
}

func TestIterateStateDomainChangesByBlockRangePreservesLogicalRowsAndBounds(t *testing.T) {
	base := ethrawdb.NewMemoryDatabase()
	db := &prefixSeekingHistoryDB{Database: base}
	owner := common.Address{0x41, 0x2e}
	change := func(blockNum, seq uint64, key, prev string) *StateDomainChange {
		return &StateDomainChange{
			BlockNum: blockNum, TxNum: blockNum, Seq: seq,
			FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 1,
			Domain: kvdomains.SystemDynamicProperty, Key: []byte(key),
			PrevExists: true, Prev: []byte(prev),
		}
	}
	if err := WriteStateDomainChangeRow(db, change(1, 1, "before", "block-1")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateDomainChangeBlockRows(db, []*StateDomainChange{
		change(2, 1, "one", "packed-one"),
		change(2, 2, "two", "packed-two"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateDomainChangeRow(db, change(2, 2, "two", "repair-two")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateDomainChangeRow(db, change(2, 4, "four", "extra-four")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateDomainChangeRow(db, change(3, 1, "three", "block-three")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateDomainChangeRow(db, change(4, 1, "after", "block-4")); err != nil {
		t.Fatal(err)
	}

	var got []string
	if err := IterateStateDomainChangesByBlockRange(db, 2, 3, func(row *StateDomainChange) (bool, error) {
		got = append(got, fmt.Sprintf("%d/%d/%s/%s", row.BlockNum, row.Seq, row.Key, row.Prev))
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"2/1/one/packed-one",
		"2/2/two/repair-two",
		"2/4/four/extra-four",
		"3/1/three/block-three",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("range rows = %v, want %v", got, want)
	}
	if db.changeRangeIteratorCalls != 1 || db.changeBlockIteratorCalls != 0 {
		t.Fatalf("range/block iterator calls = %d/%d, want 1/0", db.changeRangeIteratorCalls, db.changeBlockIteratorCalls)
	}

	seen := 0
	if err := IterateStateDomainChangesByBlockRange(db, 2, 3, func(*StateDomainChange) (bool, error) {
		seen++
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Fatalf("early-stop rows = %d, want 1", seen)
	}
}

func BenchmarkReadFirstStateKVChangesUnindexedTail(b *testing.B) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x2d}
	const (
		indexedHead = uint64(1)
		headBlock   = uint64(64)
		keyCount    = 128
	)
	keys := make([][]byte, keyCount)
	changes := make([]*StateDomainChange, keyCount)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("dynamic/%03d", i))
		changes[i] = &StateDomainChange{
			BlockNum: headBlock, TxNum: headBlock, Seq: uint64(i + 1),
			FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 1,
			Domain: kvdomains.SystemDynamicProperty, Key: keys[i],
			PrevExists: true, Prev: []byte("old"),
		}
	}
	if err := WriteStateDomainChangeBlockRows(db, changes); err != nil {
		b.Fatal(err)
	}
	if err := WriteStageProgressWithHash(db, StageStateHistoryIndex, indexedHead, common.Hash{1}); err != nil {
		b.Fatal(err)
	}
	b.Run("point", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, key := range keys {
				change, err := ReadFirstStateDomainChangeByKeyBlockRange(db, indexedHead, headBlock, indexedHead, headBlock, StateFlatDomainKVLatest, owner, 1, kvdomains.SystemDynamicProperty, key)
				if err != nil || change == nil {
					b.Fatalf("point change = %+v err=%v", change, err)
				}
			}
		}
	})
	b.Run("batch", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			first, err := ReadFirstStateKVChangesByKeysBlockRange(db, indexedHead, headBlock, indexedHead, headBlock, owner, 1, kvdomains.SystemDynamicProperty, keys)
			if err != nil || len(first) != len(keys) {
				b.Fatalf("batch changes = %d err=%v", len(first), err)
			}
		}
	})
}

func BenchmarkIterateStateDomainChangesUnindexedTail(b *testing.B) {
	const (
		tailBlocks   = uint64(2_800)
		rowsPerBlock = 8
	)
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x2f}
	for blockNum := uint64(1); blockNum <= tailBlocks; blockNum++ {
		changes := make([]*StateDomainChange, rowsPerBlock)
		for row := range changes {
			changes[row] = &StateDomainChange{
				BlockNum: blockNum, TxNum: blockNum * rowsPerBlock, Seq: uint64(row + 1),
				FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 1,
				Domain:     kvdomains.SystemDynamicProperty,
				Key:        []byte(fmt.Sprintf("dynamic/%03d", row)),
				PrevExists: true, Prev: []byte("old"),
			}
		}
		if err := WriteStateDomainChangeBlockRows(db, changes); err != nil {
			b.Fatal(err)
		}
	}
	want := int(tailBlocks) * rowsPerBlock
	b.Run("iterator-per-block", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			seen := 0
			for blockNum := uint64(1); blockNum <= tailBlocks; blockNum++ {
				if err := IterateStateDomainChanges(db, blockNum, func(*StateDomainChange) (bool, error) {
					seen++
					return true, nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			if seen != want {
				b.Fatalf("rows = %d, want %d", seen, want)
			}
		}
	})
	b.Run("range-iterator", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			seen := 0
			if err := IterateStateDomainChangesByBlockRange(db, 1, tailBlocks, func(*StateDomainChange) (bool, error) {
				seen++
				return true, nil
			}); err != nil {
				b.Fatal(err)
			}
			if seen != want {
				b.Fatalf("rows = %d, want %d", seen, want)
			}
		}
	})
}

type prefixSeekingHistoryDB struct {
	ethdb.Database
	seekCalls                int
	durableSeekCalls         int
	inverseIteratorCalls     int
	changeBlockIteratorCalls int
	changeRangeIteratorCalls int
}

func (db *prefixSeekingHistoryDB) SeekPrefix(prefix, start []byte) (key, value []byte, ok bool, err error) {
	db.seekCalls++
	it := db.Database.NewIterator(prefix, start)
	defer it.Release()
	if !it.Next() {
		return nil, nil, false, it.Error()
	}
	return append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...), true, nil
}

func (db *prefixSeekingHistoryDB) SeekDurablePrefix(prefix, start []byte) (key, value []byte, ok bool, err error) {
	db.durableSeekCalls++
	it := db.Database.NewIterator(prefix, start)
	defer it.Release()
	if !it.Next() {
		return nil, nil, false, it.Error()
	}
	return append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...), true, nil
}

func (db *prefixSeekingHistoryDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	if bytes.HasPrefix(prefix, stateChangeInversePrefix) {
		db.inverseIteratorCalls++
	}
	if bytes.HasPrefix(prefix, stateChangeSetPrefix) && len(prefix) == len(stateChangeSetPrefix)+8 {
		db.changeBlockIteratorCalls++
	}
	if bytes.Equal(prefix, stateChangeSetPrefix) {
		db.changeRangeIteratorCalls++
	}
	return db.Database.NewIterator(prefix, start)
}

func TestIterateStateDomainChangesByPrefixFiltersTxWindowAndPrefix(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x26}
	for _, row := range []StateTxRange{
		{BlockNum: 30, BlockHash: common.Hash{0x30}, BeginTxNum: 30, EndTxNum: 30},
		{BlockNum: 31, BlockHash: common.Hash{0x31}, BeginTxNum: 31, EndTxNum: 31},
	} {
		if err := WriteStateTxRange(db, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
			t.Fatalf("write range %d: %v", row.BlockNum, err)
		}
	}
	changes := []*StateDomainChange{
		{
			BlockNum:   30,
			TxNum:      30,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 2,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("acct/a"),
			NextExists: true,
			Next:       []byte("too-old"),
		},
		{
			BlockNum:   31,
			TxNum:      31,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 2,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("acct/a"),
			NextExists: true,
			Next:       []byte("a"),
		},
		{
			BlockNum:   31,
			TxNum:      31,
			Seq:        2,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 2,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("other/b"),
			NextExists: true,
			Next:       []byte("b"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change %+v: %v", change, err)
		}
	}
	var got []*StateDomainChange
	if err := IterateStateDomainChangesByPrefix(db, 30, 31, owner, 2, kvdomains.SystemReward, []byte("acct/"), func(change *StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate prefix changes: %v", err)
	}
	if len(got) != 1 || got[0].BlockNum != 31 || string(got[0].Key) != "acct/a" {
		t.Fatalf("prefix changes = %+v, want only acct/a at block 31", got)
	}
}

func TestIterateStateDomainChangesByTxRangeSameBlock(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x21}
	begin, end, err := NextStateTxRange(100, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteStateTxRange(db, 12, common.Hash{0x12}, begin, end); err != nil {
		t.Fatal(err)
	}
	for i, txNum := range []uint64{begin, begin + 1, end} {
		if err := WriteStateDomainChange(db, &StateDomainChange{
			BlockNum:   12,
			BlockHash:  common.Hash{0x12},
			TxNum:      txNum,
			Seq:        uint64(i + 1),
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        []byte{byte('a' + i)},
			NextExists: true,
			Next:       []byte{byte('1' + i)},
		}); err != nil {
			t.Fatalf("write change %d: %v", i, err)
		}
	}

	var got []*StateDomainChange
	if err := IterateStateDomainChangesByTxRange(db, begin+1, begin+1, func(change *StateDomainChange) (bool, error) {
		got = append(got, cloneStateDomainChange(change))
		return true, nil
	}); err != nil {
		t.Fatalf("iterate tx range: %v", err)
	}
	if len(got) != 1 || got[0].Seq != 2 || got[0].BlockNum != 12 || got[0].BlockHash != (common.Hash{0x12}) {
		t.Fatalf("changes in tx range = %+v, want block 12 hash 12 seq 2", got)
	}
}

func TestIterateStateTxRangesByBlockRangeSeeksAndStops(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := db.Put(stateTxRangeKey(1), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	for _, blockNum := range []uint64{2, 3} {
		if err := WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum*10, blockNum*10+1); err != nil {
			t.Fatalf("write range %d: %v", blockNum, err)
		}
	}
	if err := db.Put(stateTxRangeKey(4), []byte{0xff}); err != nil {
		t.Fatal(err)
	}

	var got []uint64
	if err := IterateStateTxRangesByBlockRange(db, 2, 3, func(row *StateTxRange) (bool, error) {
		got = append(got, row.BlockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate bounded tx ranges: %v", err)
	}
	if !slices.Equal(got, []uint64{2, 3}) {
		t.Fatalf("bounded tx ranges = %v, want [2 3]", got)
	}
}

func TestStateDomainChangeRejectsUntypedRows(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	err := WriteStateDomainChange(db, &StateDomainChange{
		BlockNum: 1,
		TxNum:    1,
		Seq:      1,
		Owner:    common.Address{0x41, 0x01},
		Domain:   kvdomains.SystemReward,
		Key:      []byte("legacy"),
	})
	if err == nil {
		t.Fatal("untyped generic KV changeset row accepted")
	}
}

func TestDeleteStateDomainChangesUsesPointDeletes(t *testing.T) {
	db := &rangeDeleteCountingStore{KeyValueStore: ethrawdb.NewMemoryDatabase()}
	owner := common.Address{0x41, 0x01}
	for seq, key := range [][]byte{[]byte("reward/1"), []byte("reward/2")} {
		if err := WriteStateDomainChange(db, &StateDomainChange{
			BlockNum:   9,
			BlockHash:  common.Hash{0x09},
			TxNum:      9,
			Seq:        uint64(seq + 1),
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 3,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("old"),
			NextExists: true,
			Next:       []byte("new"),
		}); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	if err := DeleteStateDomainChanges(db, 9); err != nil {
		t.Fatalf("delete changes: %v", err)
	}
	if db.rangeDeletes != 0 {
		t.Fatalf("DeleteStateDomainChanges used DeleteRange %d time(s)", db.rangeDeletes)
	}
	rows := 0
	if err := IterateStateDomainChanges(db, 9, func(change *StateDomainChange) (bool, error) {
		rows++
		return true, nil
	}); err != nil {
		t.Fatalf("iterate changes: %v", err)
	}
	if rows != 0 {
		t.Fatalf("forward changes survived: %d", rows)
	}
	var blocks []uint64
	if err := IterateStateDomainChangeBlocks(db, owner, 3, kvdomains.SystemReward, []byte("reward/1"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate inverse: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("inverse blocks survived: %v", blocks)
	}
}

func TestDeleteStateDomainChangesDoesNotRescanDeferredDeletes(t *testing.T) {
	base := ethrawdb.NewMemoryDatabase()
	owner := common.Address{common.AddressPrefixMainnet, 0x01}
	rows := resetScanBatch + 1
	for seq := 1; seq <= rows; seq++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(seq))
		if err := WriteStateDomainChange(base, &StateDomainChange{
			BlockNum:   9,
			BlockHash:  common.Hash{0x09},
			TxNum:      uint64(seq),
			Seq:        uint64(seq),
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 3,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("old"),
			NextExists: true,
			Next:       []byte("new"),
		}); err != nil {
			t.Fatalf("write change %d: %v", seq, err)
		}
	}

	// Reads and iterators deliberately see only base. Deletes remain invisible
	// until Flush, matching pruning's committed-reader/uncommitted-batch store.
	deferred := newDeferredDeleteStateStore(base, rows*2+100)
	if err := DeleteStateDomainChanges(deferred, 9); err != nil {
		t.Fatalf("delete deferred state changes: %v", err)
	}
	if deferred.deleteCalls != rows*2 {
		t.Fatalf("delete calls = %d, want %d", deferred.deleteCalls, rows*2)
	}
	if err := deferred.Flush(); err != nil {
		t.Fatalf("flush deferred deletes: %v", err)
	}
	remaining := 0
	if err := IterateStateDomainChanges(base, 9, func(*StateDomainChange) (bool, error) {
		remaining++
		return true, nil
	}); err != nil {
		t.Fatalf("iterate remaining changes: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("remaining state changes = %d", remaining)
	}
}

type deferredDeleteStateStore struct {
	base           ethdb.KeyValueStore
	pending        ethdb.Batch
	deleteCalls    int
	maxDeleteCalls int
}

func newDeferredDeleteStateStore(base ethdb.KeyValueStore, maxDeleteCalls int) *deferredDeleteStateStore {
	return &deferredDeleteStateStore{
		base:           base,
		pending:        base.NewBatch(),
		maxDeleteCalls: maxDeleteCalls,
	}
}

func (s *deferredDeleteStateStore) Has(key []byte) (bool, error) {
	return s.base.Has(key)
}

func (s *deferredDeleteStateStore) Get(key []byte) ([]byte, error) {
	return s.base.Get(key)
}

func (s *deferredDeleteStateStore) Put(key, value []byte) error {
	return s.base.Put(key, value)
}

func (s *deferredDeleteStateStore) Delete(key []byte) error {
	s.deleteCalls++
	if s.maxDeleteCalls > 0 && s.deleteCalls > s.maxDeleteCalls {
		return fmt.Errorf("deferred delete limit exceeded: %d", s.deleteCalls)
	}
	return s.pending.Delete(key)
}

func (s *deferredDeleteStateStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	return s.base.NewIterator(prefix, start)
}

func (s *deferredDeleteStateStore) Flush() error {
	defer s.pending.Reset()
	return s.pending.Write()
}

type rangeDeleteCountingStore struct {
	ethdb.KeyValueStore
	rangeDeletes int
}

func (db *rangeDeleteCountingStore) DeleteRange(start, end []byte) error {
	db.rangeDeletes++
	return db.KeyValueStore.DeleteRange(start, end)
}

func TestReadStateKVAsOfRollsBackChanges(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	key := []byte("history/key")

	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemReward, key, []byte("v7"))
	changes := []*StateDomainChange{
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("v2"),
			NextExists: true,
			Next:       []byte("v3"),
		},
		{
			BlockNum:   5,
			TxNum:      5,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("v3"),
			NextExists: true,
			Next:       []byte("v5"),
		},
		{
			BlockNum:   7,
			TxNum:      7,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("v5"),
			NextExists: true,
			Next:       []byte("v7"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	tests := []struct {
		block uint64
		want  []byte
	}{
		{7, []byte("v7")},
		{6, []byte("v5")},
		{5, []byte("v5")},
		{4, []byte("v3")},
		{3, []byte("v3")},
		{2, []byte("v2")},
	}
	for _, tt := range tests {
		got, ok, err := ReadStateKVAsOf(db, owner, 0, kvdomains.SystemReward, key, tt.block, 7)
		if err != nil || !ok || !bytes.Equal(got, tt.want) {
			t.Fatalf("as-of block %d = %q ok:%v err:%v, want %q", tt.block, got, ok, err, tt.want)
		}
	}
}

func TestReadStateAccountLatestAsOfTxNum(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x42}
	begin, end, err := NextStateTxRange(100, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteStateTxRange(db, 15, common.Hash{0x15}, begin, end); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateAccountLatest(db, owner, []byte("account-v2")); err != nil {
		t.Fatal(err)
	}
	changes := []*StateDomainChange{
		{
			BlockNum:   15,
			BlockHash:  common.Hash{0x15},
			TxNum:      begin,
			Seq:        1,
			FlatDomain: StateFlatDomainAccountLatest,
			Owner:      owner,
			NextExists: true,
			Next:       []byte("account-v1"),
		},
		{
			BlockNum:   15,
			BlockHash:  common.Hash{0x15},
			TxNum:      begin + 1,
			Seq:        2,
			FlatDomain: StateFlatDomainAccountLatest,
			Owner:      owner,
			PrevExists: true,
			Prev:       []byte("account-v1"),
			NextExists: true,
			Next:       []byte("account-v2"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := ReadStateAccountLatestAsOfTxNum(db, owner, begin, end)
	if err != nil || !ok || !bytes.Equal(got, []byte("account-v1")) {
		t.Fatalf("account as-of tx0 = %q ok=%v err=%v", got, ok, err)
	}
	got, ok, err = ReadStateAccountLatestAsOfTxNum(db, owner, begin+1, end)
	if err != nil || !ok || !bytes.Equal(got, []byte("account-v2")) {
		t.Fatalf("account as-of tx1 = %q ok=%v err=%v", got, ok, err)
	}
	got, ok, err = ReadStateAccountLatestAsOfTxNum(db, owner, begin-1, end)
	if err != nil || ok {
		t.Fatalf("account before creation = %q ok=%v err=%v", got, ok, err)
	}
}

func TestReadStateKVAsOfHandlesCreatedKey(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	key := []byte("created")
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemReward, key, []byte("new"))
	if err := WriteStateDomainChange(db, &StateDomainChange{
		BlockNum:   4,
		TxNum:      4,
		Seq:        1,
		FlatDomain: StateFlatDomainKVLatest,
		Owner:      owner,
		Domain:     kvdomains.SystemReward,
		Key:        key,
		NextExists: true,
		Next:       []byte("new"),
	}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadStateKVAsOf(db, owner, 0, kvdomains.SystemReward, key, 3, 4); err != nil || ok {
		t.Fatalf("created key before creation = %q ok:%v err:%v", got, ok, err)
	}
}

func TestReadStateKVAsOfTxNumWithinBlock(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x22}
	domain := kvdomains.SystemReward
	key := []byte("txnum/key")
	begin, end, err := NextStateTxRange(100, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteStateTxRange(db, 13, common.Hash{0x13}, begin, end); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, db, owner, 0, domain, key, []byte("v2"))
	changes := []*StateDomainChange{
		{
			BlockNum:   13,
			TxNum:      begin,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        key,
			NextExists: true,
			Next:       []byte("v1"),
		},
		{
			BlockNum:   13,
			TxNum:      begin + 1,
			Seq:        2,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("v1"),
			NextExists: true,
			Next:       []byte("v2"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	tests := []struct {
		target uint64
		want   string
		ok     bool
	}{
		{end, "v2", true},
		{begin + 1, "v2", true},
		{begin, "v1", true},
		{begin - 1, "", false},
	}
	for _, tt := range tests {
		got, ok, err := ReadStateKVAsOfTxNum(db, owner, 0, domain, key, tt.target, end)
		if err != nil || ok != tt.ok || string(got) != tt.want {
			t.Fatalf("as-of tx %d = %q ok:%v err:%v, want %q ok:%v", tt.target, got, ok, err, tt.want, tt.ok)
		}
	}
}

func TestIterateStateKVAsOfPrefixRollsBackRange(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	domain := kvdomains.SystemReward

	mustWriteStateKVLatest(t, db, owner, 0, domain, []byte("acct/a"), []byte("a3"))
	mustWriteStateKVLatest(t, db, owner, 0, domain, []byte("acct/b"), []byte("b3"))
	mustWriteStateKVLatest(t, db, owner, 0, domain, []byte("other/c"), []byte("c3"))
	changes := []*StateDomainChange{
		{
			BlockNum:   2,
			TxNum:      2,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        []byte("acct/a"),
			PrevExists: true,
			Prev:       []byte("a1"),
			NextExists: true,
			Next:       []byte("a2"),
		},
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        []byte("acct/a"),
			PrevExists: true,
			Prev:       []byte("a2"),
			NextExists: true,
			Next:       []byte("a3"),
		},
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        2,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        []byte("acct/b"),
			NextExists: true,
			Next:       []byte("b3"),
		},
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        3,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        []byte("other/c"),
			PrevExists: true,
			Prev:       []byte("c2"),
			NextExists: true,
			Next:       []byte("c3"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	got := make(map[string]string)
	if err := IterateStateKVAsOfPrefix(db, owner, 0, domain, []byte("acct/"), 2, 3, func(key, value []byte) (bool, error) {
		got[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate as-of prefix: %v", err)
	}
	if len(got) != 1 || got["acct/a"] != "a2" {
		t.Fatalf("as-of prefix at block 2 = %v, want only acct/a=a2", got)
	}

	got = make(map[string]string)
	if err := IterateStateKVAsOfPrefix(db, owner, 0, domain, []byte("acct/"), 3, 3, func(key, value []byte) (bool, error) {
		got[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate head prefix: %v", err)
	}
	if len(got) != 2 || got["acct/a"] != "a3" || got["acct/b"] != "b3" {
		t.Fatalf("as-of prefix at head = %v", got)
	}
}

func TestReadStateAccountKVAsOfCrossesGenerationReset(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x02}
	domain := kvdomains.SystemReward
	key := []byte("cycle")

	if err := WriteStateKVGeneration(db, owner, 1); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, db, owner, 0, domain, key, []byte("old2"))
	mustWriteStateKVLatest(t, db, owner, 1, domain, key, []byte("new"))
	changes := []*StateDomainChange{
		{
			BlockNum:   2,
			TxNum:      2,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 0,
			Domain:     domain,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("old1"),
			NextExists: true,
			Next:       []byte("old2"),
		},
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        1,
			FlatDomain: StateFlatDomainKVGeneration,
			Owner:      owner,
			NextExists: true,
			Next:       EncodeStateKVGenerationValue(1),
		},
		{
			BlockNum:   4,
			TxNum:      4,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     domain,
			Key:        key,
			NextExists: true,
			Next:       []byte("new"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	tests := []struct {
		block uint64
		want  string
		ok    bool
	}{
		{4, "new", true},
		{3, "", false},
		{2, "old2", true},
		{1, "old1", true},
	}
	for _, tt := range tests {
		got, ok, err := ReadStateAccountKVAsOf(db, owner, domain, key, tt.block, 4)
		if err != nil || ok != tt.ok || string(got) != tt.want {
			t.Fatalf("account kv as-of block %d = %q ok:%v err:%v, want %q ok:%v", tt.block, got, ok, err, tt.want, tt.ok)
		}
	}
	if gen, ok, err := ReadStateKVGenerationAsOf(db, owner, 2, 4); err != nil || ok || gen != 0 {
		t.Fatalf("generation as-of block 2 = %d ok:%v err:%v, want default 0 without row", gen, ok, err)
	}
}

func TestReadStateAccountKVAsOfTxNumCrossesGenerationResetWithinBlock(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x23}
	domain := kvdomains.SystemReward
	key := []byte("generation/txnum")
	begin, end, err := NextStateTxRange(100, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteStateTxRange(db, 14, common.Hash{0x14}, begin, end); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateKVGeneration(db, owner, 1); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, db, owner, 0, domain, key, []byte("old"))
	mustWriteStateKVLatest(t, db, owner, 1, domain, key, []byte("new"))
	changes := []*StateDomainChange{
		{
			BlockNum:   14,
			TxNum:      begin,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 0,
			Domain:     domain,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("old0"),
			NextExists: true,
			Next:       []byte("old"),
		},
		{
			BlockNum:   14,
			TxNum:      begin + 1,
			Seq:        2,
			FlatDomain: StateFlatDomainKVGeneration,
			Owner:      owner,
			NextExists: true,
			Next:       EncodeStateKVGenerationValue(1),
		},
		{
			BlockNum:   14,
			TxNum:      begin + 1,
			Seq:        3,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     domain,
			Key:        key,
			NextExists: true,
			Next:       []byte("new"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	tests := []struct {
		target uint64
		want   string
		ok     bool
	}{
		{begin + 1, "new", true},
		{begin, "old", true},
		{begin - 1, "old0", true},
	}
	for _, tt := range tests {
		got, ok, err := ReadStateAccountKVAsOfTxNum(db, owner, domain, key, tt.target, end)
		if err != nil || ok != tt.ok || string(got) != tt.want {
			t.Fatalf("account kv as-of tx %d = %q ok:%v err:%v, want %q ok:%v", tt.target, got, ok, err, tt.want, tt.ok)
		}
	}
}

func TestIterateStateAccountKVAsOfPrefixCrossesGenerationReset(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x03}
	domain := kvdomains.SystemReward

	if err := WriteStateKVGeneration(db, owner, 1); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, db, owner, 0, domain, []byte("acct/a"), []byte("a2"))
	mustWriteStateKVLatest(t, db, owner, 0, domain, []byte("acct/b"), []byte("b2"))
	mustWriteStateKVLatest(t, db, owner, 1, domain, []byte("acct/c"), []byte("c4"))
	changes := []*StateDomainChange{
		{
			BlockNum:   2,
			TxNum:      2,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 0,
			Domain:     domain,
			Key:        []byte("acct/a"),
			PrevExists: true,
			Prev:       []byte("a1"),
			NextExists: true,
			Next:       []byte("a2"),
		},
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        1,
			FlatDomain: StateFlatDomainKVGeneration,
			Owner:      owner,
			NextExists: true,
			Next:       EncodeStateKVGenerationValue(1),
		},
		{
			BlockNum:   4,
			TxNum:      4,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     domain,
			Key:        []byte("acct/c"),
			NextExists: true,
			Next:       []byte("c4"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	got := make(map[string]string)
	if err := IterateStateAccountKVAsOfPrefix(db, owner, domain, []byte("acct/"), 2, 4, func(key, value []byte) (bool, error) {
		got[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate account kv as-of prefix: %v", err)
	}
	if len(got) != 2 || got["acct/a"] != "a2" || got["acct/b"] != "b2" {
		t.Fatalf("account kv prefix as-of block 2 = %v, want acct/a=a2 acct/b=b2", got)
	}

	got = make(map[string]string)
	if err := IterateStateAccountKVAsOfPrefix(db, owner, domain, []byte("acct/"), 1, 4, func(key, value []byte) (bool, error) {
		got[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate account kv as-of prefix: %v", err)
	}
	if len(got) != 2 || got["acct/a"] != "a1" || got["acct/b"] != "b2" {
		t.Fatalf("account kv prefix as-of block 1 = %v, want acct/a=a1 acct/b=b2", got)
	}
}
