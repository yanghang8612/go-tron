package rawdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

const sharedHistoryReaderFixtureBlock = uint64(2047)

type sharedHistoryReaderFixture struct {
	pack, raw []byte
	values    map[string][]byte
	keys      []string
}

// Construct exact-format packs independently of writer admission and chunk
// splitting. Chunk boundaries, codecs and repeated references stay fixed.
func newSharedHistoryReaderFixture(chunks [][]byte, codecs []byte) sharedHistoryReaderFixture {
	f := sharedHistoryReaderFixture{values: make(map[string][]byte)}
	for _, chunk := range chunks {
		f.raw = append(f.raw, chunk...)
	}
	f.pack = append(f.pack, stateDomainChangeBlockEnvelopeMagic[:]...)
	f.pack = append(f.pack, stateDomainChangeBlockSharedVersion)
	f.pack = binary.AppendUvarint(f.pack, sharedHistoryReaderFixtureBlock)
	f.pack = binary.AppendUvarint(f.pack, uint64(len(f.raw)))
	digest := sha256.Sum256(f.raw)
	f.pack = append(f.pack, digest[:]...)
	f.pack = binary.AppendUvarint(f.pack, uint64(len(chunks)))
	for i, chunk := range chunks {
		hash := sha256.Sum256(chunk)
		key := string(stateHistoryChunkKey(stateHistoryChunkBucket(sharedHistoryReaderFixtureBlock), hash))
		f.keys = append(f.keys, key)
		f.pack = binary.AppendUvarint(f.pack, uint64(len(chunk)))
		f.pack = append(f.pack, hash[:]...)
		value := binary.AppendUvarint([]byte{1, codecs[i]}, uint64(len(chunk)))
		if codecs[i] == 0 {
			value = append(value, chunk...)
		} else {
			value = append(value, snappy.Encode(nil, chunk)...)
		}
		f.values[key] = value
	}
	return f
}

func sharedHistoryReaderChunks(size, count int, repeated bool, codec string) ([][]byte, []byte) {
	rng := rand.New(rand.NewSource(20260914))
	chunks, codecs := make([][]byte, count), make([]byte, count)
	for i := range chunks {
		if repeated && i > 0 {
			chunks[i], codecs[i] = chunks[0], codecs[0]
			continue
		}
		chunks[i] = make([]byte, size)
		if codec == "snappy" || codec == "mixed" && i%2 == 1 {
			codecs[i] = 1
			// Distinct compressible chunks, with no unique-ref hash collision.
			for j := range chunks[i] {
				chunks[i][j] = byte((j + i) % 251)
			}
		} else {
			_, _ = rng.Read(chunks[i])
		}
	}
	return chunks, codecs
}

type sharedHistoryReaderOracleDB struct {
	values      map[string][]byte
	trace       []string
	failAt      int // One-based operation index, including presence checks.
	replaceAt   int // One-based value read, allowing a repeated key to change.
	replacement []byte
	valueReads  int
	failure     error
}

func (r *sharedHistoryReaderOracleDB) record(op string, key []byte) error {
	r.trace = append(r.trace, op+":"+string(key))
	if r.failAt == len(r.trace) {
		return r.failure
	}
	return nil
}

func (r *sharedHistoryReaderOracleDB) value(key []byte) ([]byte, bool) {
	r.valueReads++
	if r.valueReads == r.replaceAt {
		return r.replacement, true
	}
	v, ok := r.values[string(key)]
	return v, ok
}

func (r *sharedHistoryReaderOracleDB) Get(key []byte) ([]byte, error) {
	if err := r.record("Get", key); err != nil {
		return nil, err
	}
	v, _ := r.value(key)
	return v, nil
}

func (r *sharedHistoryReaderOracleDB) Has(key []byte) (bool, error) {
	if err := r.record("Has", key); err != nil {
		return false, err
	}
	_, ok := r.values[string(key)]
	return ok, nil
}

type sharedHistoryReaderPresenceDB struct{ *sharedHistoryReaderOracleDB }

func (r sharedHistoryReaderPresenceDB) GetWithPresence(key []byte) ([]byte, bool, error) {
	if err := r.record("GetWithPresence", key); err != nil {
		return nil, false, err
	}
	v, ok := r.value(key)
	return v, ok, nil
}

type sharedHistoryReaderOperation func(ethdb.KeyValueReader, []byte, uint64) ([]byte, error)

func TestSharedHistoryReaderFullOperationOracle(t *testing.T) {
	check := func(t *testing.T, f sharedHistoryReaderFixture, block uint64, presence bool, failAt, replaceAt int, replacement []byte) {
		t.Helper()
		packBefore := bytes.Clone(f.pack)
		valuesBefore := make(map[string][]byte, len(f.values))
		for key, value := range f.values {
			valuesBefore[key] = bytes.Clone(value)
		}
		var outputs [2][]byte
		var failures [2]error
		var readers [2]*sharedHistoryReaderOracleDB
		injected := errors.New("injected shared reader operation failure")
		for i, operation := range []sharedHistoryReaderOperation{legacySharedHistoryReaderDecodeStateHistorySharedPack, decodeStateHistorySharedPack} {
			r := &sharedHistoryReaderOracleDB{values: f.values, failAt: failAt, replaceAt: replaceAt, replacement: replacement, failure: injected}
			readers[i] = r
			var db ethdb.KeyValueReader = r
			if presence {
				db = sharedHistoryReaderPresenceDB{r}
			}
			outputs[i], failures[i] = operation(db, f.pack, block)
			if failures[i] != nil && outputs[i] != nil {
				t.Fatal("failed authentication published partial output")
			}
		}
		if !reflect.DeepEqual(outputs[0], outputs[1]) || !reflect.DeepEqual(failures[0], failures[1]) || !reflect.DeepEqual(readers[0].trace, readers[1].trace) {
			t.Fatalf("full reader changed: error=%v/%v reads=%v/%v outputs=%d/%d", failures[0], failures[1], readers[0].trace, readers[1].trace, len(outputs[0]), len(outputs[1]))
		}
		if failures[0] == nil && !bytes.Equal(outputs[0], f.raw) {
			t.Fatal("valid fixture did not reproduce complete input")
		}
		if len(outputs[1]) != 0 {
			outputs[1][0] ^= 1
			if !bytes.Equal(outputs[0], f.raw) {
				t.Fatal("separate operations shared mutable output")
			}
		}
		if !bytes.Equal(f.pack, packBefore) || !reflect.DeepEqual(f.values, valuesBefore) {
			t.Fatal("decode/output mutation modified stored input")
		}
	}
	for _, presence := range []bool{false, true} {
		for _, codec := range []string{"raw", "snappy", "mixed"} {
			for _, repeated := range []bool{false, true} {
				for _, dimensions := range [][2]int{{1, 1}, {historychunk.MinSize - 1, 1}, {historychunk.MinSize, 3}, {historychunk.MaxSize, 48}} {
					name := fmt.Sprintf("presence_%t/%s/repeated_%t/%d_%d", presence, codec, repeated, dimensions[0], dimensions[1])
					t.Run(name, func(t *testing.T) {
						f := newSharedHistoryReaderFixture(sharedHistoryReaderChunks(dimensions[0], dimensions[1], repeated, codec))
						check(t, f, sharedHistoryReaderFixtureBlock, presence, 0, 0, nil)
					})
				}
			}
		}
		for _, repeated := range []bool{false, true} {
			base := newSharedHistoryReaderFixture(sharedHistoryReaderChunks(historychunk.MinSize, 3, repeated, "snappy"))
			for failAt := 1; failAt <= 6; failAt++ {
				t.Run(fmt.Sprintf("presence_%t/repeated_%t/fail_%d", presence, repeated, failAt), func(t *testing.T) {
					check(t, base, sharedHistoryReaderFixtureBlock, presence, failAt, 0, nil)
				})
			}
			for _, position := range []int{1, 2, 3} {
				// Every reference is still read and authenticated, including a
				// repeated key whose nth response changes after earlier success.
				for _, kind := range []string{"empty", "bad_codec", "bad_length", "bad_snappy_size", "corrupt_snappy", "wrong_hash"} {
					t.Run(fmt.Sprintf("presence_%t/repeated_%t/replace_%d/%s", presence, repeated, position, kind), func(t *testing.T) {
						value := bytes.Clone(base.values[base.keys[position-1]])
						switch kind {
						case "empty":
							value = nil
						case "bad_codec":
							value[1] = 2
						case "bad_length":
							value[2] ^= 1
						case "bad_snappy_size":
							_, n := binary.Uvarint(value[2:])
							value[2+n] ^= 1
						case "corrupt_snappy":
							value = value[:len(value)-1]
						case "wrong_hash":
							value = encodeStateHistorySharedChunk(bytes.Repeat([]byte{0x7d}, historychunk.MinSize))
						}
						check(t, base, sharedHistoryReaderFixtureBlock, presence, 0, position, value)
					})
				}
			}
			for _, position := range []int{0, 1, 2} {
				t.Run(fmt.Sprintf("presence_%t/repeated_%t/missing_%d", presence, repeated, position), func(t *testing.T) {
					f := newSharedHistoryReaderFixture(sharedHistoryReaderChunks(historychunk.MinSize, 3, repeated, "snappy"))
					delete(f.values, f.keys[position])
					check(t, f, sharedHistoryReaderFixtureBlock, presence, 0, 0, nil)
				})
			}
		}
	}
	base := newSharedHistoryReaderFixture(sharedHistoryReaderChunks(historychunk.MinSize, 3, false, "mixed"))
	// All byte truncations and one-bit mutations include the full header and
	// every reference. The frozen reader determines the exact error precedence
	// and whether a failure is preflight, per-chunk, or whole-pack authentication.
	for i := 0; i < len(base.pack); i++ {
		for _, truncate := range []bool{false, true} {
			t.Run(fmt.Sprintf("header_%d/truncate_%t", i, truncate), func(t *testing.T) {
				f := base
				f.pack = bytes.Clone(base.pack)
				if truncate {
					f.pack = f.pack[:i]
				} else {
					f.pack[i] ^= 1
				}
				check(t, f, sharedHistoryReaderFixtureBlock, true, 0, 0, nil)
			})
		}
	}
	check(t, base, sharedHistoryReaderFixtureBlock+1, true, 0, 0, nil)
	for _, operation := range []sharedHistoryReaderOperation{legacySharedHistoryReaderDecodeStateHistorySharedPack, decodeStateHistorySharedPack} {
		if got, err := operation(nil, nil, 0); got != nil || err == nil || err.Error() != "rawdb: shared history requires a coherent database reader" {
			t.Fatalf("nil reader error precedence changed: %v", err)
		}
	}
}

func TestSharedHistoryReaderDestinationOracleAndOwnership(t *testing.T) {
	check := func(data []byte, want int, hash [32]byte) {
		t.Helper()
		before := bytes.Clone(data)
		expected, expectedErr := legacySharedHistoryReaderDecodeStateHistorySharedChunk(data, want, hash)
		guard := bytes.Repeat([]byte{0xa5}, max(0, want)+32)
		for _, into := range []bool{false, true} {
			var dst []byte
			if into && want > 0 {
				dst = guard[16 : 16+want : 16+want]
			}
			got, err := decodeStateHistorySharedChunkInto(dst, data, want, hash)
			if !reflect.DeepEqual(got, expected) || !reflect.DeepEqual(err, expectedErr) || !bytes.Equal(before, data) {
				t.Fatalf("chunk oracle changed: size=%d dst=%t err=%v/%v", want, into, err, expectedErr)
			}
			if !bytes.Equal(guard[:16], bytes.Repeat([]byte{0xa5}, 16)) || !bytes.Equal(guard[len(guard)-16:], bytes.Repeat([]byte{0xa5}, 16)) {
				t.Fatal("decoder wrote outside destination")
			}
			if err == nil {
				if dst != nil && &got[0] != &dst[0] {
					t.Fatal("valid destination was not used")
				}
				_, n := binary.Uvarint(data[2:])
				aliases := &got[0] == &data[2+n]
				if aliases != (dst == nil && data[1] == 0) {
					t.Fatal("standalone raw/Snappy ownership changed")
				}
			}
		}
	}
	rng := rand.New(rand.NewSource(20260915))
	for _, size := range []int{1, historychunk.MinSize - 1, historychunk.MinSize, historychunk.MaxSize} {
		for _, codec := range []string{"raw", "snappy"} {
			f := newSharedHistoryReaderFixture(sharedHistoryReaderChunks(size, 1, false, codec))
			data, hash := f.values[f.keys[0]], sha256.Sum256(f.raw)
			for _, want := range []int{-1, 0, size, size - 1, size + 1, historychunk.MaxSize + 1} {
				check(data, want, hash)
			}
			wrongHash := hash
			wrongHash[0] ^= 1
			check(data, size, wrongHash)
			for i := range data {
				if i > 20 && i != len(data)-1 {
					continue
				}
				corrupt := bytes.Clone(data)
				corrupt[i] ^= 0x80
				check(corrupt, size, hash)
			}
			check(data[:len(data)-1], size, hash)
			check(append(bytes.Clone(data), 0), size, hash)
		}
	}
	for i := 0; i < 1000; i++ {
		data := make([]byte, rng.Intn(256))
		_, _ = rng.Read(data)
		check(data, rng.Intn(257)-1, sha256.Sum256(data))
	}
}

func TestSharedHistoryReaderCompressedMultiChunkPublicFailureAtomicity(t *testing.T) {
	f, owner := newHistoryReadViewFixture(t)
	row := &StateDomainChange{BlockNum: 10, Seq: 1, TxNum: 100, FlatDomain: StateFlatDomainKVLatest,
		Owner: owner, Key: []byte("shared-key"), PrevExists: true,
		Prev: bytes.Repeat([]byte("compressible-history-prev"), 12000)}
	// Reuse the established domain and physical/tx metadata of the public
	// fixture, while making its RLP require several compressed chunks.
	original, exists, err := ReadStateDomainChange(f, 10, 1)
	if err != nil || !exists {
		t.Fatalf("fixture read: %v", err)
	}
	row.Domain = original.Domain
	raw := encodeBorrowedStateDomainChangeTestBlock(t, []*StateDomainChange{row})
	var chunks [][]byte
	var codecs []byte
	for start := 0; start < len(raw); start += historychunk.MaxSize {
		chunks = append(chunks, raw[start:min(start+historychunk.MaxSize, len(raw))])
		codecs = append(codecs, 1)
	}
	pack := newSharedHistoryReaderFixture(chunks, codecs)
	// Replace only the physical-height varint (the payload has no height).
	prefix := len(stateDomainChangeBlockEnvelopeMagic) + 1
	_, n := binary.Uvarint(pack.pack[prefix:])
	encoded := append(bytes.Clone(pack.pack[:prefix]), byte(10))
	encoded = append(encoded, pack.pack[prefix+n:]...)
	for i, chunk := range chunks {
		hash := sha256.Sum256(chunk)
		if err := f.KeyValueStore.Put(stateHistoryChunkKey(stateHistoryChunkBucket(10), hash), pack.values[pack.keys[i]]); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.KeyValueStore.Put(stateChangeSetKey(10, 0), encoded); err != nil {
		t.Fatal(err)
	}
	for _, borrowed := range []bool{false, true} {
		visits := 0
		expected := *row
		if borrowed {
			// Tx-range iteration attaches the canonical range hash; direct
			// block iteration intentionally leaves BlockHash zero.
			expected.BlockHash[0] = 10
		}
		visit := func(got *StateDomainChange) (bool, error) {
			visits++
			if !reflect.DeepEqual(got, &expected) {
				t.Fatal("public compressed multi-chunk row changed")
			}
			return true, nil
		}
		if borrowed {
			err = IterateStateDomainChangesByBlockTxRangeBorrowed(f, 10, 10, 100, 100, visit)
		} else {
			err = IterateStateDomainChanges(f, 10, visit)
		}
		if err != nil || visits != 1 {
			t.Fatalf("public compressed read: visits=%d err=%v", visits, err)
		}
	}
	// A final chunk or whole-pack failure must happen before any public callback.
	for _, wholePack := range []bool{false, true} {
		corrupt := bytes.Clone(encoded)
		if wholePack {
			_, used := binary.Uvarint(corrupt[prefix+1:])
			corrupt[prefix+1+used] ^= 1
		} else {
			corrupt[len(corrupt)-1] ^= 1
		}
		if err := f.KeyValueStore.Put(stateChangeSetKey(10, 0), corrupt); err != nil {
			t.Fatal(err)
		}
		for _, borrowed := range []bool{false, true} {
			visits := 0
			visit := func(*StateDomainChange) (bool, error) { visits++; return true, nil }
			if borrowed {
				err = IterateStateDomainChangesByBlockTxRangeBorrowed(f, 10, 10, 100, 100, visit)
			} else {
				err = IterateStateDomainChanges(f, 10, visit)
			}
			if err == nil || visits != 0 || f.opened != f.closed {
				t.Fatalf("failed public decode escaped: visits=%d views=%d/%d err=%v", visits, f.opened, f.closed, err)
			}
		}
	}
}
