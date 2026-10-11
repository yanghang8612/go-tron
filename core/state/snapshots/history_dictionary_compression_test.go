package snapshots

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sort"
	"testing"
)

func historyDictionaryCompressionFixture(random bool) []byte {
	rng := rand.New(rand.NewSource(71))
	keys := make([]stateDomainChangeBinaryAccessorV6Key, 128)
	for i := range keys {
		key := make([]byte, 54)
		key[0] = 1
		rng.Read(key[1:22])
		if random {
			rng.Read(key[22:])
		} else {
			binary.BigEndian.PutUint64(key[46:], uint64(i%13))
		}
		keys[i] = stateDomainChangeBinaryAccessorV6Key{key: key, keyID: uint32(i), postingOff: uint64(i * 13), postingCount: uint32(1 + i%7)}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i].key, keys[j].key) < 0 })
	raw, err := encodeStateDomainChangeBinaryAccessorV6KeyBlock(keys)
	if err != nil {
		panic(err)
	}
	return raw
}
func BenchmarkHistoryDictionaryCompression(b *testing.B) {
	enc, dec, err := cbCodec()
	if err != nil {
		b.Fatal(err)
	}
	for _, random := range []bool{false, true} {
		name := "account_slot"
		if random {
			name = "random_storage"
		}
		raw := historyDictionaryCompressionFixture(random)
		compressed := enc.EncodeAll(raw, nil)
		decoded, err := dec.DecodeAll(compressed, make([]byte, 0, len(raw)))
		if err != nil || !bytes.Equal(raw, decoded) {
			b.Fatal(err)
		}
		b.Run(name+"/write", func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(raw)), "raw-B")
			b.ReportMetric(float64(len(compressed)), "stored-B")
			for i := 0; i < b.N; i++ {
				enc.EncodeAll(raw, nil)
			}
		})
		b.Run(name+"/read", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := dec.DecodeAll(compressed, make([]byte, 0, len(raw))); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestHistoryDictionaryCompressionPreservesAggregateRawBound(t *testing.T) {
	var largest int
	raw := historyDictionaryCompressionFixture(false)
	inputs := [][]byte{raw, raw, append(append([]byte(nil), raw...), raw...), raw}
	var physical uint64
	_, dec, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	for i, input := range inputs {
		stored, flags, err := compressHistoryDictionaryKeyBlock(input, &largest)
		if err != nil {
			t.Fatal(err)
		}
		physical += uint64(len(stored))
		if i == 0 || i == 2 {
			if flags&stateDomainChangeBinaryAccessorV6StoredRaw == 0 {
				t.Fatal("largest must remain raw")
			}
		} else {
			if flags&stateDomainChangeBinaryAccessorV6StoredRaw != 0 {
				t.Fatal("repeated compressible block must compress")
			}
			decoded, err := dec.DecodeAll(stored, make([]byte, 0, len(input)))
			if err != nil || !bytes.Equal(decoded, input) {
				t.Fatal("roundtrip", err)
			}
		}
		if physical < uint64(largest) {
			t.Fatal("aggregate raw bound violated")
		}
	}
	random := historyDictionaryCompressionFixture(true)
	largest = len(random) + 1
	stored, flags, err := compressHistoryDictionaryKeyBlock(random, &largest)
	if err != nil || flags&stateDomainChangeBinaryAccessorV6StoredRaw == 0 || !bytes.Equal(stored, random) {
		t.Fatal("incompressible fallback")
	}
}

func TestHistoryDictionaryCompressedV7BuilderAndPointReads(t *testing.T) {
	data := buildStateDomainChangeV7AccessorBytes(t, 2048)
	r := bytes.NewReader(data)
	size := uint64(len(data))
	h, err := decodeStateDomainChangeBinaryAccessorV7Header(r, size)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(r, h)
	if err != nil {
		t.Fatal(err)
	}
	compressed := 0
	var rawBytes uint64
	for i, block := range blocks {
		rawBytes += uint64(block.rawLen)
		if block.dataLen&stateDomainChangeBinaryAccessorV6StoredRaw == 0 {
			compressed++
		}
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, size, h, block, uint32(i*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			_, got, ok, err := stateDomainChangeBinaryAccessorV7Lookup(r, size, record.key)
			if err != nil || !ok || !bytes.Equal(got.key, record.key) {
				t.Fatal("point read", err)
			}
		}
	}
	if compressed == 0 || h.keyDataLen >= rawBytes {
		t.Fatalf("no savings: compressed=%d physical=%d raw=%d", compressed, h.keyDataLen, rawBytes)
	}
	if err := checkStateDomainChangeBinaryAccessorV7(r, size); err != nil {
		t.Fatal(err)
	}
	t.Logf("dictionary raw=%d stored=%d blocks=%d compressed=%d", rawBytes, h.keyDataLen, len(blocks), compressed)
}

func BenchmarkHistoryPostingCompressionCandidate(b *testing.B) {
	enc, dec, err := cbCodec()
	if err != nil {
		b.Fatal(err)
	}
	for _, regular := range []bool{false, true} {
		name := "sparse_random"
		if regular {
			name = "regular_hot"
		}
		rng := rand.New(rand.NewSource(91))
		postings := make([]stateDomainChangeBinaryAccessorV6Posting, 128)
		var tx, off uint64
		var record uint32
		for i := range postings {
			dt, do, dr := uint64(1), uint64(97), uint32(1)
			if !regular {
				dt = uint64(1 + rng.Intn(100000))
				do = uint64(1 + rng.Intn(1000000))
				dr = uint32(1 + rng.Intn(10000))
			}
			tx += dt
			off += do
			record += dr
			postings[i] = stateDomainChangeBinaryAccessorV6Posting{txNum: tx, offset: off, recordIndex: record}
		}
		raw, err := encodeStateDomainChangeBinaryAccessorV7PostingList(0, postings)
		if err != nil {
			b.Fatal(err)
		}
		compressed := enc.EncodeAll(raw, nil)
		decoded, err := dec.DecodeAll(compressed, make([]byte, 0, len(raw)))
		if err != nil || !bytes.Equal(decoded, raw) {
			b.Fatal(err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(raw)), "raw-B")
			b.ReportMetric(float64(len(compressed)), "stored-B")
			for i := 0; i < b.N; i++ {
				enc.EncodeAll(raw, nil)
			}
		})
	}
}
