package snapshots

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"sort"
	"testing"
)

func eventLookupCompressionFixture(random bool) []byte {
	rng := rand.New(rand.NewSource(41))
	keys := make([]eventLogV3LookupKey, 128)
	for i := range keys {
		key := make([]byte, 40)
		if random {
			rng.Read(key[8:])
		} else {
			rng.Read(key[20:40])
		}
		keys[i] = eventLogV3LookupKey{key: key, firstFrame: uint64(i * 3), frameCount: 3, postingCount: 1, checksum: crc32.ChecksumIEEE([]byte{byte(i)})}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i].key, keys[j].key) < 0 })
	for i := range keys {
		keys[i].firstFrame = uint64(i * 3)
		binary.BigEndian.PutUint64(keys[i].key[:8], 0)
	}
	return encodeEventLogV3LookupV2KeyBlock(keys)
}

func eventLookupSidecarFixture() []byte {
	rng := rand.New(rand.NewSource(41))
	keys := make([]eventLogV3LookupKey, 128)
	for i := range keys {
		key := make([]byte, 40)
		rng.Read(key[8:])
		keys[i] = eventLogV3LookupKey{key: key, firstFrame: uint64(i * 4), frameCount: 4, postingCount: 1, checksum: crc32.ChecksumIEEE([]byte{128, 128, 128, 1})}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i].key, keys[j].key) < 0 })
	return encodeEventLogV3LookupV2KeyBlock(keys)
}
func BenchmarkEventLookupSidecarCompression(b *testing.B) {
	enc, dec, err := cbCodec()
	if err != nil {
		b.Fatal(err)
	}
	raw := eventLookupSidecarFixture()
	compressed := enc.EncodeAll(raw, nil)
	b.ReportMetric(float64(len(raw)), "raw-B")
	b.ReportMetric(float64(len(compressed)), "stored-B")
	decoded, err := dec.DecodeAll(compressed, make([]byte, 0, len(raw)))
	if err != nil || !bytes.Equal(raw, decoded) {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportMetric(float64(len(raw)), "raw-B")
	b.ReportMetric(float64(len(compressed)), "stored-B")
	for i := 0; i < b.N; i++ {
		enc.EncodeAll(raw, nil)
	}
}

func BenchmarkEventLookupCompression(b *testing.B) {
	enc, dec, err := cbCodec()
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		random bool
	}{{"address_topic", false}, {"random_topic", true}} {
		raw := eventLookupCompressionFixture(tc.random)
		compressed := enc.EncodeAll(raw, nil)
		decoded, err := dec.DecodeAll(compressed, make([]byte, 0, len(raw)))
		if err != nil || !bytes.Equal(raw, decoded) {
			b.Fatal("roundtrip", err)
		}
		b.Run(tc.name+"/encode", func(b *testing.B) {
			b.ReportMetric(float64(len(raw)), "raw-B")
			b.ReportMetric(float64(len(compressed)), "stored-B")
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				enc.EncodeAll(raw, nil)
			}
		})
		b.Run(tc.name+"/decode", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := dec.DecodeAll(compressed, make([]byte, 0, len(raw))); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestEventLookupKeyBlockAdaptiveCompression(t *testing.T) {
	raw := eventLookupSidecarFixture()
	keys := make([]eventLogV3LookupKey, 128)
	for i := range keys {
		keys[i].checksum = 7
	}
	stored, length, err := compressEventLogLookupKeyBlock(raw, keys)
	if err != nil || length&eventLogV3LookupV2StoredRaw != 0 || len(stored)*10 > len(raw)*9 {
		t.Fatalf("compression not selected: %d/%d %v", len(stored), len(raw), err)
	}
	_, dec, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	got, err := dec.DecodeAll(stored, make([]byte, 0, len(raw)))
	if err != nil || !bytes.Equal(raw, got) {
		t.Fatal("roundtrip", err)
	}
	for i := range keys {
		keys[i].checksum = uint32(i)
	}
	stored, length, err = compressEventLogLookupKeyBlock(raw, keys)
	if err != nil || length&eventLogV3LookupV2StoredRaw == 0 || !bytes.Equal(stored, raw) {
		t.Fatal("unprofitable candidate should skip")
	}
	// A checksum collision is only a compression hint, never a correctness assumption.
	random := make([]byte, len(raw))
	rand.New(rand.NewSource(5)).Read(random)
	for i := range keys {
		keys[i].checksum = 7
	}
	stored, length, err = compressEventLogLookupKeyBlock(random, keys)
	if err != nil || length&eventLogV3LookupV2StoredRaw == 0 || !bytes.Equal(stored, random) {
		t.Fatal("incompressible fallback")
	}
}

func BenchmarkEventLookupAdaptive(b *testing.B) {
	for _, candidate := range []bool{false, true} {
		name := "random_main"
		raw := eventLookupCompressionFixture(true)
		keys := make([]eventLogV3LookupKey, 128)
		for i := range keys {
			keys[i].checksum = uint32(i)
		}
		if candidate {
			name = "singleton_sidecar"
			raw = eventLookupSidecarFixture()
			for i := range keys {
				keys[i].checksum = 7
			}
		}
		stored, _, err := compressEventLogLookupKeyBlock(raw, keys)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(name+"/write", func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(raw)), "raw-B")
			b.ReportMetric(float64(len(stored)), "stored-B")
			for i := 0; i < b.N; i++ {
				if _, _, err := compressEventLogLookupKeyBlock(raw, keys); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(name+"/read", func(b *testing.B) {
			_, dec, err := cbCodec()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if candidate {
					if _, err := dec.DecodeAll(stored, make([]byte, 0, len(raw))); err != nil {
						b.Fatal(err)
					}
				} else {
					_ = bytes.Clone(stored)
				}
			}
		})
	}
}
