package main

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/statecodec"
)

func TestFastDecoderMatchesReference(t *testing.T) {
	for seed := int64(0); seed < 100; seed++ {
		native, err := statecodec.Marshal(fixture(seed))
		if err != nil {
			t.Fatal(err)
		}
		compact, err := encodeCompactSnapshot(native)
		if err != nil {
			t.Fatal(err)
		}
		fast, err := decodeCompactSnapshotFast(compact)
		if err != nil || !bytes.Equal(native, fast) {
			t.Fatalf("seed %d: %v", seed, err)
		}
		for end := 0; end < len(compact); end++ {
			if _, err := decodeCompactSnapshotFast(compact[:end]); err == nil {
				t.Fatalf("seed %d truncation %d accepted", seed, end)
			}
		}
		if _, err := decodeCompactSnapshotFast(append(compact, 0)); err == nil {
			t.Fatal("trailing byte accepted")
		}
	}
}

func BenchmarkCompactSnapshotFast(b *testing.B) {
	native, err := statecodec.Marshal(fixture(27))
	if err != nil {
		b.Fatal(err)
	}
	compact, err := encodeCompactSnapshot(native)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(native)), "native-B")
	b.ReportMetric(float64(len(compact)), "compact-B")
	for i := 0; i < b.N; i++ {
		if _, err := decodeCompactSnapshotFast(compact); err != nil {
			b.Fatal(err)
		}
	}
}
