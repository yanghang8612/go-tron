package snapshots

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestMarshalManifestJSONMatchesEncodingJSON(t *testing.T) {
	cases := []struct {
		name string
		m    *Manifest
	}{
		{name: "nil", m: nil},
		{name: "zero_value_nil_segments", m: &Manifest{}},
		{
			name: "empty_non_nil_segments_empty_progress",
			m: &Manifest{
				CommitmentScheme: "muhash3072",
				ElementVersion:   1,
				Version:          2,
				Progress:         &Progress{},
				Segments:         []SegmentRef{},
				Retired:          []SegmentRef{},
			},
		},
		{
			name: "full_fields_and_escaping",
			m: &Manifest{
				LatestStateTxNum:         987654321,
				SourceRows:               [3]uint64{10, 20, 30},
				CommitmentScheme:         "muhash3072/<v1>&\"ok\"",
				ElementVersion:           2,
				Version:                  4,
				Generation:               77,
				HistoryStagingResetEpoch: 3,
				PublishedUnix:            1732700000,
				VisibleTxStart:           100,
				VisibleTxEnd:             200,
				Chain: &ChainIdentity{
					ChainID:        1,
					NetworkID:      -2,
					GenesisHash:    "0xabc\t\n\r\b\f\x00\u2028\u2029",
					ForkConfigHash: "0x<\xff>",
				},
				Progress: &Progress{
					LatestBuildTxNum:              100,
					HistoryBuildTxNum:             99,
					AccessorBuildTxNum:            98,
					CommitmentFlushTxNum:          97,
					HotPruneTxNum:                 96,
					HotPruneBlockNum:              95,
					StateChangeIndexPruneBlockNum: 94,
				},
				Segments: []SegmentRef{
					{
						Rows:             1234,
						Dataset:          SegmentDatasetStateDomainChange,
						Domain:           kvdomains.ContractStorage,
						Kind:             SegmentHistory,
						FromTxNum:        0,
						ToTxNum:          999,
						AggregationSteps: 5,
						Path:             "state-domain-change/000000-000999.seg",
						Size:             4096,
						Checksum:         "deadbeef",
						LookupRoot:       "cafebabe",
					},
					{
						Kind:      SegmentEventLogIndex,
						FromTxNum: 1000,
						ToTxNum:   1999,
						Path:      "event-log/<bad\xfe>.idx",
						Size:      512,
					},
				},
				Retired: []SegmentRef{
					{
						Kind:      SegmentHistory,
						FromTxNum: 0,
						ToTxNum:   499,
						Path:      "retired/0.seg",
						Size:      1024,
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, err := json.Marshal(tc.m)
			if err != nil {
				t.Fatalf("json.Marshal: %v", err)
			}
			got, err := marshalManifestJSON(tc.m)
			if err != nil {
				t.Fatalf("marshalManifestJSON: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("mismatch:\ngot:  %s\nwant: %s", got, want)
			}
		})
	}
}

func makeBenchmarkManifest(segments int) *Manifest {
	refs := make([]SegmentRef, segments)
	for i := range refs {
		refs[i] = SegmentRef{
			Rows:             50000,
			Dataset:          SegmentDatasetStateDomainChange,
			Domain:           kvdomains.AccountAssetV2,
			Kind:             SegmentHistory,
			FromTxNum:        uint64(i) * 10000,
			ToTxNum:          uint64(i+1)*10000 - 1,
			AggregationSteps: 4,
			Path:             fmt.Sprintf("state-domain-change/seg-%06d.seg", i),
			Size:             1 << 20,
			Checksum:         "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			LookupRoot:       "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
		}
	}
	return &Manifest{
		LatestStateTxNum: 50_000_000,
		SourceRows:       [3]uint64{1000, 2000, 3000},
		CommitmentScheme: "muhash3072",
		ElementVersion:   1,
		Version:          2,
		PublishedUnix:    1732700000,
		VisibleTxStart:   0,
		VisibleTxEnd:     uint64(segments) * 10000,
		Chain: &ChainIdentity{
			ChainID:     728126428,
			NetworkID:   0,
			GenesisHash: "0000000000000000109d7bb0b2e6f1e6b6b3a7c8b8d9c0e1f2a3b4c5d6e7f809",
		},
		Progress: &Progress{
			LatestBuildTxNum:     50_000_000,
			HistoryBuildTxNum:    49_990_000,
			AccessorBuildTxNum:   49_990_000,
			CommitmentFlushTxNum: 50_000_000,
		},
		Segments: refs,
	}
}

func TestMarshalManifestJSONRealistic10KMatchesEncodingJSON(t *testing.T) {
	m := makeBenchmarkManifest(10000)
	want, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := marshalManifestJSON(m)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("10k manifest mismatch (len got=%d want=%d)", len(got), len(want))
	}
	// Capacity should stay tightly sized so manifestCacheCharge isn't inflated.
	if cap(got)-len(got) > 64<<10 {
		t.Fatalf("overallocated capacity: len=%d cap=%d", len(got), cap(got))
	}
}

func TestMarshalManifestJSONFitsTightCacheBudget(t *testing.T) {
	m := makeBenchmarkManifest(10000)
	// Include omitted-optional segments (.idx / .kv) matching production manifests.
	for i := 0; i < len(m.Segments); i += 3 {
		m.Segments[i].Rows = 0
		m.Segments[i].AggregationSteps = 0
		m.Segments[i].LookupRoot = ""
	}
	want, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := marshalManifestJSON(m)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("manifest mismatch (len got=%d want=%d)", len(got), len(want))
	}
	if cap(got) != len(got) {
		t.Fatalf("expected exact capacity for ASCII manifest: len=%d cap=%d", len(got), cap(got))
	}
	wantCharge := manifestCacheCharge(want, m)
	gotCharge := manifestCacheCharge(got, m)
	if gotCharge > wantCharge+(32<<10) {
		t.Fatalf("manifestCacheCharge inflated: got=%d want<=%d", gotCharge, wantCharge+(32<<10))
	}

	var cache manifestDecodeCache
	budget := wantCharge + (1 << 20)
	cache.retainPublished(got, m, budget)
	cache.mu.Lock()
	residentManifest := cache.manifest
	residentCharge := cache.charge
	cache.mu.Unlock()
	if residentManifest == nil || residentCharge > budget {
		t.Fatalf("retainPublished failed tight budget: resident=%v charge=%d budget=%d", residentManifest != nil, residentCharge, budget)
	}
}

func BenchmarkMarshalManifestJSON(b *testing.B) {
	for _, n := range []int{5000, 10000} {
		m := makeBenchmarkManifest(n)
		b.Run(fmt.Sprintf("json.Marshal_%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out, err := json.Marshal(m)
				if err != nil || len(out) == 0 {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("marshalManifestJSON_%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out, err := marshalManifestJSON(m)
				if err != nil || len(out) == 0 {
					b.Fatal(err)
				}
			}
		})
	}
}
