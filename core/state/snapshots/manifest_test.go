package snapshots

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestPublishLoadManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	manifest := NewManifestForChain(10, 30, []SegmentRef{
		{
			Domain:    kvdomains.SystemReward,
			Kind:      SegmentHistory,
			FromTxNum: 10,
			ToTxNum:   20,
			Path:      "history/reward-10-20.seg",
			Size:      123,
			Checksum:  "sha256:abc",
		},
		{
			Domain:    kvdomains.SystemReward,
			Kind:      SegmentHistory,
			FromTxNum: 21,
			ToTxNum:   30,
			Path:      "history/reward-21-30.seg",
			Size:      456,
		},
		{
			Domain:    kvdomains.ContractStorage,
			Kind:      SegmentLatest,
			FromTxNum: 10,
			ToTxNum:   30,
			Path:      "latest/storage-10-30.json",
		},
	}, ChainIdentity{
		ChainID:        1,
		NetworkID:      11111,
		GenesisHash:    "0x0000000000000000000000000000000000000000000000000000000000000001",
		ForkConfigHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ManifestFile)); err != nil {
		t.Fatalf("manifest not published: %v", err)
	}
	loaded, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if loaded.Version != ManifestVersion || loaded.VisibleTxStart != 10 || loaded.VisibleTxEnd != 30 {
		t.Fatalf("loaded manifest = %+v", loaded)
	}
	if len(loaded.Segments) != 3 {
		t.Fatalf("loaded segments = %d", len(loaded.Segments))
	}
	if loaded.Chain == nil {
		t.Fatal("loaded manifest missing chain identity")
	}
	if loaded.Chain.GenesisHash != "0000000000000000000000000000000000000000000000000000000000000001" {
		t.Fatalf("loaded genesis hash not normalised: %q", loaded.Chain.GenesisHash)
	}
	if err := loaded.ValidateChainIdentity(ChainIdentity{
		ChainID:        1,
		NetworkID:      11111,
		GenesisHash:    "0000000000000000000000000000000000000000000000000000000000000001",
		ForkConfigHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}); err != nil {
		t.Fatalf("chain identity validation failed: %v", err)
	}
	if err := loaded.ValidateChainIdentity(ChainIdentity{
		ChainID:        1,
		NetworkID:      201910292,
		GenesisHash:    "0000000000000000000000000000000000000000000000000000000000000001",
		ForkConfigHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}); err == nil {
		t.Fatal("wrong network id accepted by chain identity validation")
	}
}

func TestManifestValidateChainIdentityRequiresIdentity(t *testing.T) {
	manifest := NewManifest(1, 1, nil)
	if err := manifest.ValidateChainIdentity(ChainIdentity{
		ChainID:     1,
		NetworkID:   11111,
		GenesisHash: "0000000000000000000000000000000000000000000000000000000000000001",
	}); err == nil {
		t.Fatal("manifest without chain identity accepted")
	}
}

func TestManifestRejectsInvalidChainIdentity(t *testing.T) {
	tests := []struct {
		name     string
		identity ChainIdentity
	}{
		{
			name: "missing genesis",
			identity: ChainIdentity{
				ChainID:   1,
				NetworkID: 11111,
			},
		},
		{
			name: "bad genesis hash",
			identity: ChainIdentity{
				ChainID:     1,
				NetworkID:   11111,
				GenesisHash: "not-hex",
			},
		},
		{
			name: "bad fork hash",
			identity: ChainIdentity{
				ChainID:        1,
				NetworkID:      11111,
				GenesisHash:    "0000000000000000000000000000000000000000000000000000000000000001",
				ForkConfigHash: "not-sha256",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manifest := NewManifestForChain(1, 1, nil, tt.identity)
			if err := manifest.Validate(); err == nil {
				t.Fatal("invalid chain identity accepted")
			}
		})
	}
}

func TestManifestAcceptsFlatLatestDatasets(t *testing.T) {
	manifest := NewManifest(100, 200, []SegmentRef{
		{Dataset: SegmentDatasetAccountLatest, Kind: SegmentLatest, FromTxNum: 100, ToTxNum: 200, Path: "latest/accounts.seg"},
		{Dataset: SegmentDatasetAccountLatest, Kind: SegmentAccessor, FromTxNum: 100, ToTxNum: 200, Path: "latest/accounts.lidx"},
		{Dataset: SegmentDatasetAccountLatest, Kind: SegmentBTree, FromTxNum: 100, ToTxNum: 200, Path: "latest/accounts.bt"},
		{Dataset: SegmentDatasetKVLatest, Domain: kvdomains.ContractStorage, Kind: SegmentLatest, FromTxNum: 100, ToTxNum: 200, Path: "latest/contract-storage.seg"},
		{Dataset: SegmentDatasetKVLatest, Domain: kvdomains.ContractStorage, Kind: SegmentAccessor, FromTxNum: 100, ToTxNum: 200, Path: "latest/contract-storage.lidx"},
		{Dataset: SegmentDatasetKVLatest, Domain: kvdomains.ContractStorage, Kind: SegmentBTree, FromTxNum: 100, ToTxNum: 200, Path: "latest/contract-storage.bt"},
		{Dataset: SegmentDatasetKVGeneration, Kind: SegmentLatest, FromTxNum: 100, ToTxNum: 200, Path: "latest/kv-generation.seg"},
		{Dataset: SegmentDatasetKVGeneration, Kind: SegmentAccessor, FromTxNum: 100, ToTxNum: 200, Path: "latest/kv-generation.lidx"},
		{Dataset: SegmentDatasetKVGeneration, Kind: SegmentBTree, FromTxNum: 100, ToTxNum: 200, Path: "latest/kv-generation.bt"},
		{Dataset: SegmentDatasetCommitmentRoot, Kind: SegmentLatest, FromTxNum: 100, ToTxNum: 200, Path: "commitment/root.seg"},
		{Dataset: SegmentDatasetCommitmentRoot, Kind: SegmentAccessor, FromTxNum: 100, ToTxNum: 200, Path: "commitment/root.lidx"},
		{Dataset: SegmentDatasetCommitmentRoot, Kind: SegmentBTree, FromTxNum: 100, ToTxNum: 200, Path: "commitment/root.bt"},
		{Dataset: SegmentDatasetCommitmentCheckpoint, Kind: SegmentLatest, FromTxNum: 100, ToTxNum: 200, Path: "commitment/checkpoints.seg"},
		{Dataset: SegmentDatasetCommitmentCheckpoint, Kind: SegmentAccessor, FromTxNum: 100, ToTxNum: 200, Path: "commitment/checkpoints.lidx"},
		{Dataset: SegmentDatasetCommitmentCheckpoint, Kind: SegmentBTree, FromTxNum: 100, ToTxNum: 200, Path: "commitment/checkpoints.bt"},
	})
	if err := manifest.Validate(); err != nil {
		t.Fatalf("flat latest manifest rejected: %v", err)
	}
}

func TestManifestValidatesBinaryLatestCompanions(t *testing.T) {
	tests := []struct {
		name     string
		segments []SegmentRef
	}{
		{
			name: "btree only",
			segments: []SegmentRef{
				{Dataset: SegmentDatasetAccountLatest, Kind: SegmentLatest, FromTxNum: 1, ToTxNum: 10, Path: "latest/accounts.seg"},
				{Dataset: SegmentDatasetAccountLatest, Kind: SegmentBTree, FromTxNum: 1, ToTxNum: 10, Path: "latest/accounts.bt"},
			},
		},
		{
			name: "missing btree",
			segments: []SegmentRef{
				{Dataset: SegmentDatasetAccountLatest, Kind: SegmentLatest, FromTxNum: 1, ToTxNum: 10, Path: "latest/accounts.seg"},
				{Dataset: SegmentDatasetAccountLatest, Kind: SegmentAccessor, FromTxNum: 1, ToTxNum: 10, Path: "latest/accounts.lidx"},
			},
		},
		{
			name: "orphan accessor",
			segments: []SegmentRef{
				{Dataset: SegmentDatasetAccountLatest, Kind: SegmentAccessor, FromTxNum: 1, ToTxNum: 10, Path: "latest/accounts.lidx"},
			},
		},
		{
			name: "orphan btree",
			segments: []SegmentRef{
				{Dataset: SegmentDatasetAccountLatest, Kind: SegmentBTree, FromTxNum: 1, ToTxNum: 10, Path: "latest/accounts.bt"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewManifest(1, 10, tt.segments).Validate()
			if tt.name == "btree only" {
				if err != nil {
					t.Fatalf("btree-only latest companion set rejected: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid binary latest companion set accepted")
			}
		})
	}
}

func TestManifestRejectsKVDomainOnNonKVDataset(t *testing.T) {
	manifest := NewManifest(1, 10, []SegmentRef{
		{Dataset: SegmentDatasetAccountLatest, Domain: kvdomains.SystemReward, Kind: SegmentLatest, FromTxNum: 1, ToTxNum: 10, Path: "accounts.seg"},
	})
	if err := manifest.Validate(); err == nil {
		t.Fatal("account latest segment with kv domain accepted")
	}
}

func TestManifestAcceptsStateDomainChangeIndexedDatasets(t *testing.T) {
	manifest := NewManifest(100, 200, []SegmentRef{
		{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 100, ToTxNum: 150, Path: "history/state-domain-change-100-150.seg"},
		{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentInverted, FromTxNum: 100, ToTxNum: 150, Path: "history/state-domain-change-100-150.idx"},
		{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, FromTxNum: 100, ToTxNum: 150, Path: "history/state-domain-change-100-150.kv"},
		{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 151, ToTxNum: 200, Path: "history/state-domain-change-151-200.seg"},
		{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentInverted, FromTxNum: 151, ToTxNum: 200, Path: "history/state-domain-change-151-200.idx"},
		{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, FromTxNum: 151, ToTxNum: 200, Path: "history/state-domain-change-151-200.kv"},
	})
	if err := manifest.Validate(); err != nil {
		t.Fatalf("state domain change manifest rejected: %v", err)
	}
	if err := manifest.ValidateProduction(); err != nil {
		t.Fatalf("production state domain change manifest rejected: %v", err)
	}
}

func TestManifestValidatesEventLogIndexCompanions(t *testing.T) {
	valid := NewManifest(0, 0, []SegmentRef{
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 10, ToTxNum: 12, Path: "log/event-log-10-12.seg"},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 13, ToTxNum: 15, Path: "log/event-log-13-15.seg"},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: 10, ToTxNum: 15, Path: "log/event-log-index-10-15.idx"},
	})
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid event-log index companions rejected: %v", err)
	}

	tests := []struct {
		name     string
		segments []SegmentRef
	}{
		{
			name: "orphan index",
			segments: []SegmentRef{
				{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: 10, ToTxNum: 12, Path: "log/event-log-index-10-12.idx"},
			},
		},
		{
			name: "gap in coverage",
			segments: []SegmentRef{
				{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 10, ToTxNum: 11, Path: "log/event-log-10-11.seg"},
				{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 13, ToTxNum: 15, Path: "log/event-log-13-15.seg"},
				{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: 10, ToTxNum: 15, Path: "log/event-log-index-10-15.idx"},
			},
		},
		{
			name: "prefix missing",
			segments: []SegmentRef{
				{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 11, ToTxNum: 15, Path: "log/event-log-11-15.seg"},
				{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: 10, ToTxNum: 15, Path: "log/event-log-index-10-15.idx"},
			},
		},
		{
			name: "suffix missing",
			segments: []SegmentRef{
				{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 10, ToTxNum: 14, Path: "log/event-log-10-14.seg"},
				{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: 10, ToTxNum: 15, Path: "log/event-log-index-10-15.idx"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := NewManifest(0, 0, tt.segments).Validate(); err == nil {
				t.Fatal("invalid event-log index companions accepted")
			}
		})
	}
}

func TestProductionManifestRejectsJSONHistory(t *testing.T) {
	manifest := NewManifest(100, 150, []SegmentRef{
		{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 100, ToTxNum: 150, Path: "history/state-domain-change-100-150.json"},
	})
	if err := manifest.Validate(); err != nil {
		t.Fatalf("legacy state-domain-change JSON manifest rejected by base validation: %v", err)
	}
	if err := manifest.ValidateProduction(); err == nil {
		t.Fatal("production state-domain-change JSON history accepted")
	}
}

func TestManifestRejectsIncompleteBinaryStateDomainChangeTrio(t *testing.T) {
	tests := []struct {
		name     string
		segments []SegmentRef
	}{
		{
			name: "missing index",
			segments: []SegmentRef{
				{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 100, ToTxNum: 150, Path: "history/state-domain-change-100-150.seg"},
				{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, FromTxNum: 100, ToTxNum: 150, Path: "history/state-domain-change-100-150.kv"},
			},
		},
		{
			name: "missing accessor",
			segments: []SegmentRef{
				{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 100, ToTxNum: 150, Path: "history/state-domain-change-100-150.seg"},
				{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentInverted, FromTxNum: 100, ToTxNum: 150, Path: "history/state-domain-change-100-150.idx"},
			},
		},
		{
			name: "orphan accessor",
			segments: []SegmentRef{
				{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, FromTxNum: 100, ToTxNum: 150, Path: "history/state-domain-change-100-150.kv"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := NewManifest(100, 150, tt.segments).Validate(); err == nil {
				t.Fatal("incomplete binary state-domain-change trio accepted")
			}
		})
	}
}

func TestManifestRejectsStateDomainChangeDatasetOnLatest(t *testing.T) {
	manifest := NewManifest(1, 10, []SegmentRef{
		{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentLatest, FromTxNum: 1, ToTxNum: 10, Path: "latest/state-domain-change.seg"},
	})
	if err := manifest.Validate(); err == nil {
		t.Fatal("state domain change latest dataset accepted")
	}
}

func TestManifestRejectsKVDomainOnStateDomainChangeDataset(t *testing.T) {
	manifest := NewManifest(1, 10, []SegmentRef{
		{Dataset: SegmentDatasetStateDomainChange, Domain: kvdomains.SystemReward, Kind: SegmentHistory, FromTxNum: 1, ToTxNum: 10, Path: "history/state-domain-change.seg"},
	})
	if err := manifest.Validate(); err == nil {
		t.Fatal("state domain change segment with kv domain accepted")
	}
}

func TestManifestRejectsLatestDatasetOnHistory(t *testing.T) {
	manifest := NewManifest(1, 10, []SegmentRef{
		{Dataset: SegmentDatasetKVLatest, Domain: kvdomains.SystemReward, Kind: SegmentHistory, FromTxNum: 1, ToTxNum: 10, Path: "history/kv-latest.seg"},
	})
	if err := manifest.Validate(); err == nil {
		t.Fatal("latest dataset on history segment accepted")
	}
}

func TestManifestRejectsOverlappingVisibleSegments(t *testing.T) {
	manifest := NewManifest(1, 10, []SegmentRef{
		{Domain: kvdomains.SystemReward, Kind: SegmentHistory, FromTxNum: 1, ToTxNum: 5, Path: "a.seg"},
		{Domain: kvdomains.SystemReward, Kind: SegmentHistory, FromTxNum: 5, ToTxNum: 10, Path: "b.seg"},
	})
	if err := manifest.Validate(); err == nil {
		t.Fatal("expected overlapping segments to be rejected")
	}
}

func TestManifestRejectsUnsafePaths(t *testing.T) {
	tests := []string{
		"",
		"/abs.seg",
		"../parent.seg",
		"nested/../escape.seg",
		".",
	}
	for _, path := range tests {
		manifest := NewManifest(1, 1, []SegmentRef{
			{Domain: kvdomains.SystemReward, Kind: SegmentLatest, FromTxNum: 1, ToTxNum: 1, Path: path},
		})
		if err := manifest.Validate(); err == nil {
			t.Fatalf("path %q accepted", path)
		}
	}
}

func TestIsCleanRelativeSlashPathMatchesFilepathClean(t *testing.T) {
	cases := []string{
		"history/a-1-2.seg",
		"log/00001-00002.idx",
		"single.seg",
		"a/.hidden",
		"",
		"/",
		"/abs.seg",
		"trailing/",
		".",
		"..",
		"./rel.seg",
		"../parent.seg",
		"a/./b.seg",
		"a/../b.seg",
		"a//b.seg",
		`a\b.seg`,
	}
	for _, p := range cases {
		legacyReject := p == "" || filepath.IsAbs(p) || filepath.Clean(p) != p || p == "." || hasParentDir(p)
		fastAccept := isCleanRelativeSlashPath(p)
		if fastAccept && legacyReject {
			t.Fatalf("fast path accepted unsafe path %q", p)
		}
		if filepath.Separator == '/' && !legacyReject && p != `a\b.seg` && !fastAccept {
			t.Fatalf("fast path rejected clean path %q", p)
		}
	}
}

func TestValidateEventLogIndexCompanionsOrderedMatchesUnordered(t *testing.T) {
	refs := []SegmentRef{
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 10, ToTxNum: 20, Path: "log/10-20.seg", Size: 100},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 1, ToTxNum: 9, Path: "log/1-9.seg", Size: 100},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: 1, ToTxNum: 20, Path: "log/1-20.idx", Size: 50},
	}
	orderedManifest := &Manifest{Segments: append([]SegmentRef(nil), refs...)}
	sortSegments(orderedManifest.Segments)
	if err := validateEventLogIndexCompanionsWithOrder(orderedManifest, true); err != nil {
		t.Fatalf("ordered companion validation failed: %v", err)
	}
	if err := validateEventLogIndexCompanionsWithOrder(orderedManifest, false); err != nil {
		t.Fatalf("unordered companion validation failed: %v", err)
	}

	gapped := &Manifest{Segments: []SegmentRef{
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 1, ToTxNum: 9, Path: "log/1-9.seg", Size: 100},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 11, ToTxNum: 20, Path: "log/11-20.seg", Size: 100},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: 1, ToTxNum: 20, Path: "log/1-20.idx", Size: 50},
	}}
	sortSegments(gapped.Segments)
	if err := validateEventLogIndexCompanionsWithOrder(gapped, true); err == nil {
		t.Fatal("ordered companion validation accepted gap")
	}
}

func TestNextEventLogCatchupRangeZeroCopyMatchesCases(t *testing.T) {
	empty := &Manifest{}
	if got, ok := nextEventLogCatchupRange(empty, 100, 10); !ok || got.from != 1 || got.to != 10 {
		t.Fatalf("empty range = %+v ok=%v, want [1,10]", got, ok)
	}

	contiguous := &Manifest{Segments: []SegmentRef{
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 1, ToTxNum: 10, Path: "log/1-10.seg", Size: 1},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: 1, ToTxNum: 10, Path: "log/1-10.idx", Size: 1},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 15, ToTxNum: 20, Path: "log/15-20.seg", Size: 1},
	}}
	if got, ok := nextEventLogCatchupRange(contiguous, 25, 10); !ok || got.from != 11 || got.to != 14 {
		t.Fatalf("gap range = %+v ok=%v, want [11,14]", got, ok)
	}

	// Overlapping companion beyond covered=5 expands to full connected component [6,9].
	overlap := &Manifest{Segments: []SegmentRef{
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 1, ToTxNum: 5, Path: "log/1-5.seg", Size: 1},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: 1, ToTxNum: 5, Path: "log/1-5.idx", Size: 1},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 6, ToTxNum: 8, Path: "log/6-8.seg", Size: 1},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: 7, ToTxNum: 9, Path: "log/7-9.idx", Size: 1},
	}}
	if got, ok := nextEventLogCatchupRange(overlap, 20, 2); !ok || got.from != 6 || got.to != 9 {
		t.Fatalf("overlap range = %+v ok=%v, want [6,9]", got, ok)
	}
}
