package snapshots

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The entry count was observed in the 2026-09-15 03:15:55 UTC production
// event-log cache metric. The records below are synthetic metadata, not copied
// production proofs. This fixture measures JSON encoding, not cold throughput.
func verificationCacheEncodingFixture(eventCount int) chainFreezerVerificationDisk {
	disk := chainFreezerVerificationDisk{
		Version:      chainFreezerVerificationCacheVersion,
		Entries:      make([]chainFreezerVerificationRecord, 0),
		EventEntries: make([]eventLogVerificationRecord, 0, eventCount),
	}
	for i := 0; i < eventCount; i++ {
		from := uint64(30_000_000 + i*16)
		to := from + 15
		ref := SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex,
			FromTxNum: from, ToTxNum: to, Path: EventLogIndexSegmentPath(from, to),
			Size: 8192, Checksum: fmt.Sprintf("sha256:%064x", i+1)}
		event := ref
		event.Kind, event.Path, event.Size = SegmentEventLog, EventLogSegmentPath(from, to), 16384
		disk.EventEntries = append(disk.EventEntries, eventLogVerificationRecord{
			Index: ref, Events: []SegmentRef{event},
			IdentityKey: "8192:1789442155000000000;16384:1789442155000000000;",
		})
	}
	return disk
}

func TestChainFreezerVerificationCacheEncodingCompatibility(t *testing.T) {
	disk := verificationCacheEncodingFixture(3)
	for i := 0; i < 2; i++ {
		ref := SegmentRef{Dataset: SegmentDatasetChainFreezer, Kind: SegmentChainFreezer,
			FromTxNum: uint64(i * 16), ToTxNum: uint64(i*16 + 15),
			Path: fmt.Sprintf("chain/freezer-%d.seg", i), Size: 1024, Checksum: "sha256:" + strings.Repeat("a", 64)}
		index := ref
		index.Kind, index.Path = SegmentChainIndex, fmt.Sprintf("chain/index-%d.idx", i)
		record := chainFreezerVerificationRecord{Freezer: ref, Index: index}
		if i == 1 {
			record.HasAccessor, record.Accessor = true, ref
			record.Accessor.Kind, record.Accessor.Path = SegmentChainFreezerAccessor, "chain/accessor-1.acc"
		}
		disk.Entries = append(disk.Entries, record)
	}
	// Multiple source companions and exact identity strings must survive both
	// layouts, just as single-source proofs and optional chain accessors do.
	record := &disk.EventEntries[0]
	first, second := record.Events[0], record.Events[0]
	first.ToTxNum = first.FromTxNum + 7
	second.FromTxNum = first.ToTxNum + 1
	first.Path, second.Path = "log/first.seg", "log/second.seg"
	record.Events = []SegmentRef{first, second}
	record.IdentityKey += "16384:1789442155000000000;"
	legacy, err := json.MarshalIndent(disk, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, chainFreezerVerificationCacheFile)
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	cache := NewChainFreezerVerificationCache(dir)
	if err := cache.LoadError(); err != nil {
		t.Fatalf("legacy layout: %v", err)
	}
	cache.dirty = true
	if err := cache.persistPending(); err != nil {
		t.Fatal(err)
	}
	if cache.dirty {
		t.Fatal("successful persistence left dirty state")
	}
	compact, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var expected bytes.Buffer
	if err := json.Compact(&expected, legacy); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(compact, expected.Bytes()) {
		t.Fatal("persistence changed record values or ordering beyond whitespace")
	}
	restarted := NewChainFreezerVerificationCache(dir)
	if err := restarted.LoadError(); err != nil {
		t.Fatalf("compact layout: %v", err)
	}
	if !reflect.DeepEqual(cache.persistent, restarted.persistent) || !reflect.DeepEqual(cache.eventPersistent, restarted.eventPersistent) {
		t.Fatal("restart changed persistent record keys or values")
	}
	if stats := restarted.Stats(); stats.Entries != 2 || stats.EventEntries != 3 {
		t.Fatalf("restart entry counts: %+v", stats)
	}
	// A rewrite from the map must remain byte deterministic.
	restarted.dirty = true
	if err := restarted.persistPending(); err != nil {
		t.Fatal(err)
	}
	again, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(compact, again) {
		t.Fatalf("second persistence differs: %v", err)
	}
}

func TestChainFreezerVerificationCacheEncodingStrictLoad(t *testing.T) {
	valid, err := json.Marshal(verificationCacheEncodingFixture(1))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"unknown_field": bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"unexpected":true`), 1),
		"wrong_version": bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":2`), 1),
		"bad_checksum":  bytes.Replace(valid, []byte(fmt.Sprintf("%064x", 1)), []byte("invalid"), 1),
		"bad_path":      bytes.Replace(valid, []byte(`"path":"log/`), []byte(`"path":"../`), 1),
	}
	// An empty companion set is invalid independently of its JSON layout.
	invalid := verificationCacheEncodingFixture(1)
	invalid.EventEntries[0].Events = nil
	cases["incomplete_coverage"], err = json.Marshal(invalid)
	if err != nil {
		t.Fatal(err)
	}
	for name, compact := range cases {
		t.Run(name, func(t *testing.T) {
			var pretty bytes.Buffer
			if err := json.Indent(&pretty, compact, "", "  "); err != nil {
				t.Fatal(err)
			}
			for _, data := range [][]byte{compact, pretty.Bytes()} {
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, chainFreezerVerificationCacheFile), data, 0o600); err != nil {
					t.Fatal(err)
				}
				cache := NewChainFreezerVerificationCache(dir)
				if cache.LoadError() == nil || cache.Stats().LoadErrors != 1 || cache.Stats().EventEntries != 0 || !cache.dirty {
					t.Fatalf("malformed proof did not fall back completely: error=%v stats=%+v dirty=%v", cache.LoadError(), cache.Stats(), cache.dirty)
				}
			}
		})
	}
	t.Run("trailing_json", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, chainFreezerVerificationCacheFile), append(valid, []byte("\n{}")...), 0o600); err != nil {
			t.Fatal(err)
		}
		if cache := NewChainFreezerVerificationCache(dir); cache.LoadError() == nil {
			t.Fatal("accepted trailing JSON")
		}
	})
}

func BenchmarkChainFreezerVerificationCacheEncoding(b *testing.B) {
	disk := verificationCacheEncodingFixture(39_069)
	for _, record := range disk.EventEntries {
		if err := validateEventLogVerificationRecord(record); err != nil {
			b.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name   string
		encode func(any) ([]byte, error)
	}{
		{"legacy_indent", func(v any) ([]byte, error) { return json.MarshalIndent(v, "", "  ") }},
		{"compact", json.Marshal},
	} {
		b.Run(tc.name, func(b *testing.B) {
			encoded, err := tc.encode(disk)
			if err != nil {
				b.Fatal(err)
			}
			var decoded chainFreezerVerificationDisk
			if err := json.Unmarshal(encoded, &decoded); err != nil || !reflect.DeepEqual(decoded, disk) {
				b.Fatalf("encoding changed decoded proof metadata: %v", err)
			}
			if len(encoded) > maxChainFreezerVerificationCacheBytes {
				b.Fatal("fixture exceeds the production cache byte limit")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := tc.encode(disk)
				if err != nil {
					b.Fatal(err)
				}
				if len(data) != len(encoded) {
					b.Fatal("encoded length changed")
				}
			}
			b.ReportMetric(float64(len(encoded)), "encoded-bytes")
			b.ReportMetric(float64(len(disk.EventEntries)), "event-records")
		})
	}
}
