package rawdb

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

func TestDiskSpaceObservationRangesCoverFamilies(t *testing.T) {
	ranges := DiskSpaceObservationRanges()
	if len(ranges) != 12 {
		t.Fatalf("range count = %d, want 12", len(ranges))
	}
	cases := []struct {
		key, family string
	}{
		{"state-account-latest-v1-\x00owner\xff", "account_latest"},
		{"state-kv-latest-v2-\x00owner\xff", "account_kv_latest"},
		{"state-kv-generation-v2-\x00owner\xff", "kv_generation"},
		{"state-code-v1-\x00hash\xff", "state_code"},
		{"state-commitment-branch-v1-\x00\xff", "commitment"},
		{"state-commitment-branch-delta-v1-\x00\xff", "commitment"},
		{"state-commitment-branch-base-v1", "commitment"},
		{"state-commitment-branch-rotation-v1", "commitment"},
		{"state-commitment-domain-v1-checkpoint/\x00\xff", "commitment"},
		{"state-commitment-engine-state-v1", "commitment"},
		{"state-changeset-v2-\x00\xff", "state_changeset"},
		{"state-change-posting-v3-\x00\xff", "state_change_index"},
		{"state-change-keys-v3-state-kv-latest-v2-\x00\xff", "state_change_index"},
		{"state-tx-range-v1-\x00\xff", "state_tx_range"},
		{"sync-staged-block-v1-\x00\xff", "staged_body"},
		{"b-\x00\xff", "block_body"},
		{"tx-\x00\xff", "transaction_index"},
		{"ti-\x00\xff", "transaction_receipts"},
		{"tib-\x00\xff", "transaction_receipts"},
		// The observer intentionally does not estimate every legacy, trace,
		// chain-metadata or singleton namespace.
		{"state-account-latest-v2-owner", ""},
		{"a-owner", ""},
		{"s-storage", ""},
		{"bh-hash", ""},
		{"bnh-number", ""},
		{"bsr-root", ""},
		{"btrace-block", ""},
		{"tps-slot", ""},
		{"stage-progress-v1-Finish", ""},
		{"LastBlock", ""},
		{"state-commitment.", ""},
		{"state-change.", ""},
		{"state-changeset-v2.", ""},
		{"tj", ""},
	}
	covered := make(map[string]bool)
	for _, test := range cases {
		var matched []string
		for _, r := range ranges {
			if bytes.Compare([]byte(test.key), r.Start) >= 0 && bytes.Compare([]byte(test.key), r.End) < 0 {
				matched = append(matched, r.Name)
			}
		}
		if test.family == "" {
			if len(matched) != 0 {
				t.Errorf("unselected key %q matched %v", test.key, matched)
			}
		} else if len(matched) != 1 || matched[0] != test.family {
			t.Errorf("key %q matched %v, want only %s", test.key, matched, test.family)
		} else {
			covered[test.family] = true
		}
	}
	if len(covered) != len(ranges) {
		t.Fatalf("covered %d of %d range names", len(covered), len(ranges))
	}
}

func TestDiskSpaceObservationRangesDisjointAndOwned(t *testing.T) {
	ranges := DiskSpaceObservationRanges()
	want := DiskSpaceObservationRanges()
	seen := make(map[string]bool)
	for i, r := range ranges {
		if r.Name == "" || seen[r.Name] {
			t.Fatalf("empty or repeated name %q", r.Name)
		}
		seen[r.Name] = true
		if len(r.Start) == 0 || len(r.End) == 0 || bytes.Compare(r.Start, r.End) >= 0 {
			t.Fatalf("invalid range %s: [%x, %x)", r.Name, r.Start, r.End)
		}
		for j := 0; j < i; j++ {
			prior := ranges[j]
			if bytes.Compare(prior.End, r.Start) > 0 && bytes.Compare(r.End, prior.Start) > 0 {
				t.Fatalf("ranges %s and %s overlap", prior.Name, r.Name)
			}
		}
	}
	for i := range ranges {
		ranges[i].Name = "changed"
		for j := range ranges[i].Start {
			ranges[i].Start[j] = 0xff
		}
		for j := range ranges[i].End {
			ranges[i].End[j] = 0
		}
	}
	if got := DiskSpaceObservationRanges(); !reflect.DeepEqual(got, want) {
		t.Fatal("returned ranges alias schema storage or another result")
	}
}

func TestDiskSpaceObservationSwitch(t *testing.T) {
	input := PebbleOptions{
		LBaseMaxBytes: 17 << 20,
		DiskSpaceRanges: []pebbledb.DiskSpaceRange{
			{Name: "caller", Start: []byte("a"), End: []byte("b")},
		},
	}
	for _, value := range []string{"", "0", "1", "true", "false", "2", " 1", "1\n"} {
		t.Run("value="+value, func(t *testing.T) {
			t.Setenv(diskSpaceObserverEnv, value)
			got, err := withDiskSpaceObservation(input)
			if value != "" && value != "0" && value != "1" {
				if err == nil || !strings.Contains(err.Error(), diskSpaceObserverEnv) {
					t.Fatalf("invalid switch returned %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.LBaseMaxBytes != input.LBaseMaxBytes {
				t.Fatal("space observation changed unrelated tuning")
			}
			if value != "1" {
				if got.DiskSpaceRanges != nil {
					t.Fatal("disabled switch retained caller-supplied ranges")
				}
				return
			}
			want := DiskSpaceObservationRanges()
			if len(got.DiskSpaceRanges) != len(want) {
				t.Fatalf("enabled ranges = %d, want %d", len(got.DiskSpaceRanges), len(want))
			}
			for i, r := range got.DiskSpaceRanges {
				if r.Name != want[i].Name || !bytes.Equal(r.Start, want[i].Start) || !bytes.Equal(r.End, want[i].End) {
					t.Fatalf("converted range %d differs from schema", i)
				}
				got.DiskSpaceRanges[i].Start[0] = 0
				got.DiskSpaceRanges[i].End[0] = 0
			}
			fresh, err := withDiskSpaceObservation(input)
			if err != nil {
				t.Fatal(err)
			}
			for i, r := range fresh.DiskSpaceRanges {
				if !bytes.Equal(r.Start, want[i].Start) || !bytes.Equal(r.End, want[i].End) {
					t.Fatal("enabled opens share mutable range bounds")
				}
			}
		})
	}
	if len(input.DiskSpaceRanges) != 1 || input.DiskSpaceRanges[0].Name != "caller" || string(input.DiskSpaceRanges[0].Start) != "a" || string(input.DiskSpaceRanges[0].End) != "b" {
		t.Fatal("space observation mutated caller options")
	}
}

func TestDiskSpaceObservationInvalidSwitchBeforeOpen(t *testing.T) {
	t.Setenv(diskSpaceObserverEnv, "invalid")
	constructors := []struct {
		name string
		open func(string) (ethdb.KeyValueStore, error)
	}{
		{"default", func(path string) (ethdb.KeyValueStore, error) { return NewPebbleDB(path, 16, 16) }},
		{"options", func(path string) (ethdb.KeyValueStore, error) {
			return NewPebbleDBWithOptions(path, 16, 16, DefaultPebbleOptions())
		}},
	}
	for _, constructor := range constructors {
		path := filepath.Join(t.TempDir(), constructor.name)
		db, err := constructor.open(path)
		if db != nil {
			_ = db.Close()
			t.Fatalf("%s opened a database with invalid switch", constructor.name)
		}
		if err == nil || !strings.Contains(err.Error(), diskSpaceObserverEnv) {
			t.Fatalf("%s returned %v", constructor.name, err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s touched the database path: %v", constructor.name, err)
		}
	}
}

func TestDiskSpaceObservationReadOnlyIgnoresSwitch(t *testing.T) {
	t.Setenv(diskSpaceObserverEnv, "0")
	path := filepath.Join(t.TempDir(), "db")
	db, err := NewPebbleDB(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"1", "invalid"} {
		t.Setenv(diskSpaceObserverEnv, value)
		reader, err := NewPebbleDBReadOnly(path, 16, 16)
		if err != nil {
			t.Fatalf("read-only open with switch %q: %v", value, err)
		}
		got, readErr := reader.Get([]byte("key"))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || string(got) != "value" {
			t.Fatalf("read-only get/close with switch %q: value=%q, errors=%v/%v", value, got, readErr, closeErr)
		}
	}
}
