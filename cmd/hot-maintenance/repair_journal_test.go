package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestRepairJournalDurablePhasesAndCorruptionFailClosed(t *testing.T) {
	cold := t.TempDir()
	j := repairJournal{Version: 1, Phase: journalPrepared, OldSHA: [32]byte{1}, CandidateSHA: [32]byte{2}, PlanSHA: [32]byte{3}, Epoch: 1, PublishedUnix: 1,
		Plan: snapshots.OfflineHistoryRepairPlan{SourceRefs: []snapshots.SegmentRef{{Path: "old"}}}, NewRefs: []snapshots.SegmentRef{{Path: "new"}}, Bindings: []rawdb.HistoryStagingColdBinding{{Bucket: 1}}, Slices: []repairTargetSlice{{Bucket: 1}}}
	if err := writeRepairJournal(cold, j); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{journalPrepared, journalPublished, journalRebound} {
		got, found, err := readRepairJournal(cold)
		if err != nil || !found || got.Phase != phase {
			t.Fatalf("read phase %s: %+v found=%v err=%v", phase, got, found, err)
		}
		if phase != journalRebound {
			if phase == journalPrepared {
				j.Phase = journalPublished
			} else {
				j.Phase = journalRebound
			}
			if err := writeRepairJournal(cold, j); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writeRepairJournal(cold, j); err == nil {
		t.Fatal("completed journal was overwritten")
	}
	path := repairJournalPath(cold)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-2] ^= 1
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readRepairJournal(cold); err == nil {
		t.Fatal("corrupted journal was accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(cold, "missing"), path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readRepairJournal(cold); err == nil {
		t.Fatal("symlink journal was accepted")
	}
}
