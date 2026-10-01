package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHistoryStagingJobPlanRecoversAfterPublishCrash(t *testing.T) {
	dir := t.TempDir()
	header := historyStagingPlanHeader{Version: 1,
		JobID: strings.Repeat("a", 32), CandidateSHA256: strings.Repeat("b", 64),
		LastBucket: 1, FullEligibleLastBucket: 2}
	data, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	job := filepath.Join(dir, header.JobID+".jsonl")
	if err := os.WriteFile(job, data, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	want := hex.EncodeToString(digest[:])
	for attempt := 0; attempt < 2; attempt++ {
		got, found, err := recoverHistoryStagingJobPlan(dir, header)
		if err != nil || !found || got != want {
			t.Fatalf("recovery %d: id=%q found=%v err=%v", attempt, got, found, err)
		}
		jobInfo, err := os.Stat(job)
		if err != nil {
			t.Fatal(err)
		}
		idInfo, err := os.Stat(filepath.Join(dir, want+".jsonl"))
		if err != nil || !os.SameFile(jobInfo, idInfo) {
			t.Fatalf("recovery %d did not preserve one durable plan inode: %v", attempt, err)
		}
	}
	other := header
	other.FullEligibleLastBucket++
	if _, _, err := recoverHistoryStagingJobPlan(dir, other); err == nil {
		t.Fatal("same job accepted a different eligibility boundary")
	}
}

func TestHistoryStagingPartialPlanCannotVerifyComplete(t *testing.T) {
	partial := &historyStagingPlanReader{header: historyStagingPlanHeader{
		LastBucket: 1, FullEligibleLastBucket: 2}}
	if partial.CompleteEligibleCoverage() {
		t.Fatal("truncated plan could activate the reader")
	}
	partial.header.LastBucket = 2
	if !partial.CompleteEligibleCoverage() {
		t.Fatal("full plan rejected")
	}
}

func TestHistoryStagingPlanRowRejectsUnknownAndTrailingJSON(t *testing.T) {
	var header historyStagingPlanHeader
	for _, row := range [][]byte{
		[]byte(`{"version":1,"unexpected":true}`),
		[]byte(`{"version":1} {"version":2}`),
	} {
		if err := decodeHistoryStagingPlanRow(row, &header); err == nil {
			t.Fatalf("unsafe plan row accepted: %s", row)
		}
	}
	if err := encodeHistoryStagingPlanRow(io.Discard, strings.Repeat("x", historyStagingMaxPlanRowBytes)); err == nil {
		t.Fatal("oversized JSONL row accepted")
	}
}
