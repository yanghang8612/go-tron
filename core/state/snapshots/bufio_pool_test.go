package snapshots

import (
	"bytes"
	"testing"
)

func TestStateDomainChangeHistoryWriterPoolDiscardsAbortedBytes(t *testing.T) {
	var aborted bytes.Buffer
	writer := acquireStateDomainChangeHistoryWriter(&aborted)
	retained := writer
	if _, err := writer.WriteString("aborted"); err != nil {
		t.Fatal(err)
	}
	if retained.Buffered() == 0 {
		t.Fatal("test did not leave buffered bytes to discard")
	}

	releaseStateDomainChangeHistoryWriter(&writer)
	if writer != nil {
		t.Fatal("released writer still reachable through caller")
	}
	if retained.Buffered() != 0 {
		t.Fatalf("released writer retained %d aborted bytes", retained.Buffered())
	}
	if aborted.Len() != 0 {
		t.Fatalf("release flushed aborted bytes: %q", aborted.Bytes())
	}

	var committed bytes.Buffer
	writer = acquireStateDomainChangeHistoryWriter(&committed)
	defer releaseStateDomainChangeHistoryWriter(&writer)
	if _, err := writer.WriteString("committed"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := committed.String(); got != "committed" {
		t.Fatalf("reused writer output = %q, want committed", got)
	}
}

func TestStateDomainChangeAccessorGroupOffsetsPoolResetsLength(t *testing.T) {
	offsets := acquireStateDomainChangeAccessorGroupOffsets()
	*offsets = append(*offsets, 1, 2, 3)
	retained := offsets
	releaseStateDomainChangeAccessorGroupOffsets(&offsets)
	if offsets != nil {
		t.Fatal("released offsets still reachable through caller")
	}
	if len(*retained) != 0 {
		t.Fatalf("released offsets retained length %d", len(*retained))
	}

	offsets = acquireStateDomainChangeAccessorGroupOffsets()
	defer releaseStateDomainChangeAccessorGroupOffsets(&offsets)
	if len(*offsets) != 0 {
		t.Fatalf("acquired offsets length = %d, want zero", len(*offsets))
	}
}
