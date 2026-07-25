package blockbuffer

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func readSessionBranch(t *testing.T, session pointread.CommitmentParentSession, reader int, prefix []byte) ([]byte, bool, bool) {
	t.Helper()
	var got []byte
	var stable bool
	found, err := rawdb.ViewCommitmentParentBranchInSession(session, reader, prefix, func(value []byte, valueStable bool) error {
		got = append(got, value...)
		stable = valueStable
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return got, found, stable
}

func TestCommitmentParentReadSessionKeepsOverlayAndDurableCut(t *testing.T) {
	disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	durablePrefix := []byte{1, 2}
	overridePrefix := []byte{2, 3}
	if err := rawdb.WriteCommitmentBranch(disk, durablePrefix, []byte("durable-before")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteCommitmentBranch(disk, overridePrefix, []byte("override-before")); err != nil {
		t.Fatal(err)
	}

	buf := New(disk)
	buf.SetBaseReadCacheSize(1 << 20)
	buf.BeginBlock(bufHash(1), 1)
	h1, _ := buf.NewestInflight()
	if err := rawdb.WriteCommitmentBranch(buf.ViewLayer(h1), overridePrefix, []byte("override-layer")); err != nil {
		t.Fatal(err)
	}
	if err := buf.CommitInflight(h1); err != nil {
		t.Fatal(err)
	}
	buf.BeginBlock(bufHash(2), 2)
	h2, _ := buf.NewestInflight()
	session, err := buf.ViewLayer(h2).NewCommitmentParentReadSession(17)
	if err != nil || session == nil {
		t.Fatalf("NewCommitmentParentReadSession = (%T,%v)", session, err)
	}
	defer session.Close()

	// Move the captured overlay into the durable DB, then mutate a row that was
	// already durable. The session must retain both sides of its original cut.
	if err := buf.FlushUpTo(1, disk); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteCommitmentBranch(disk, durablePrefix, []byte("durable-after")); err != nil {
		t.Fatal(err)
	}

	if got, found, stable := readSessionBranch(t, session, 2, overridePrefix); !found || !stable || !bytes.Equal(got, []byte("override-layer")) {
		t.Fatalf("overlay = (%q,%v,stable=%v)", got, found, stable)
	}
	if got, found, stable := readSessionBranch(t, session, 1, durablePrefix); !found || stable || !bytes.Equal(got, []byte("durable-before")) {
		t.Fatalf("durable snapshot = (%q,%v,stable=%v)", got, found, stable)
	}
}
