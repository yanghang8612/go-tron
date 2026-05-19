package rawdb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

// TestChainDBNoopAncient composes a memdb with a NoopAncient and confirms
// the embedded KV interface still works while every Ancient call reports
// "not found".
func TestChainDBNoopAncient(t *testing.T) {
	t.Parallel()

	kv := NewMemoryDatabase()
	cdb := NewChainDB(kv, NoopAncient{})

	// KV round-trip via the embedded interface.
	if err := cdb.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := cdb.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get returned %q", got)
	}

	// Every kind reports zero count and a not-in-ancient error.
	for _, kind := range []string{"headers", "bodies", "tx_infos", "state_roots"} {
		count, err := cdb.AncientCount(kind)
		if err != nil {
			t.Fatalf("AncientCount(%s): %v", kind, err)
		}
		if count != 0 {
			t.Fatalf("AncientCount(%s)=%d on NoopAncient", kind, count)
		}
		if _, err := cdb.Ancient(kind, 0); !errors.Is(err, ErrNotInAncient) {
			t.Fatalf("Ancient(%s, 0): want ErrNotInAncient, got %v", kind, err)
		}
		ok, err := cdb.HasAncient(kind, 0)
		if err != nil {
			t.Fatalf("HasAncient(%s, 0): %v", kind, err)
		}
		if ok {
			t.Fatalf("HasAncient(%s, 0) returned true on NoopAncient", kind)
		}
	}
}

// TestChainDBNilAncient confirms NewChainDB substitutes NoopAncient when the
// caller passes a nil reader (matches the slice-1 "freezer disabled" config).
func TestChainDBNilAncient(t *testing.T) {
	t.Parallel()
	cdb := NewChainDB(NewMemoryDatabase(), nil)
	if cdb.AncientReader == nil {
		t.Fatalf("nil AncientReader after NewChainDB(_, nil)")
	}
	if _, err := cdb.Ancient("headers", 0); !errors.Is(err, ErrNotInAncient) {
		t.Fatalf("Ancient on nil-AncientReader path: want ErrNotInAncient, got %v", err)
	}
}

// TestChainDBFreezerReader plumbs a real on-disk freezer through ChainDB and
// confirms the error translation maps freezer.ErrOutOfBounds → ErrNotInAncient.
func TestChainDBFreezerReader(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tables := map[string]freezer.TableConfig{
		"headers": {NoSnappy: false},
	}
	f, err := freezer.NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatalf("freezer: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	if _, err := f.ModifyAncients(func(op freezer.AncientWriteOp) error {
		return op.AppendRaw("headers", 0, []byte("first"))
	}); err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}

	cdb := NewChainDB(NewMemoryDatabase(), NewFreezerReader(f))

	// In-bounds read works.
	got, err := cdb.Ancient("headers", 0)
	if err != nil {
		t.Fatalf("Ancient: %v", err)
	}
	if !bytes.Equal(got, []byte("first")) {
		t.Fatalf("Ancient: %x", got)
	}
	// Out-of-bounds translates to ErrNotInAncient.
	if _, err := cdb.Ancient("headers", 1); !errors.Is(err, ErrNotInAncient) {
		t.Fatalf("post-head read: want ErrNotInAncient, got %v", err)
	}
	// Unknown table translates to ErrNotInAncient.
	if _, err := cdb.Ancient("missing", 0); !errors.Is(err, ErrNotInAncient) {
		t.Fatalf("unknown table: want ErrNotInAncient, got %v", err)
	}
	// HasAncient at the existing item is true.
	ok, err := cdb.HasAncient("headers", 0)
	if err != nil {
		t.Fatalf("HasAncient: %v", err)
	}
	if !ok {
		t.Fatalf("HasAncient(headers,0)=false")
	}
}
