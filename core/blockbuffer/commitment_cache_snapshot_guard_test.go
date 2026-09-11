package blockbuffer

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// An epoch sampled at a cache miss only covers mutations after that miss. A
// session can take its durable snapshot first, observe its first miss after a
// newer flush, and therefore read old bytes with the already-new invalidation
// epoch. The old snapshot must remain readable without seeding the shared cache
// with a stale present value or negative entry for a later session.
func TestCommitmentCacheSnapshotGuardAfterFlushBeforeFirstProbe(t *testing.T) {
	for _, source := range []string{"foreground", "prefetch"} {
		for _, tc := range []struct {
			name   string
			before []byte
			after  []byte
		}{
			{name: "overwrite", before: []byte("before"), after: []byte("after")},
			{name: "delete", before: []byte("before")},
			{name: "negative-then-create", after: []byte("after")},
		} {
			t.Run(source+"/"+tc.name, func(t *testing.T) {
				disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := disk.Close(); err != nil {
						t.Errorf("close disk: %v", err)
					}
				}()
				path := []byte{6, 1, 2, 3, 4, 5}
				if tc.before != nil {
					if err := rawdb.WriteCommitmentBranch(disk, path, tc.before); err != nil {
						t.Fatal(err)
					}
				}
				buf := New(disk)
				buf.SetBaseReadCacheSizeWithTrunk(1<<20, 4, rawdb.CommitmentBranchKeyPrefix)
				buf.BeginBlock(bufHash(1), 1)
				first, _ := buf.NewestInflight()
				old, err := buf.ViewLayer(first).NewCommitmentParentReadSession(1)
				if err != nil || old == nil {
					t.Fatalf("old parent session = (%T, %v)", old, err)
				}
				defer func() {
					if err := old.Close(); err != nil {
						t.Errorf("close old: %v", err)
					}
				}()

				// This layer is excluded from old's captured parent topology. Flush
				// its later mutation before old has ever probed this physical key.
				if tc.after == nil {
					err = rawdb.DeleteCommitmentBranch(buf.ViewLayer(first), path)
				} else {
					err = rawdb.WriteCommitmentBranch(buf.ViewLayer(first), path, tc.after)
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := buf.CommitInflight(first); err != nil {
					t.Fatal(err)
				}
				if err := buf.FlushUpTo(1, disk); err != nil {
					t.Fatal(err)
				}

				// Repeat to cross ordinary two-hit admission, including a negative
				// value. Prefetch force-admission must obey the same snapshot guard.
				for attempt := 0; attempt < 2; attempt++ {
					if source == "prefetch" {
						found, err := rawdb.LegacyCommitmentBranchKeyspace().PrefetchParentInSession(
							old.(pointread.CommitmentParentPrefetchSession), 0, path)
						if err != nil || found != (tc.before != nil) {
							t.Fatalf("old snapshot prefetch = (%v, %v), want present=%v", found, err, tc.before != nil)
						}
					} else {
						got, found, _ := readSessionBranch(t, old, 0, path)
						if found != (tc.before != nil) || !bytes.Equal(got, tc.before) {
							t.Fatalf("old snapshot = (%q, %v), want (%q, %v)", got, found, tc.before, tc.before != nil)
						}
					}
				}

				buf.BeginBlock(bufHash(2), 2)
				second, _ := buf.NewestInflight()
				fresh, err := buf.ViewLayer(second).NewCommitmentParentReadSession(1)
				if err != nil || fresh == nil {
					t.Fatalf("fresh parent session = (%T, %v)", fresh, err)
				}
				defer func() {
					if err := fresh.Close(); err != nil {
						t.Errorf("close fresh: %v", err)
					}
				}()
				got, found, _ := readSessionBranch(t, fresh, 0, path)
				if found != (tc.after != nil) || !bytes.Equal(got, tc.after) {
					t.Fatalf("fresh snapshot after old read = (%q, %v), want latest (%q, %v)", got, found, tc.after, tc.after != nil)
				}
			})
		}
	}
}
