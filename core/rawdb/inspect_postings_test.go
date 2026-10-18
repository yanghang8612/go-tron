package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

func TestPostingCensusMatchesActualWholeFrameDeletion(t *testing.T) {
	for _, h := range []uint64{2, 4, 10} {
		t.Run(strconv.FormatUint(h, 10), func(t *testing.T) {
			db, err := NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			frames := [][]uint64{{0}, {1, 2}, {2, 4}, {3, 5}, {6, 7}, {10}}
			for i, blocks := range frames {
				putPostingChunkFrame(t, db, byte(i+1), blocks...)
			}
			directory := stateChangeKeyDirectoryKey([]byte("state-kv-latest-v2-retained"))
			if err := db.Put(directory, nil); err != nil {
				t.Fatal(err)
			}
			for _, key := range [][]byte{{0}, []byte("zz-after-postings"), bytes.Clone(stateChangePostingPrefix)} {
				if bytes.Equal(key, stateChangePostingPrefix) {
					// This exact prefix would be a malformed posting; use the
					// immediately preceding key to exercise the first scan range.
					key[len(key)-1]--
				}
				if err := db.Put(key, []byte("non-posting-preserve")); err != nil {
					t.Fatal(err)
				}
			}
			complementBefore, err := InspectStateChangePostingComplement(context.Background(), db)
			if err != nil || complementBefore.Rows != 4 {
				t.Fatalf("complement=%+v error=%v", complementBefore, err)
			}
			before, err := InspectStateChangePostings(context.Background(), db, []uint64{h}, nil)
			if err != nil || !before.Complete || before.All.Frames != uint64(len(frames)) || before.DirectoryRows != 1 || before.DirectoryBytes != uint64(len(directory)) {
				t.Fatalf("before census=%+v err=%v", before, err)
			}
			var eligible, crossing, newer uint64
			for _, blocks := range frames {
				if blocks[len(blocks)-1] <= h {
					eligible++
				} else if blocks[0] <= h {
					crossing++
				} else {
					newer++
				}
			}
			c := before.Cutoffs[0]
			if c.Eligible.Frames != eligible || c.Crossing.Frames != crossing || c.Newer.Frames != newer ||
				c.Eligible.LogicalBytes+c.Crossing.LogicalBytes+c.Newer.LogicalBytes != before.All.LogicalBytes {
				t.Fatalf("independent classification disagrees: %+v", c)
			}
			// Independent production deleter is the byte/decision oracle.
			result, err := PruneStaleStateChangePostingChunkContext(context.Background(), db, h, nil, postingChunkTestLimits())
			if err != nil || !result.Complete || result.RowsDeleted != c.Eligible.Frames || result.BytesDeleted != c.Eligible.LogicalBytes {
				t.Fatalf("deletion=%+v err=%v census=%+v", result, err, c.Eligible)
			}
			if err := db.Compact(nil, nil); err != nil {
				t.Fatal(err)
			}
			after, err := InspectStateChangePostings(context.Background(), db, []uint64{h}, nil)
			if err != nil || !after.Complete || after.Cutoffs[0].Eligible.Frames != 0 ||
				!reflect.DeepEqual(after.Cutoffs[0].Crossing, c.Crossing) || !reflect.DeepEqual(after.Cutoffs[0].Newer, c.Newer) ||
				after.DirectorySHA != before.DirectorySHA || after.DirectoryRows != 1 || after.All.LogicalBytes+c.Eligible.LogicalBytes != before.All.LogicalBytes {
				t.Fatalf("retained rows changed after real compaction: after=%+v err=%v", after, err)
			}
			complementAfter, err := InspectStateChangePostingComplement(context.Background(), db)
			if err != nil || complementBefore != complementAfter {
				t.Fatalf("non-posting rows changed: before=%+v after=%+v err=%v", complementBefore, complementAfter, err)
			}
		})
	}
}

type postingCensusFaultReader struct {
	ethdb.Iteratee
	iterations int
	err        error
}

func (db *postingCensusFaultReader) NewIterator(prefix, start []byte) ethdb.Iterator {
	db.iterations++
	return &postingCensusFaultIterator{Iterator: db.Iteratee.NewIterator(prefix, start), fault: db.err}
}

type postingCensusFaultIterator struct {
	ethdb.Iterator
	fault error
}

func (it *postingCensusFaultIterator) Error() error {
	return errors.Join(it.Iterator.Error(), it.fault)
}

func TestPostingCensusErrorsAreIncomplete(t *testing.T) {
	for _, phase := range []string{"posting", "directory", "canceled", "malformed"} {
		t.Run(phase, func(t *testing.T) {
			db := NewMemoryDatabase()
			defer db.Close()
			key, _ := putPostingChunkFrame(t, db, 1, 1, 2)
			if phase == "malformed" {
				if err := db.Put(key, []byte{1, 2, 0}); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Put(stateChangeKeyDirectoryKey([]byte("a")), []byte{1}); phase != "directory" {
				if err := db.Delete(stateChangeKeyDirectoryKey([]byte("a"))); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader := &postingCensusFaultReader{Iteratee: db}
			if phase == "posting" {
				reader.err = errors.New("injected iterator failure")
			}
			if phase == "canceled" {
				cancel()
			}
			r, err := InspectStateChangePostings(ctx, reader, []uint64{2}, nil)
			if err == nil || r.Complete || r.Error == "" {
				t.Fatalf("failed scan reported success: %+v %v", r, err)
			}
			if phase == "canceled" && reader.iterations != 0 {
				t.Fatal("pre-canceled census created an iterator")
			}
		})
	}
}

func TestPostingCensusMultipleCutoffsAndFullValidation(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	putPostingChunkFrame(t, db, 1, 1, 2, 4)
	putPostingChunkFrame(t, db, 1, 5, 7)
	putPostingChunkFrame(t, db, 2, 3)
	r, err := InspectStateChangePostings(context.Background(), db, []uint64{2, 4, 7}, nil)
	if err != nil || !r.Complete || r.All.KeyHashes != 2 || r.All.BlockReferences != 6 || r.All.FrameRows[3] != 1 || r.All.FrameRows[2] != 1 || r.All.FrameRows[1] != 1 {
		t.Fatalf("census=%+v %v", r, err)
	}
	for i, h := range []uint64{2, 4, 7} {
		single, err := InspectStateChangePostings(context.Background(), db, []uint64{h}, nil)
		if err != nil || !reflect.DeepEqual(single.Cutoffs[0], r.Cutoffs[i]) {
			t.Fatal("single and multi-cutoff digests/decisions differ")
		}
	}
	for _, cutoffs := range [][]uint64{nil, {0}, {3, 2}, {2, 2}, {1, 2, 3, 4, 5, 6, 7, 8, 9}} {
		if _, err := InspectStateChangePostings(context.Background(), db, cutoffs, nil); err == nil {
			t.Fatalf("accepted invalid cutoffs %v", cutoffs)
		}
	}
	// A corrupt value outside the eligible prefix must still stop the census.
	var keyHash [32]byte
	keyHash[0] = 3
	bad := append([]byte{1}, binary.AppendUvarint(nil, 2)...)
	bad = append(bad, 0)
	if err := db.Put(stateChangePostingKey(keyHash, 100), bad); err != nil {
		t.Fatal(err)
	}
	r, err = InspectStateChangePostings(context.Background(), db, []uint64{2}, nil)
	if err == nil || r.Complete || r.All.Frames != 3 || r.Error == "" {
		t.Fatalf("newer corrupt frame hidden: %+v %v", r, err)
	}
}
