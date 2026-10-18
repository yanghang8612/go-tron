package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
)

// PostingCensusBytes counts visible, uncompressed KV bytes. It excludes SST
// indexes/compression, old versions, WAL, tombstones and compaction scratch.
// KeyHashes is distinct only within this class; classes can share a hash.
type PostingCensusBytes struct {
	Frames          uint64      `json:"frames"`
	BlockReferences uint64      `json:"block_references"`
	KeyHashes       uint64      `json:"key_hashes"`
	KeyBytes        uint64      `json:"key_bytes"`
	ValueBytes      uint64      `json:"value_bytes"`
	LogicalBytes    uint64      `json:"logical_bytes"`
	FrameRows       [257]uint64 `json:"frame_rows"`
	SHA256          string      `json:"framed_kv_sha256"`
}

type PostingCensusCutoff struct {
	ThroughBlock uint64             `json:"through_block"`
	Eligible     PostingCensusBytes `json:"eligible_whole_frames"`
	Crossing     PostingCensusBytes `json:"retained_crossing_frames"`
	Newer        PostingCensusBytes `json:"retained_newer_frames"`
}

type PostingCensus struct {
	Complete       bool                  `json:"complete"`
	StartedUTC     time.Time             `json:"started_utc"`
	ElapsedSeconds float64               `json:"elapsed_seconds"`
	All            PostingCensusBytes    `json:"all_postings"`
	Cutoffs        []PostingCensusCutoff `json:"cutoffs"`
	DirectoryRows  uint64                `json:"retained_directory_rows"`
	DirectoryBytes uint64                `json:"retained_directory_logical_bytes"`
	DirectorySHA   string                `json:"directory_framed_kv_sha256"`
	Error          string                `json:"error,omitempty"`
}

// PostingComplementCensus covers every visible KV outside the posting prefix,
// including directories and all other history data. It is a preservation
// oracle for experiments on disposable clones, not a deletion authorization.
type PostingComplementCensus struct {
	Rows         uint64 `json:"rows"`
	LogicalBytes uint64 `json:"logical_bytes"`
	SHA256       string `json:"framed_kv_sha256"`
}

func InspectStateChangePostingComplement(ctx context.Context, db ethdb.Iteratee) (report PostingComplementCensus, err error) {
	if ctx == nil || db == nil {
		return report, errors.New("rawdb: posting complement requires context and reader")
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte("go-tron-posting-complement-v1\x00"))
	var length [8]byte
	// Skip the entire posting range, rather than reading its billions of
	// frames again. Prefix definitions and their lexicographic successor stay
	// in the schema-owning package.
	upper := bytes.Clone(stateChangePostingPrefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 255 {
			upper[i]++
			upper = upper[:i+1]
			break
		}
		if i == 0 {
			return report, errors.New("rawdb: posting prefix has no successor")
		}
	}
	for _, start := range [][]byte{nil, upper} {
		if e := ctx.Err(); e != nil {
			return report, e
		}
		e := func() error {
			it := db.NewIterator(nil, start)
			defer it.Release()
			for it.Next() {
				if e := ctx.Err(); e != nil {
					return e
				}
				key, value := it.Key(), it.Value()
				if start == nil && bytes.Compare(key, stateChangePostingPrefix) >= 0 {
					break
				}
				n := uint64(len(key)) + uint64(len(value))
				if report.Rows == math.MaxUint64 || report.LogicalBytes > math.MaxUint64-n {
					return errors.New("rawdb: posting complement counter overflow")
				}
				report.Rows++
				report.LogicalBytes += n
				censusHashKV(digest, key, value, &length)
			}
			return errors.Join(it.Error(), ctx.Err())
		}()
		if e != nil {
			return report, e
		}
	}
	report.SHA256 = hex.EncodeToString(digest.Sum(nil))
	return report, nil
}

type postingCensusAccumulator struct {
	stat     *PostingCensusBytes
	digest   hash.Hash
	lastHash [sha256.Size]byte
	hasHash  bool
	length   [8]byte
}

func newPostingCensusAccumulator(stat *PostingCensusBytes) postingCensusAccumulator {
	h := sha256.New()
	_, _ = h.Write([]byte("go-tron-posting-census-kv-v1\x00"))
	return postingCensusAccumulator{stat: stat, digest: h}
}

func censusHashKV(h hash.Hash, key, value []byte, length *[8]byte) {
	binary.BigEndian.PutUint64(length[:], uint64(len(key)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(key)
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(value)
}

func (a *postingCensusAccumulator) add(key, value []byte, refs uint64, keyHash [32]byte) error {
	k, v := uint64(len(key)), uint64(len(value))
	if a.stat.Frames == math.MaxUint64 || a.stat.BlockReferences > math.MaxUint64-refs ||
		a.stat.KeyBytes > math.MaxUint64-k || a.stat.ValueBytes > math.MaxUint64-v ||
		k > math.MaxUint64-v || a.stat.LogicalBytes > math.MaxUint64-k-v {
		return errors.New("rawdb: posting census counter overflow")
	}
	a.stat.Frames++
	a.stat.BlockReferences += refs
	a.stat.KeyBytes += k
	a.stat.ValueBytes += v
	a.stat.LogicalBytes += k + v
	a.stat.FrameRows[refs]++ // validated count is in [1,256].
	if !a.hasHash || a.lastHash != keyHash {
		a.stat.KeyHashes++
		a.lastHash, a.hasHash = keyHash, true
	}
	censusHashKV(a.digest, key, value, &a.length)
	return nil
}

// InspectStateChangePostings performs a complete read-only scan for at most
// eight caller-supplied cutoffs. For each H it applies exactly the online
// posting-only worker's whole-frame predicate (last block <= H). This does not
// authorize H: the caller must separately prove canonical cold-covered hot
// pruning. Directory rows are always retained. A coherent immutable DB image
// or externally owned snapshot is required; opening independent iterators on a
// concurrently mutating DB is not a point-in-time census.
//
// Every posting value is fully validated by the production bounds decoder.
// Digests cover length-framed original keys/values in physical key order,
// including partial-prefix digests on failure. Complete is true only after
// both iterators finish successfully. Progress is called once per 65536 rows;
// it must not mutate the source. Individual iterator IO is not cancellable.
func InspectStateChangePostings(ctx context.Context, db ethdb.Iteratee, cutoffs []uint64, progress func(phase string, rows, logicalBytes uint64)) (report PostingCensus, retErr error) {
	if ctx == nil || db == nil || len(cutoffs) == 0 || len(cutoffs) > 8 {
		return report, errors.New("rawdb: posting census requires context, reader and 1..8 cutoffs")
	}
	for i, h := range cutoffs {
		if h == 0 || i > 0 && h <= cutoffs[i-1] {
			return report, errors.New("rawdb: posting census cutoffs must be positive and strictly increasing")
		}
	}
	started := time.Now()
	report.StartedUTC = started.UTC()
	report.Cutoffs = make([]PostingCensusCutoff, len(cutoffs))
	all := newPostingCensusAccumulator(&report.All)
	groups := make([][3]postingCensusAccumulator, len(cutoffs))
	for i, h := range cutoffs {
		r := &report.Cutoffs[i]
		r.ThroughBlock = h
		groups[i] = [3]postingCensusAccumulator{newPostingCensusAccumulator(&r.Eligible), newPostingCensusAccumulator(&r.Crossing), newPostingCensusAccumulator(&r.Newer)}
	}
	directory := sha256.New()
	var directoryLength [8]byte
	_, _ = directory.Write([]byte("go-tron-posting-census-directory-v1\x00"))
	defer func() {
		report.ElapsedSeconds = time.Since(started).Seconds()
		report.All.SHA256 = hex.EncodeToString(all.digest.Sum(nil))
		for i := range groups {
			for j := range groups[i] {
				groups[i][j].stat.SHA256 = hex.EncodeToString(groups[i][j].digest.Sum(nil))
			}
		}
		report.DirectorySHA = hex.EncodeToString(directory.Sum(nil))
		if retErr != nil {
			report.Error = retErr.Error()
		}
	}()
	if err := ctx.Err(); err != nil {
		return report, err
	}
	err := func() error {
		it := db.NewIterator(stateChangePostingPrefix, nil)
		defer it.Release()
		var previous []byte
		for it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key, value := it.Key(), it.Value()
			if len(key) != len(stateChangePostingPrefix)+sha256.Size+8 || !bytes.HasPrefix(key, stateChangePostingPrefix) ||
				len(previous) > 0 && bytes.Compare(key, previous) <= 0 || len(value) > maxStateChangePostingEncodedBytes {
				return errors.New("rawdb: malformed or unordered census posting")
			}
			first := binary.BigEndian.Uint64(key[len(key)-8:])
			last, refs, err := stateChangePostingLastBlock(first, value)
			if err != nil {
				return err
			}
			var keyHash [32]byte
			copy(keyHash[:], key[len(stateChangePostingPrefix):])
			if err := all.add(key, value, refs, keyHash); err != nil {
				return err
			}
			for i, h := range cutoffs {
				class := 2
				if last <= h {
					class = 0
				} else if first <= h {
					class = 1
				}
				if err := groups[i][class].add(key, value, refs, keyHash); err != nil {
					return err
				}
			}
			previous = append(previous[:0], key...)
			if progress != nil && report.All.Frames%65536 == 0 {
				progress("posting", report.All.Frames, report.All.LogicalBytes)
			}
		}
		return errors.Join(it.Error(), ctx.Err())
	}()
	if err != nil {
		return report, err
	}
	if err := ScanStateChangeKeyDirectory(ctx, db, func(row StateChangeDirectoryCursor) error {
		n := uint64(len(row.Key))
		if report.DirectoryRows == math.MaxUint64 || report.DirectoryBytes > math.MaxUint64-n {
			return errors.New("rawdb: directory census counter overflow")
		}
		report.DirectoryRows++
		report.DirectoryBytes += n
		censusHashKV(directory, row.Key, nil, &directoryLength)
		if progress != nil && report.DirectoryRows%65536 == 0 {
			progress("directory", report.DirectoryRows, report.DirectoryBytes)
		}
		return nil
	}); err != nil {
		return report, fmt.Errorf("rawdb: posting census directory: %w", err)
	}
	report.Complete = true
	return report, nil
}
