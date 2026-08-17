package freezer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// CompactTransactionIndexTail geometrically merges the rightmost equal-sized
// adjacent pair. Normally that is the manifest tail and produces a binary
// counter layout. Searching backward is important after maintenance was
// deferred: a tail-only compactor gets permanently stuck on [1,1,2], while
// merging the earlier pair repairs it to [2,2] and then [4].
//
// The merge is possible because a run is ordered by routed 64-bit fingerprint.
// The canonical block lookup still verifies every returned candidate against
// the complete transaction hash, so merging fingerprint/location pairs does
// not weaken correctness.
func CompactTransactionIndexTail(ancientDir string) (TransactionIndexBuildResult, []string, bool, error) {
	var zero TransactionIndexBuildResult
	base := filepath.Join(ancientDir, transactionIndexDirectoryName)
	manifest, err := readTransactionIndexManifest(base)
	if errors.Is(err, os.ErrNotExist) || len(manifest.Runs) < 2 {
		return zero, nil, false, nil
	}
	if err != nil {
		return zero, nil, false, err
	}
	if err := validateTransactionIndexManifest(manifest); err != nil {
		return zero, nil, false, err
	}
	mergeAt := -1
	for i := len(manifest.Runs) - 2; i >= 0; i-- {
		left, right := manifest.Runs[i], manifest.Runs[i+1]
		if left.EndBlock-left.StartBlock == right.EndBlock-right.StartBlock {
			mergeAt = i
			break
		}
	}
	if mergeAt < 0 {
		return zero, nil, false, nil
	}
	leftDecl := manifest.Runs[mergeAt]
	rightDecl := manifest.Runs[mergeAt+1]
	runsDir := filepath.Join(base, transactionIndexRunsDirectory)
	leftPath := filepath.Join(runsDir, leftDecl.File)
	rightPath := filepath.Join(runsDir, rightDecl.File)
	left, err := OpenTransactionIndexRun(leftPath)
	if err != nil {
		return zero, nil, false, fmt.Errorf("open left transaction index run: %w", err)
	}
	defer left.Close()
	right, err := OpenTransactionIndexRun(rightPath)
	if err != nil {
		return zero, nil, false, fmt.Errorf("open right transaction index run: %w", err)
	}
	defer right.Close()
	if left.EndBlock() != right.StartBlock() {
		return zero, nil, false, nil
	}

	mergedPath := TransactionIndexRunPath(ancientDir, left.StartBlock(), right.EndBlock())
	result, err := buildOrRecoverMergedTransactionIndex(mergedPath, left, right)
	if err != nil {
		return zero, nil, false, err
	}
	if err := publishTransactionIndexMerge(ancientDir, result, mergeAt, leftDecl, rightDecl); err != nil {
		return zero, nil, false, err
	}
	return result, []string{leftPath, rightPath}, true, nil
}

func buildOrRecoverMergedTransactionIndex(path string, left, right *TransactionIndexRun) (TransactionIndexBuildResult, error) {
	prefixBits := min(left.PrefixBits(), right.PrefixBits())
	if run, err := OpenTransactionIndexRun(path); err == nil {
		defer run.Close()
		if run.StartBlock() != left.StartBlock() || run.EndBlock() != right.EndBlock() ||
			run.PrefixBits() != prefixBits || run.Rows() != left.Rows()+right.Rows() {
			return TransactionIndexBuildResult{}, fmt.Errorf("transaction index merge: existing run %q has incompatible metadata", path)
		}
		if err := run.Verify(); err != nil {
			return TransactionIndexBuildResult{}, fmt.Errorf("transaction index merge: verify existing run: %w", err)
		}
		return TransactionIndexBuildResult{
			Path:       path,
			Rows:       run.Rows(),
			StartBlock: run.StartBlock(),
			EndBlock:   run.EndBlock(),
			PrefixBits: run.PrefixBits(),
			FileBytes:  run.Size(),
		}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return TransactionIndexBuildResult{}, err
	}
	return BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
		PrefixBits: prefixBits,
		StartBlock: left.StartBlock(),
		EndBlock:   right.EndBlock(),
		Iterate:    mergeTransactionIndexIterator(left, right),
	})
}

type transactionIndexMergeEntry struct {
	high, next uint64
	location   uint64
}

type transactionIndexMergeCursor struct {
	run                        *TransactionIndexRun
	prefix, currentPrefix, row uint32
	rows                       uint32
	fingerprints, locations    []byte
}

func (c *transactionIndexMergeCursor) next() (transactionIndexMergeEntry, bool, error) {
	for c.row >= c.rows {
		if uint64(c.prefix) >= uint64(1)<<c.run.PrefixBits() {
			return transactionIndexMergeEntry{}, false, nil
		}
		fingerprints, locations, err := c.run.readBucket(c.prefix)
		if err != nil {
			return transactionIndexMergeEntry{}, false, err
		}
		c.currentPrefix = c.prefix
		c.prefix++
		c.fingerprints, c.locations = fingerprints, locations
		c.row, c.rows = 0, uint32(len(fingerprints)/8)
	}
	i := int(c.row) * 8
	fingerprint := binary.BigEndian.Uint64(c.fingerprints[i : i+8])
	location := binary.BigEndian.Uint64(c.locations[i : i+8])
	c.row++
	high, next := transactionIndexRoute(c.currentPrefix, fingerprint, c.run.PrefixBits())
	return transactionIndexMergeEntry{high: high, next: next, location: location}, true, nil
}

func mergeTransactionIndexIterator(left, right *TransactionIndexRun) TransactionIndexIterator {
	return func(yield func(TransactionIndexEntry) error) error {
		leftCursor := transactionIndexMergeCursor{run: left}
		rightCursor := transactionIndexMergeCursor{run: right}
		leftEntry, haveLeft, err := leftCursor.next()
		if err != nil {
			return err
		}
		rightEntry, haveRight, err := rightCursor.next()
		if err != nil {
			return err
		}
		var previousHigh, previousNext, tie uint64
		havePrevious := false
		for haveLeft || haveRight {
			useLeft := !haveRight || haveLeft && (leftEntry.high < rightEntry.high || leftEntry.high == rightEntry.high && leftEntry.next <= rightEntry.next)
			entry := rightEntry
			if useLeft {
				entry = leftEntry
				leftEntry, haveLeft, err = leftCursor.next()
			} else {
				rightEntry, haveRight, err = rightCursor.next()
			}
			if err != nil {
				return err
			}
			if !havePrevious || entry.high != previousHigh || entry.next != previousNext {
				previousHigh, previousNext, tie, havePrevious = entry.high, entry.next, 0, true
			} else {
				tie++
			}
			hash := syntheticTransactionIndexRouteHash(entry.high, entry.next, tie)
			if err := yield(TransactionIndexEntry{Hash: hash, Location: entry.location}); err != nil {
				return err
			}
		}
		return nil
	}
}

func transactionIndexRoute(prefix uint32, fingerprint uint64, prefixBits uint32) (uint64, uint64) {
	high := uint64(prefix)<<(64-prefixBits) | fingerprint>>prefixBits
	next := fingerprint << (64 - prefixBits)
	return high, next
}

func syntheticTransactionIndexRouteHash(high, next, tie uint64) [32]byte {
	var hash [32]byte
	binary.BigEndian.PutUint64(hash[0:8], high)
	binary.BigEndian.PutUint64(hash[8:16], next)
	binary.BigEndian.PutUint64(hash[16:24], tie)
	return hash
}

// syntheticTransactionIndexHash reconstructs the routed prefix/fingerprint
// bits consumed by BuildTransactionIndexRun. Remaining bits carry a monotonic
// tie breaker so distinct candidates with the same 64-bit fingerprint remain
// strictly ordered without pretending that the discarded full hash is known.
func syntheticTransactionIndexHash(prefix uint32, fingerprint, tie uint64, prefixBits uint32) [32]byte {
	high, next := transactionIndexRoute(prefix, fingerprint, prefixBits)
	return syntheticTransactionIndexRouteHash(high, next, tie)
}

func publishTransactionIndexMerge(ancientDir string, result TransactionIndexBuildResult, mergeAt int, left, right transactionIndexManifestRun) error {
	base := filepath.Join(ancientDir, transactionIndexDirectoryName)
	if err := verifyTransactionIndexBuildResult(ancientDir, result); err != nil {
		return err
	}
	manifest, err := readTransactionIndexManifest(base)
	if err != nil {
		return err
	}
	if err := validateTransactionIndexManifest(manifest); err != nil {
		return err
	}
	if mergeAt < 0 || mergeAt+1 >= len(manifest.Runs) || manifest.Runs[mergeAt] != left || manifest.Runs[mergeAt+1] != right {
		return errors.New("transaction index merge: manifest pair changed")
	}
	if result.StartBlock != left.StartBlock || result.EndBlock != right.EndBlock || result.Rows != left.Rows+right.Rows {
		return errors.New("transaction index merge: build result does not cover manifest tail")
	}
	merged := transactionIndexManifestRun{
		File:       filepath.Base(result.Path),
		StartBlock: result.StartBlock,
		EndBlock:   result.EndBlock,
		Rows:       result.Rows,
	}
	runs := make([]transactionIndexManifestRun, 0, len(manifest.Runs)-1)
	runs = append(runs, manifest.Runs[:mergeAt]...)
	runs = append(runs, merged)
	runs = append(runs, manifest.Runs[mergeAt+2:]...)
	manifest.Runs = runs
	return writeTransactionIndexManifest(base, manifest)
}
