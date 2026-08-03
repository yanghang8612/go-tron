package freezer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// CompactTransactionIndexTail geometrically merges the final two immutable
// runs when they span the same number of blocks. Repeating this after each
// segment publication produces a binary-counter layout: logarithmic lookup
// fan-out without retaining full transaction hashes in the compact files.
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
	leftDecl := manifest.Runs[len(manifest.Runs)-2]
	rightDecl := manifest.Runs[len(manifest.Runs)-1]
	if leftDecl.EndBlock-leftDecl.StartBlock != rightDecl.EndBlock-rightDecl.StartBlock {
		return zero, nil, false, nil
	}
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
	if left.EndBlock() != right.StartBlock() || left.PrefixBits() != right.PrefixBits() {
		return zero, nil, false, nil
	}

	mergedPath := TransactionIndexRunPath(ancientDir, left.StartBlock(), right.EndBlock())
	result, err := buildOrRecoverMergedTransactionIndex(mergedPath, left, right)
	if err != nil {
		return zero, nil, false, err
	}
	if err := publishTransactionIndexTailMerge(ancientDir, result, leftDecl, rightDecl); err != nil {
		return zero, nil, false, err
	}
	return result, []string{leftPath, rightPath}, true, nil
}

func buildOrRecoverMergedTransactionIndex(path string, left, right *TransactionIndexRun) (TransactionIndexBuildResult, error) {
	if run, err := OpenTransactionIndexRun(path); err == nil {
		defer run.Close()
		if run.StartBlock() != left.StartBlock() || run.EndBlock() != right.EndBlock() ||
			run.PrefixBits() != left.PrefixBits() || run.Rows() != left.Rows()+right.Rows() {
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
		PrefixBits: left.PrefixBits(),
		StartBlock: left.StartBlock(),
		EndBlock:   right.EndBlock(),
		Iterate:    mergeTransactionIndexIterator(left, right),
	})
}

func mergeTransactionIndexIterator(left, right *TransactionIndexRun) TransactionIndexIterator {
	return func(yield func(TransactionIndexEntry) error) error {
		bucketCount := uint32(1) << left.PrefixBits()
		for prefix := uint32(0); prefix < bucketCount; prefix++ {
			leftFingerprints, leftLocations, err := left.readBucket(prefix)
			if err != nil {
				return err
			}
			rightFingerprints, rightLocations, err := right.readBucket(prefix)
			if err != nil {
				return err
			}
			leftRows := len(leftFingerprints) / 8
			rightRows := len(rightFingerprints) / 8
			li, ri := 0, 0
			var previousFingerprint uint64
			var tie uint64
			haveFingerprint := false
			for li < leftRows || ri < rightRows {
				useLeft := ri == rightRows
				if li < leftRows && ri < rightRows {
					lf := binary.BigEndian.Uint64(leftFingerprints[li*8 : (li+1)*8])
					rf := binary.BigEndian.Uint64(rightFingerprints[ri*8 : (ri+1)*8])
					useLeft = lf <= rf
				}
				var fingerprint, location uint64
				if useLeft {
					fingerprint = binary.BigEndian.Uint64(leftFingerprints[li*8 : (li+1)*8])
					location = binary.BigEndian.Uint64(leftLocations[li*8 : (li+1)*8])
					li++
				} else {
					fingerprint = binary.BigEndian.Uint64(rightFingerprints[ri*8 : (ri+1)*8])
					location = binary.BigEndian.Uint64(rightLocations[ri*8 : (ri+1)*8])
					ri++
				}
				if !haveFingerprint || fingerprint != previousFingerprint {
					previousFingerprint = fingerprint
					tie = 0
					haveFingerprint = true
				} else {
					tie++
				}
				hash := syntheticTransactionIndexHash(prefix, fingerprint, tie, left.PrefixBits())
				if err := yield(TransactionIndexEntry{Hash: hash, Location: location}); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// syntheticTransactionIndexHash reconstructs the routed prefix/fingerprint
// bits consumed by BuildTransactionIndexRun. Remaining bits carry a monotonic
// tie breaker so distinct candidates with the same 64-bit fingerprint remain
// strictly ordered without pretending that the discarded full hash is known.
func syntheticTransactionIndexHash(prefix uint32, fingerprint, tie uint64, prefixBits uint32) [32]byte {
	var hash [32]byte
	high := uint64(prefix)<<(64-prefixBits) | fingerprint>>prefixBits
	next := fingerprint << (64 - prefixBits)
	binary.BigEndian.PutUint64(hash[0:8], high)
	binary.BigEndian.PutUint64(hash[8:16], next)
	binary.BigEndian.PutUint64(hash[16:24], tie)
	return hash
}

func publishTransactionIndexTailMerge(ancientDir string, result TransactionIndexBuildResult, left, right transactionIndexManifestRun) error {
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
	if len(manifest.Runs) < 2 || manifest.Runs[len(manifest.Runs)-2] != left || manifest.Runs[len(manifest.Runs)-1] != right {
		return errors.New("transaction index merge: manifest tail changed")
	}
	if result.StartBlock != left.StartBlock || result.EndBlock != right.EndBlock || result.Rows != left.Rows+right.Rows {
		return errors.New("transaction index merge: build result does not cover manifest tail")
	}
	manifest.Runs = append(manifest.Runs[:len(manifest.Runs)-2], transactionIndexManifestRun{
		File:       filepath.Base(result.Path),
		StartBlock: result.StartBlock,
		EndBlock:   result.EndBlock,
		Rows:       result.Rows,
	})
	return writeTransactionIndexManifest(base, manifest)
}
