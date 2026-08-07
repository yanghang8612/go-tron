package freezer

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildTransactionIndexRunHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "canceled.gtxi")
	_, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
		Context:    ctx,
		PrefixBits: 8,
		StartBlock: 1,
		EndBlock:   2,
		Iterate:    transactionIndexTestIterator(nil),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled build error = %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("canceled build published destination: %v", err)
	}
}

func transactionIndexTestIterator(entries []TransactionIndexEntry) TransactionIndexIterator {
	return func(yield func(TransactionIndexEntry) error) error {
		for _, entry := range entries {
			if err := yield(entry); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestTransactionIndexRunRoundTrip(t *testing.T) {
	entries := make([]TransactionIndexEntry, 4096)
	for i := range entries {
		binary.BigEndian.PutUint64(entries[i].Hash[:8], uint64(i)<<40)
		binary.BigEndian.PutUint64(entries[i].Hash[8:16], uint64(i)*0x9e3779b97f4a7c15)
		entries[i].Location = transactionLocationMarker | uint64(i%65_536)<<transactionLocationOrdinalBits | uint64(i%100)
	}
	path := filepath.Join(t.TempDir(), "run.gtxi")
	result, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
		PrefixBits: 12,
		StartBlock: 0,
		EndBlock:   65_536,
		Iterate:    transactionIndexTestIterator(entries),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != uint64(len(entries)) || result.StartBlock != 0 || result.EndBlock != 65_536 || result.FileBytes == 0 || result.MaxBucketRows == 0 {
		t.Fatalf("result = %+v", result)
	}
	run, err := OpenTransactionIndexRun(path)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	if run.Rows() != uint64(len(entries)) || run.StartBlock() != 0 || run.EndBlock() != 65_536 || run.PrefixBits() != 12 || run.Size() != result.FileBytes {
		t.Fatalf("run metadata rows=%d range=[%d,%d) prefix=%d size=%d", run.Rows(), run.StartBlock(), run.EndBlock(), run.PrefixBits(), run.Size())
	}
	if err := run.Verify(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run.VerifyContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("VerifyContext canceled error = %v", err)
	}
	for i := 0; i < len(entries); i += 31 {
		locations, err := run.Candidates(entries[i].Hash)
		if err != nil {
			t.Fatal(err)
		}
		if len(locations) != 1 || locations[0] != entries[i].Location {
			t.Fatalf("entry %d locations = %v", i, locations)
		}
	}
	missing := entries[100].Hash
	missing[9] ^= 0x80
	locations, err := run.Candidates(missing)
	if err != nil {
		t.Fatal(err)
	}
	if len(locations) != 0 {
		t.Fatalf("missing locations = %v", locations)
	}
}

func TestTransactionIndexRunReturnsFingerprintCollisions(t *testing.T) {
	var first, second TransactionIndexEntry
	copy(first.Hash[:], []byte{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xd0})
	second.Hash = first.Hash
	second.Hash[31] = 1 // outside prefix + 64-bit fingerprint
	first.Location = transactionLocationMarker | 11<<transactionLocationOrdinalBits
	second.Location = transactionLocationMarker | 22<<transactionLocationOrdinalBits
	path := filepath.Join(t.TempDir(), "collision.gtxi")
	if _, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
		PrefixBits: 20,
		StartBlock: 0,
		EndBlock:   100,
		Iterate:    transactionIndexTestIterator([]TransactionIndexEntry{first, second}),
	}); err != nil {
		t.Fatal(err)
	}
	run, err := OpenTransactionIndexRun(path)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	locations, err := run.Candidates(first.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(locations) != 2 || locations[0] != first.Location || locations[1] != second.Location {
		t.Fatalf("collision locations = %v", locations)
	}
}

func TestTransactionIndexRunDetectsBucketCorruption(t *testing.T) {
	var entry TransactionIndexEntry
	entry.Hash[0] = 0x40
	entry.Location = transactionLocationMarker | 99<<transactionLocationOrdinalBits
	path := filepath.Join(t.TempDir(), "corrupt.gtxi")
	if _, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
		PrefixBits: 8,
		StartBlock: 0,
		EndBlock:   100,
		Iterate:    transactionIndexTestIterator([]TransactionIndexEntry{entry}),
	}); err != nil {
		t.Fatal(err)
	}
	run, err := OpenTransactionIndexRun(path)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := transactionIndexPrefixFingerprint(entry.Hash, run.prefixBits)
	offset := run.directory[prefix] + transactionIndexBucketHeaderSize
	if err := run.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, int64(offset)); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	run, err = OpenTransactionIndexRun(path)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	if _, err := run.Candidates(entry.Hash); err == nil {
		t.Fatal("corrupt fingerprint bucket accepted")
	}
}

func TestBuildTransactionIndexRunRejectsUnsortedEntries(t *testing.T) {
	var first, second TransactionIndexEntry
	first.Hash[0] = 2
	second.Hash[0] = 1
	path := filepath.Join(t.TempDir(), "unsorted.gtxi")
	if _, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
		PrefixBits: 8,
		StartBlock: 0,
		EndBlock:   1,
		Iterate:    transactionIndexTestIterator([]TransactionIndexEntry{first, second}),
	}); err == nil {
		t.Fatal("unsorted entries accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("destination exists after rejected build: %v", err)
	}
}

func TestBuildTransactionIndexRunConsumesIteratorOnce(t *testing.T) {
	var entry TransactionIndexEntry
	entry.Hash[0] = 1
	path := filepath.Join(t.TempDir(), "single-pass.gtxi")
	calls := 0
	result, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
		PrefixBits: 8,
		StartBlock: 0,
		EndBlock:   1,
		Iterate: func(yield func(TransactionIndexEntry) error) error {
			calls++
			return yield(entry)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || result.Rows != 1 {
		t.Fatalf("iterator calls=%d result=%+v", calls, result)
	}
}
