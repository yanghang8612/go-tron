package freezer

import "testing"

func TestTransactionIndexStorePublishAndOpen(t *testing.T) {
	ancientDir := t.TempDir()
	var first, second TransactionIndexEntry
	first.Hash[0] = 0x10
	first.Location = transactionLocationMarker | 11<<transactionLocationOrdinalBits
	second.Hash[0] = 0x90
	second.Location = transactionLocationMarker | 22<<transactionLocationOrdinalBits
	path := TransactionIndexRunPath(ancientDir, 0, 100)
	result, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
		PrefixBits: 8,
		StartBlock: 0,
		EndBlock:   100,
		Iterate:    transactionIndexTestIterator([]TransactionIndexEntry{first, second}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := PublishTransactionIndexRun(ancientDir, result); err != nil {
		t.Fatal(err)
	}
	store, err := OpenTransactionIndexStore(ancientDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.Coverage() != 100 || !store.CoversBlock(99) || store.CoversBlock(100) {
		t.Fatalf("coverage = %d", store.Coverage())
	}
	locations, err := store.Candidates(second.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(locations) != 1 || locations[0] != second.Location {
		t.Fatalf("locations = %v", locations)
	}
}

func TestTransactionIndexStoreIgnoresUnpublishedRun(t *testing.T) {
	ancientDir := t.TempDir()
	var entry TransactionIndexEntry
	entry.Hash[0] = 1
	path := TransactionIndexRunPath(ancientDir, 0, 10)
	if _, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
		PrefixBits: 8,
		StartBlock: 0,
		EndBlock:   10,
		Iterate:    transactionIndexTestIterator([]TransactionIndexEntry{entry}),
	}); err != nil {
		t.Fatal(err)
	}
	store, err := OpenTransactionIndexStore(ancientDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.Coverage() != 0 {
		t.Fatalf("unpublished coverage = %d", store.Coverage())
	}
	locations, err := store.Candidates(entry.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(locations) != 0 {
		t.Fatalf("unpublished run returned %v", locations)
	}
}

func TestPublishTransactionIndexRunRejectsGap(t *testing.T) {
	ancientDir := t.TempDir()
	path := TransactionIndexRunPath(ancientDir, 10, 20)
	result, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
		PrefixBits: 8,
		StartBlock: 10,
		EndBlock:   20,
		Iterate:    transactionIndexTestIterator(nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := PublishTransactionIndexRun(ancientDir, result); err == nil {
		t.Fatal("manifest gap accepted")
	}
}
