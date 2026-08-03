package rawdb

import (
	"encoding/binary"
	"testing"
)

func TestSampleTransactionIndexesAcrossHashWindows(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	for prefix := 0; prefix < 256; prefix++ {
		for ordinal := 0; ordinal < 4; ordinal++ {
			var hash [32]byte
			hash[0] = byte(prefix)
			hash[2] = byte(ordinal)
			var value [8]byte
			binary.BigEndian.PutUint64(value[:], uint64(prefix*4+ordinal))
			if err := db.Put(txKey(hash[:]), value[:]); err != nil {
				t.Fatal(err)
			}
		}
	}
	samples, err := SampleTransactionIndexes(db, TransactionIndexSampleOptions{Rows: 128, Windows: 16})
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 128 {
		t.Fatalf("samples = %d, want 128", len(samples))
	}
	seenHighBytes := make(map[byte]struct{})
	for i, sample := range samples {
		seenHighBytes[sample.Hash[0]] = struct{}{}
		if i > 0 {
			previous := samples[i-1].Hash
			if string(previous[:]) >= string(sample.Hash[:]) {
				t.Fatalf("samples not ordered at %d", i)
			}
		}
	}
	if len(seenHighBytes) != 32 {
		t.Fatalf("sampled high-byte ranges = %d, want 32", len(seenHighBytes))
	}
}

func TestSampleTransactionIndexesRejectsMalformedValue(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	var hash [32]byte
	if err := db.Put(txKey(hash[:]), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if _, err := SampleTransactionIndexes(db, TransactionIndexSampleOptions{Rows: 1, Windows: 1}); err == nil {
		t.Fatal("malformed value accepted")
	}
}

func TestVisitTransactionIndexesByBlockRange(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	locations := []uint64{
		3,
		transactionLocationMarker | 7<<transactionLocationOrdinalBits | 2,
		transactionLocationMarker | 12<<transactionLocationOrdinalBits,
	}
	for i, location := range locations {
		var hash [32]byte
		hash[0] = byte(i + 1)
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], location)
		if err := db.Put(txKey(hash[:]), value[:]); err != nil {
			t.Fatal(err)
		}
	}
	var got []TransactionIndexSample
	scanned, selected, err := VisitTransactionIndexesByBlockRange(db, 4, 10, func(sample TransactionIndexSample) error {
		got = append(got, sample)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned != 3 || selected != 1 || len(got) != 1 || got[0].Location != locations[1] {
		t.Fatalf("scanned=%d selected=%d got=%+v", scanned, selected, got)
	}
}
