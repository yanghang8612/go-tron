package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func addr(v byte) []byte {
	out := make([]byte, 21)
	out[0] = 0x41
	for i := 1; i < 21; i++ {
		out[i] = v
	}
	return out
}

func TestDrAccountIndex_V1DelegateRoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	from := addr(0xaa)
	to := addr(0xbb)

	if err := WriteDrAccountIndexDelegate(db, false, from, to, 12345); err != nil {
		t.Fatal(err)
	}

	// from-anchored: account = to
	rec := ReadDrAccountIndexEntry(db, DrAccIdxV1From, from, to)
	if rec == nil {
		t.Fatal("from-anchored record missing")
	}
	if !bytes.Equal(rec.Account, to) {
		t.Fatalf("from-anchored account: got %x, want %x", rec.Account, to)
	}
	if rec.Timestamp != 12345 {
		t.Fatalf("from-anchored ts: got %d, want 12345", rec.Timestamp)
	}

	// to-anchored: account = from
	rec = ReadDrAccountIndexEntry(db, DrAccIdxV1To, to, from)
	if rec == nil || !bytes.Equal(rec.Account, from) || rec.Timestamp != 12345 {
		t.Fatalf("to-anchored record wrong: %+v", rec)
	}
}

func TestDrAccountIndex_V2IsDisjointFromV1(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	from := addr(0xcc)
	to := addr(0xdd)

	_ = WriteDrAccountIndexDelegate(db, true /*v2*/, from, to, 111)
	if rec := ReadDrAccountIndexEntry(db, DrAccIdxV1From, from, to); rec != nil {
		t.Fatal("V1 should be empty, V2 write leaked into V1 key")
	}
	if rec := ReadDrAccountIndexEntry(db, DrAccIdxV2From, from, to); rec == nil {
		t.Fatal("V2 from-anchored missing")
	}
	if rec := ReadDrAccountIndexEntry(db, DrAccIdxV2To, to, from); rec == nil {
		t.Fatal("V2 to-anchored missing")
	}
}

func TestDrAccountIndex_UnDelegate(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	from := addr(0xee)
	to := addr(0xff)

	_ = WriteDrAccountIndexDelegate(db, false, from, to, 1)
	if err := WriteDrAccountIndexUnDelegate(db, false, from, to); err != nil {
		t.Fatal(err)
	}
	if ReadDrAccountIndexEntry(db, DrAccIdxV1From, from, to) != nil {
		t.Fatal("from-anchored should be deleted")
	}
	if ReadDrAccountIndexEntry(db, DrAccIdxV1To, to, from) != nil {
		t.Fatal("to-anchored should be deleted")
	}
}

func TestDrAccountIndex_Iterate(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	receiver := addr(0xaa)
	sender1 := addr(0x11)
	sender2 := addr(0x22)
	sender3 := addr(0x33)

	_ = WriteDrAccountIndexDelegate(db, true, sender1, receiver, 100)
	_ = WriteDrAccountIndexDelegate(db, true, sender2, receiver, 200)
	_ = WriteDrAccountIndexDelegate(db, true, sender3, receiver, 300)
	// Noise: a V1 delegation to same receiver — must not be iterated.
	_ = WriteDrAccountIndexDelegate(db, false, sender1, receiver, 999)

	collected := map[byte]int64{}
	err := IterateDrAccountIndex(db, DrAccIdxV2To, receiver, func(counterparty []byte, rec *corepb.DelegatedResourceAccountIndex) error {
		collected[counterparty[1]] = rec.Timestamp
		if !bytes.Equal(rec.Account, counterparty) {
			t.Fatalf("proto account != counterparty: %x vs %x", rec.Account, counterparty)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(collected) != 3 {
		t.Fatalf("want 3 senders in V2, got %d: %+v", len(collected), collected)
	}
	if collected[0x11] != 100 || collected[0x22] != 200 || collected[0x33] != 300 {
		t.Fatalf("timestamps wrong: %+v", collected)
	}
}

func TestDrAccountIndex_RejectsEmpty(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := WriteDrAccountIndexDelegate(db, false, nil, addr(0x11), 1); err == nil {
		t.Fatal("expected empty-from error")
	}
	if err := WriteDrAccountIndexDelegate(db, false, addr(0x11), nil, 1); err == nil {
		t.Fatal("expected empty-to error")
	}
}
