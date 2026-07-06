package rawdb

import (
	"bytes"
	"errors"
	"strings"
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
	strict, ok, err := ReadDrAccountIndexEntryStrict(db, DrAccIdxV1From, from, to)
	if err != nil || !ok || strict == nil || !bytes.Equal(strict.Account, to) || strict.Timestamp != 12345 {
		t.Fatalf("strict from-anchored record wrong: %+v/%v/%v", strict, ok, err)
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

func TestDrAccountIndex_LegacyDelegateAndUnDelegate(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	from := addr(0xa1)
	to := addr(0xb2)

	if err := WriteDrAccountIndexLegacyDelegate(db, from, to); err != nil {
		t.Fatal(err)
	}
	if err := WriteDrAccountIndexLegacyDelegate(db, from, to); err != nil {
		t.Fatal(err)
	}
	fromRec := ReadDrAccountIndexLegacy(db, from)
	if fromRec == nil || !bytes.Equal(fromRec.Account, from) || len(fromRec.ToAccounts) != 1 || !bytes.Equal(fromRec.ToAccounts[0], to) {
		t.Fatalf("legacy from index wrong: %+v", fromRec)
	}
	strictFrom, ok, err := ReadDrAccountIndexLegacyStrict(db, from)
	if err != nil || !ok || strictFrom == nil || !bytes.Equal(strictFrom.Account, from) || len(strictFrom.ToAccounts) != 1 || !bytes.Equal(strictFrom.ToAccounts[0], to) {
		t.Fatalf("strict legacy from index wrong: %+v/%v/%v", strictFrom, ok, err)
	}
	toRec := ReadDrAccountIndexLegacy(db, to)
	if toRec == nil || !bytes.Equal(toRec.Account, to) || len(toRec.FromAccounts) != 1 || !bytes.Equal(toRec.FromAccounts[0], from) {
		t.Fatalf("legacy to index wrong: %+v", toRec)
	}

	if err := WriteDrAccountIndexLegacyUnDelegate(db, from, to); err != nil {
		t.Fatal(err)
	}
	fromRec = ReadDrAccountIndexLegacy(db, from)
	if fromRec == nil || len(fromRec.ToAccounts) != 0 {
		t.Fatalf("legacy from should keep empty aggregate record, got %+v", fromRec)
	}
	toRec = ReadDrAccountIndexLegacy(db, to)
	if toRec == nil || len(toRec.FromAccounts) != 0 {
		t.Fatalf("legacy to should keep empty aggregate record, got %+v", toRec)
	}
}

func TestDrAccountIndex_ConvertLegacyUsesListOrderAsTimestamp(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	from := addr(0xc1)
	to1 := addr(0xd1)
	to2 := addr(0xd2)
	if err := WriteDrAccountIndexLegacyDelegate(db, from, to1); err != nil {
		t.Fatal(err)
	}
	if err := WriteDrAccountIndexLegacyDelegate(db, from, to2); err != nil {
		t.Fatal(err)
	}
	if err := ConvertDrAccountIndexLegacy(db, from); err != nil {
		t.Fatal(err)
	}
	if ReadDrAccountIndexLegacy(db, from) != nil {
		t.Fatal("legacy aggregate should be deleted after convert")
	}
	rec1 := ReadDrAccountIndexEntry(db, DrAccIdxV1From, from, to1)
	rec2 := ReadDrAccountIndexEntry(db, DrAccIdxV1From, from, to2)
	if rec1 == nil || rec1.Timestamp != 1 || !bytes.Equal(rec1.Account, to1) {
		t.Fatalf("converted first entry wrong: %+v", rec1)
	}
	if rec2 == nil || rec2.Timestamp != 2 || !bytes.Equal(rec2.Account, to2) {
		t.Fatalf("converted second entry wrong: %+v", rec2)
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

func TestDrAccountIndexStrictAbsent(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	account := addr(0x10)
	if got, ok, err := ReadDrAccountIndexLegacyStrict(db, account); got != nil || ok || err != nil {
		t.Fatalf("legacy strict absent = %+v/%v/%v, want nil/false/nil", got, ok, err)
	}
	if got, ok, err := ReadDrAccountIndexEntryStrict(db, DrAccIdxV2From, account, addr(0x11)); got != nil || ok || err != nil {
		t.Fatalf("entry strict absent = %+v/%v/%v, want nil/false/nil", got, ok, err)
	}
}

func TestDrAccountIndexStrictSurfacesStorageErrors(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	from := addr(0x44)
	to := addr(0x45)
	if err := WriteDrAccountIndexLegacyDelegate(db, from, to); err != nil {
		t.Fatal(err)
	}
	if err := WriteDrAccountIndexDelegate(db, true, from, to, 99); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		read func(failingStateDomainReader) (*corepb.DelegatedResourceAccountIndex, bool, error)
	}{
		{
			name: "legacy",
			read: func(r failingStateDomainReader) (*corepb.DelegatedResourceAccountIndex, bool, error) {
				return ReadDrAccountIndexLegacyStrict(r, from)
			},
		},
		{
			name: "entry",
			read: func(r failingStateDomainReader) (*corepb.DelegatedResourceAccountIndex, bool, error) {
				return ReadDrAccountIndexEntryStrict(r, DrAccIdxV2From, from, to)
			},
		},
	} {
		t.Run(tc.name+"/has", func(t *testing.T) {
			got, ok, err := tc.read(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")})
			if err == nil || ok || got != nil || !strings.Contains(err.Error(), "presence") {
				t.Fatalf("has error = %+v/%v/%v, want presence error", got, ok, err)
			}
		})
		t.Run(tc.name+"/get", func(t *testing.T) {
			got, ok, err := tc.read(failingStateDomainReader{reader: db, getErr: errors.New("get boom")})
			if err == nil || ok || got != nil || !strings.Contains(err.Error(), "get boom") {
				t.Fatalf("get error = %+v/%v/%v, want get error", got, ok, err)
			}
		})
	}
}

func TestDrAccountIndexStrictSurfacesCorruptPayloads(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	from := addr(0x55)
	to := addr(0x56)

	if err := db.Put(drAccIdxLegacyKey(from), []byte{0x80}); err != nil {
		t.Fatal(err)
	}
	if got := ReadDrAccountIndexLegacy(db, from); got != nil {
		t.Fatalf("legacy compat corrupt index = %+v, want nil", got)
	}
	if got, ok, err := ReadDrAccountIndexLegacyStrict(db, from); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode dr account index legacy") {
		t.Fatalf("strict legacy corrupt = %+v/%v/%v, want decode error", got, ok, err)
	}
	if err := ConvertDrAccountIndexLegacy(db, from); err == nil || !strings.Contains(err.Error(), "decode dr account index legacy") {
		t.Fatalf("convert corrupt legacy error = %v, want decode error", err)
	}
	if err := WriteDrAccountIndexLegacyDelegate(db, from, to); err == nil || !strings.Contains(err.Error(), "decode dr account index legacy") {
		t.Fatalf("legacy delegate corrupt index error = %v, want decode error", err)
	}
	if err := WriteDrAccountIndexLegacyUnDelegate(db, from, to); err == nil || !strings.Contains(err.Error(), "decode dr account index legacy") {
		t.Fatalf("legacy undelegate corrupt index error = %v, want decode error", err)
	}

	if err := db.Delete(drAccIdxLegacyKey(from)); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(drAccIdxKey(DrAccIdxV2From, from, to), []byte{0x80}); err != nil {
		t.Fatal(err)
	}
	if got := ReadDrAccountIndexEntry(db, DrAccIdxV2From, from, to); got != nil {
		t.Fatalf("entry compat corrupt index = %+v, want nil", got)
	}
	if got, ok, err := ReadDrAccountIndexEntryStrict(db, DrAccIdxV2From, from, to); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode dr account index entry") {
		t.Fatalf("strict entry corrupt = %+v/%v/%v, want decode error", got, ok, err)
	}
}
