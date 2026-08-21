package rawdb

import (
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestIterateLatestDomainCommitmentSourcesReportsStableSourceOrder(t *testing.T) {
	db := NewMemoryDatabase()
	if err := WriteStateAccountLatest(db, common.Address{0x41, 0x01}, []byte("account")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateKVGeneration(db, common.Address{0x41, 0x02}, 7); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(append(append([]byte(nil), stateKVLatestPrefix...), 0x01), []byte("kv")); err != nil {
		t.Fatal(err)
	}

	var got []LatestDomainCommitmentSource
	if err := IterateLatestDomainCommitmentSourcesWithSource(db, func(source LatestDomainCommitmentSource, _, _ []byte) (bool, error) {
		got = append(got, source)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []LatestDomainCommitmentSource{
		LatestDomainCommitmentSourceAccounts,
		LatestDomainCommitmentSourceKVGeneration,
		LatestDomainCommitmentSourceKVLatest,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sources = %v, want %v", got, want)
	}
}
