package state

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func exCreator(tag byte) []byte {
	raw := make([]byte, 21)
	raw[0] = 0x41
	raw[20] = tag
	return raw
}

func sameExchange(a, b *corepb.Exchange) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.ExchangeId == b.ExchangeId &&
		string(a.FirstTokenId) == string(b.FirstTokenId) &&
		a.FirstTokenBalance == b.FirstTokenBalance &&
		string(a.SecondTokenId) == string(b.SecondTokenId) &&
		a.SecondTokenBalance == b.SecondTokenBalance &&
		string(a.CreatorAddress) == string(b.CreatorAddress)
}

// V1 and V2 are independent buckets within the one domain: a write to one leg is
// invisible to the other, and deleting one leaves the other intact. Replaces the
// dual-bucket coverage from the deleted rawdb accessors_exchange_test.go.
func TestExchangeStoreV1V2SeparateBuckets(t *testing.T) {
	sdb := newTestStateDB(t)
	legacy := &corepb.Exchange{
		ExchangeId:         1,
		FirstTokenId:       []byte("TOKEN"),
		FirstTokenBalance:  100,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 200,
	}
	v2 := &corepb.Exchange{
		ExchangeId:         1,
		FirstTokenId:       []byte("1000001"),
		FirstTokenBalance:  100,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 200,
	}
	if err := sdb.WriteExchange(legacy); err != nil {
		t.Fatalf("WriteExchange: %v", err)
	}
	if err := sdb.WriteExchangeV2(v2); err != nil {
		t.Fatalf("WriteExchangeV2: %v", err)
	}

	if got := sdb.ReadExchange(1); !sameExchange(got, legacy) {
		t.Fatalf("V1 readback mismatch: %+v", got)
	}
	if got := sdb.ReadExchangeV2(1); !sameExchange(got, v2) {
		t.Fatalf("V2 readback mismatch: %+v", got)
	}

	// Deleting V2 must not touch V1.
	if err := sdb.DeleteExchangeV2(1); err != nil {
		t.Fatalf("DeleteExchangeV2: %v", err)
	}
	if sdb.ReadExchangeV2(1) != nil {
		t.Fatal("expected nil after V2 delete")
	}
	if sdb.ReadExchange(1) == nil {
		t.Fatal("V1 bucket should remain after V2 delete")
	}
}

// A missing id reads back nil under either bucket.
func TestExchangeStoreAbsent(t *testing.T) {
	sdb := newTestStateDB(t)
	if got := sdb.ReadExchange(7); got != nil {
		t.Fatalf("absent V1 should be nil, got %+v", got)
	}
	if got := sdb.ReadExchangeV2(7); got != nil {
		t.Fatalf("absent V2 should be nil, got %+v", got)
	}
}

// ListExchanges walks ids 1..latest, returns only stored ids in order, and keeps
// the V1/V2 buckets distinct. latest acts as the enumeration ceiling, mirroring
// java-tron's getLatestExchangeNum-driven scan.
func TestExchangeStoreList(t *testing.T) {
	sdb := newTestStateDB(t)
	// Seed V1 ids {1,3} and V2 ids {1,2}; id 2 has no V1 record (gap).
	if err := sdb.WriteExchange(&corepb.Exchange{ExchangeId: 1, FirstTokenId: []byte("A")}); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteExchange(&corepb.Exchange{ExchangeId: 3, FirstTokenId: []byte("C")}); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteExchangeV2(&corepb.Exchange{ExchangeId: 1, FirstTokenId: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteExchangeV2(&corepb.Exchange{ExchangeId: 2, FirstTokenId: []byte("2")}); err != nil {
		t.Fatal(err)
	}

	v1 := sdb.ListExchanges(3)
	if len(v1) != 2 || v1[0].ExchangeId != 1 || v1[1].ExchangeId != 3 {
		t.Fatalf("V1 list: want ids [1 3], got %+v", v1)
	}
	v2 := sdb.ListExchangesV2(3)
	if len(v2) != 2 || v2[0].ExchangeId != 1 || v2[1].ExchangeId != 2 {
		t.Fatalf("V2 list: want ids [1 2], got %+v", v2)
	}
	// A ceiling below a stored id excludes it.
	if got := sdb.ListExchanges(2); len(got) != 1 || got[0].ExchangeId != 1 {
		t.Fatalf("V1 list ceiling=2: want [1], got %+v", got)
	}
	// Empty ceiling → nil.
	if got := sdb.ListExchanges(0); got != nil {
		t.Fatalf("V1 list ceiling=0: want nil, got %+v", got)
	}
}

// TestExchangeStoreAnchorAndRewind is the Phase 3d state-layer gate: rooting an
// exchange change moves the state root (anchor), and reopening an old root
// recovers the old exchange set under BOTH V1 and V2 (rewind). Mirrors
// applyBlock's per-block parent-root open with a fresh StateDB per commit.
func TestExchangeStoreAnchorAndRewind(t *testing.T) {
	sdb := newTestStateDB(t)

	// R1: pre-fork dual-write of exchange 1 — V1 (token names) + V2 (numeric ids).
	v1r1 := &corepb.Exchange{
		ExchangeId:         1,
		CreatorAddress:     exCreator(1),
		FirstTokenId:       []byte("TOKEN"),
		FirstTokenBalance:  1_000,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 1_000_000,
	}
	v2r1 := &corepb.Exchange{
		ExchangeId:         1,
		CreatorAddress:     exCreator(1),
		FirstTokenId:       []byte("1000001"),
		FirstTokenBalance:  1_000,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 1_000_000,
	}
	if err := sdb.WriteExchange(v1r1); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteExchangeV2(v2r1); err != nil {
		t.Fatal(err)
	}
	r1, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit R1: %v", err)
	}

	// R2 built on R1 via a fresh StateDB: post-fork, exchange 1's V2 balances move
	// (a transaction) and a new exchange 2 is created in V2 only.
	sdb2, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	v2r2 := &corepb.Exchange{
		ExchangeId:         1,
		CreatorAddress:     exCreator(1),
		FirstTokenId:       []byte("1000001"),
		FirstTokenBalance:  1_100,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 900_000,
	}
	ex2 := &corepb.Exchange{
		ExchangeId:         2,
		CreatorAddress:     exCreator(2),
		FirstTokenId:       []byte("1000002"),
		FirstTokenBalance:  5,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 50,
	}
	if err := sdb2.WriteExchangeV2(v2r2); err != nil {
		t.Fatal(err)
	}
	if err := sdb2.WriteExchangeV2(ex2); err != nil {
		t.Fatal(err)
	}
	r2, err := sdb2.Commit()
	if err != nil {
		t.Fatalf("commit R2: %v", err)
	}

	if r1 == r2 {
		t.Fatal("anchor: exchange change did not move the state root")
	}

	// Rewind to R1: V1 and V2 both recover their R1 values; exchange 2 absent.
	atR1, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR1.ReadExchange(1); !sameExchange(got, v1r1) {
		t.Fatalf("rewind R1 V1: got %+v", got)
	}
	if got := atR1.ReadExchangeV2(1); !sameExchange(got, v2r1) {
		t.Fatalf("rewind R1 V2: got %+v", got)
	}
	if got := atR1.ReadExchangeV2(2); got != nil {
		t.Fatalf("rewind R1: exchange 2 should not exist yet, got %+v", got)
	}

	// R2 keeps its own: updated V2 exchange 1, new exchange 2; V1 leg unchanged.
	atR2, err := New(r2, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR2.ReadExchangeV2(1); !sameExchange(got, v2r2) {
		t.Fatalf("R2 V2 exchange 1: got %+v", got)
	}
	if got := atR2.ReadExchangeV2(2); !sameExchange(got, ex2) {
		t.Fatalf("R2 V2 exchange 2: got %+v", got)
	}
	if got := atR2.ReadExchange(1); !sameExchange(got, v1r1) {
		t.Fatalf("R2 V1 exchange 1 should be untouched: got %+v", got)
	}
}
