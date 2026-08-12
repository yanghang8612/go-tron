package rawdb

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestIteratePersistedStateDomainChangeBlockBorrowedMatchesOwned(t *testing.T) {
	const rows = 128
	owner := common.Address{common.AddressPrefixMainnet, 0x44}
	changes := make([]*StateDomainChange, rows)
	for i := range changes {
		changes[i] = &StateDomainChange{
			BlockNum: 9, TxNum: 100 + uint64(i/4), Seq: uint64(i + 1),
			FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 3,
			Domain: kvdomains.ContractStorage, Key: []byte{0x7f, byte(i)},
			PrevExists: true, Prev: bytes.Repeat([]byte{byte(i)}, 32),
		}
	}
	raw := encodeBorrowedStateDomainChangeTestBlock(t, changes)
	compressed, compressedOK := encodeStateDomainChangeBlockStorage(raw)
	if !compressedOK {
		t.Fatalf("test block did not compress: %d bytes", len(raw))
	}
	owned, err := decodePersistedStateDomainChangeBlock(raw, 9)
	if err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string][]byte{"raw": raw, "compressed": compressed} {
		t.Run(name, func(t *testing.T) {
			got := make([]*StateDomainChange, 0, rows)
			cont, err := iteratePersistedStateDomainChangeBlockBorrowed(payload, 9, func(change *StateDomainChange) (bool, error) {
				got = append(got, cloneStateDomainChange(change))
				return true, nil
			})
			if err != nil || !cont {
				t.Fatalf("borrowed iterate cont=%v err=%v", cont, err)
			}
			if !reflect.DeepEqual(got, owned) {
				t.Fatalf("borrowed rows differ from owned decode\ngot:  %+v\nwant: %+v", got, owned)
			}
		})
	}
}

func TestIteratePersistedStateDomainChangeBlockBorrowedStopsAndReturnsError(t *testing.T) {
	changes := []*StateDomainChange{
		borrowedStateDomainChangeTestRow(4, 1, 10),
		borrowedStateDomainChangeTestRow(4, 2, 11),
	}
	raw := encodeBorrowedStateDomainChangeTestBlock(t, changes)
	seen := 0
	cont, err := iteratePersistedStateDomainChangeBlockBorrowed(raw, 4, func(*StateDomainChange) (bool, error) {
		seen++
		return false, nil
	})
	if err != nil || cont || seen != 1 {
		t.Fatalf("early stop cont=%v seen=%d err=%v", cont, seen, err)
	}
	wantErr := errors.New("stop")
	cont, err = iteratePersistedStateDomainChangeBlockBorrowed(raw, 4, func(*StateDomainChange) (bool, error) {
		return false, wantErr
	})
	if cont || !errors.Is(err, wantErr) {
		t.Fatalf("callback error cont=%v err=%v", cont, err)
	}
}

func TestIteratePersistedStateDomainChangeBlockBorrowedWithScratchAllocatesNothing(t *testing.T) {
	raw := encodeBorrowedStateDomainChangeTestBlock(t, []*StateDomainChange{
		borrowedStateDomainChangeTestRow(4, 1, 10),
		borrowedStateDomainChangeTestRow(4, 2, 11),
	})
	var scratch StateDomainChange
	allocs := testing.AllocsPerRun(100, func() {
		cont, err := iteratePersistedStateDomainChangeBlockBorrowedWithScratch(raw, 4, &scratch, borrowedStateDomainChangeNoop)
		if err != nil || !cont {
			t.Fatalf("borrowed iterate cont=%v err=%v", cont, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("borrowed raw block decode allocated %.2f objects, want zero", allocs)
	}
}

func TestIteratePersistedStateDomainChangeBlockBorrowedRejectsMalformed(t *testing.T) {
	type testRow struct {
		TxNum      uint64
		FlatDomain uint8
		Owner      []byte
		Generation uint64
		Domain     uint16
		Key        []byte
		PrevExists uint64
		Prev       []byte
	}
	type testBlock struct {
		Version  uint8
		FirstSeq uint64
		Rows     []testRow
	}
	validRow := testRow{TxNum: 10, FlatDomain: uint8(StateFlatDomainKVLatest), Owner: make([]byte, common.AddressLength), Domain: uint16(kvdomains.ContractStorage), Key: []byte("key"), PrevExists: 1, Prev: []byte("prev")}
	encode := func(value any) []byte {
		t.Helper()
		data, err := rlp.EncodeToBytes(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	valid := encode(&testBlock{Version: persistedStateDomainChangeBlockVersion, FirstSeq: 1, Rows: []testRow{validRow}})
	badOwner := validRow
	badOwner.Owner = badOwner.Owner[:common.AddressLength-1]
	badBool := validRow
	badBool.PrevExists = 2
	unorderA := validRow
	unorderA.TxNum = 11
	unorderB := validRow
	unorderB.TxNum = 10
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{name: "trailing", data: append(append([]byte(nil), valid...), 0x80), want: "trailing bytes"},
		{name: "version", data: encode(&testBlock{Version: 2, FirstSeq: 1, Rows: []testRow{validRow}}), want: "unsupported"},
		{name: "zero sequence", data: encode(&testBlock{Version: 1, FirstSeq: 0, Rows: []testRow{validRow}}), want: "first sequence"},
		{name: "empty", data: encode(&testBlock{Version: 1, FirstSeq: 1}), want: "empty"},
		{name: "owner", data: encode(&testBlock{Version: 1, FirstSeq: 1, Rows: []testRow{badOwner}}), want: "owner has"},
		{name: "boolean", data: encode(&testBlock{Version: 1, FirstSeq: 1, Rows: []testRow{badBool}}), want: "previous presence"},
		{name: "tx order", data: encode(&testBlock{Version: 1, FirstSeq: 1, Rows: []testRow{unorderA, unorderB}}), want: "follows txNum"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := iteratePersistedStateDomainChangeBlockBorrowed(test.data, 7, func(*StateDomainChange) (bool, error) { return true, nil })
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestIterateStateDomainChangesByBlockTxRangeBorrowedFiltersAndAddsHash(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	for blockNum := uint64(1); blockNum <= 2; blockNum++ {
		blockHash := common.Hash{byte(blockNum), 0xaa}
		if err := WriteStateTxRange(db, blockNum, blockHash, blockNum*10, blockNum*10+1); err != nil {
			t.Fatal(err)
		}
		changes := []*StateDomainChange{
			borrowedStateDomainChangeTestRow(blockNum, 1, blockNum*10),
			borrowedStateDomainChangeTestRow(blockNum, 2, blockNum*10+1),
		}
		if err := WriteStateDomainChangeBlockRows(db, changes); err != nil {
			t.Fatal(err)
		}
	}
	var got []*StateDomainChange
	var scratchAddress *StateDomainChange
	if err := IterateStateDomainChangesByBlockTxRangeBorrowed(db, 1, 2, 11, 20, func(change *StateDomainChange) (bool, error) {
		if scratchAddress == nil {
			scratchAddress = change
		} else if change != scratchAddress {
			t.Fatalf("borrowed iterator changed scratch address between blocks")
		}
		got = append(got, cloneStateDomainChange(change))
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].TxNum != 11 || got[0].BlockHash != (common.Hash{0x01, 0xaa}) || got[1].TxNum != 20 || got[1].BlockHash != (common.Hash{0x02, 0xaa}) {
		t.Fatalf("borrowed filtered rows = %+v", got)
	}
}

func TestIterateStateTxRangesByBlockRangeBorrowedReusesScratch(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	for blockNum := uint64(1); blockNum <= 4; blockNum++ {
		if err := WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum*10, blockNum*10+1); err != nil {
			t.Fatal(err)
		}
	}
	var (
		got            []StateTxRange
		scratchAddress *StateTxRange
	)
	if err := IterateStateTxRangesByBlockRangeBorrowed(db, 2, 3, func(row *StateTxRange) (bool, error) {
		if scratchAddress == nil {
			scratchAddress = row
		} else if row != scratchAddress {
			t.Fatal("borrowed tx-range iterator changed scratch address")
		}
		got = append(got, *row)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []StateTxRange{
		{BlockNum: 2, BlockHash: common.Hash{2}, BeginTxNum: 20, EndTxNum: 21},
		{BlockNum: 3, BlockHash: common.Hash{3}, BeginTxNum: 30, EndTxNum: 31},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("borrowed tx ranges = %+v, want %+v", got, want)
	}
}

func TestIterateStateDomainChangesByBlockTxRangeBorrowedRejectsStandaloneRows(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteStateTxRange(db, 1, common.Hash{1}, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateDomainChangeRow(db, borrowedStateDomainChangeTestRow(1, 1, 1)); err != nil {
		t.Fatal(err)
	}
	err := IterateStateDomainChangesByBlockTxRangeBorrowed(db, 1, 1, 1, 1, func(*StateDomainChange) (bool, error) { return true, nil })
	if !errors.Is(err, ErrStateDomainChangeBorrowedLegacyRows) {
		t.Fatalf("standalone row error = %v", err)
	}
}

func TestIterateStateDomainChangesByBlockTxRangeBorrowedRejectsLegacySequenceZeroRow(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteStateTxRange(db, 1, common.Hash{1}, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateDomainChangeRow(db, borrowedStateDomainChangeTestRow(1, 0, 1)); err != nil {
		t.Fatal(err)
	}
	err := IterateStateDomainChangesByBlockTxRangeBorrowed(db, 1, 1, 1, 1, borrowedStateDomainChangeNoop)
	if !errors.Is(err, ErrStateDomainChangeBorrowedLegacyRows) {
		t.Fatalf("legacy sequence-zero row error = %v", err)
	}
}

func TestIterateStateDomainChangesByBlockTxRangeBorrowedRejectsCrossBlockDisorder(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	for blockNum := uint64(1); blockNum <= 2; blockNum++ {
		if err := WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, 10, 11); err != nil {
			t.Fatal(err)
		}
		txNum := uint64(11)
		if blockNum == 2 {
			txNum = 10
		}
		if err := WriteStateDomainChangeBlockRows(db, []*StateDomainChange{borrowedStateDomainChangeTestRow(blockNum, 1, txNum)}); err != nil {
			t.Fatal(err)
		}
	}
	err := IterateStateDomainChangesByBlockTxRangeBorrowed(db, 1, 2, 10, 11, borrowedStateDomainChangeNoop)
	if err == nil || !strings.Contains(err.Error(), "not ordered") {
		t.Fatalf("cross-block order error = %v", err)
	}
}

func encodeBorrowedStateDomainChangeTestBlock(t testing.TB, changes []*StateDomainChange) []byte {
	t.Helper()
	rows := make([]persistedStateDomainChange, len(changes))
	for i, change := range changes {
		rows[i] = persistedStateDomainChange{
			TxNum: change.TxNum, FlatDomain: change.FlatDomain, Owner: change.Owner,
			Generation: change.Generation, Domain: change.Domain, Key: change.Key,
			PrevExists: change.PrevExists, Prev: change.Prev,
		}
	}
	data, err := rlp.EncodeToBytes(&persistedStateDomainChangeBlock{Version: persistedStateDomainChangeBlockVersion, FirstSeq: changes[0].Seq, Rows: rows})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func borrowedStateDomainChangeTestRow(blockNum, seq, txNum uint64) *StateDomainChange {
	return &StateDomainChange{
		BlockNum: blockNum, TxNum: txNum, Seq: seq,
		FlatDomain: StateFlatDomainKVLatest, Owner: common.Address{common.AddressPrefixMainnet, byte(blockNum)},
		Domain: kvdomains.ContractStorage, Key: []byte{byte(seq)}, PrevExists: true, Prev: []byte("previous"),
	}
}

func borrowedStateDomainChangeNoop(*StateDomainChange) (bool, error) { return true, nil }
