package rawdb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// This is the ef25 direct-freezer validation/codec composition. Keep it
// separate from PrepareAncientV2TransactionInfos as a behavior oracle.
func prepareAncientV2ReceiptReference(number uint64, txs []*types.Transaction, data, body []byte, external bool) ([]byte, error) {
	if len(data) == 0 {
		if number == 0 {
			if err := ValidateTransactionInfosForBlock(number, txs, nil, "chain freezer append"); err != nil {
				return nil, err
			}
		} else if len(txs) != 0 {
			return nil, fmt.Errorf("freezer: missing transaction info coverage for block %d with %d transactions", number, len(txs))
		}
	} else {
		var ret corepb.TransactionRet
		if err := proto.Unmarshal(data, &ret); err != nil {
			return nil, fmt.Errorf("freezer: decode tx infos for block %d: %w", number, err)
		}
		if ret.BlockNumber != 0 && ret.BlockNumber != int64(number) {
			return nil, fmt.Errorf("freezer: tx infos row %d contains block number %d", number, ret.BlockNumber)
		}
		if err := ValidateTransactionInfosForBlock(number, txs, ret.Transactioninfo, "chain freezer append"); err != nil {
			return nil, err
		}
	}
	if external {
		return CompactAncientV2RecordWithExternalLogs(ancientTxInfos, number, data, body)
	}
	return CompactAncientV2Record(ancientTxInfos, number, data, body)
}

func assertPreparedReceiptEquivalent(t *testing.T, want, got []byte, wantErr, gotErr error) {
	t.Helper()
	if fmt.Sprint(wantErr) != fmt.Sprint(gotErr) {
		t.Fatalf("errors changed: %v/%v", wantErr, gotErr)
	}
	if bytes.Equal(want, got) {
		return
	}
	a, ae := decodeTransactionRetStorage(want)
	b, be := decodeTransactionRetStorage(got)
	if ae != nil || be != nil || !a.externalLogs || !b.externalLogs || a.expectedLogCount != b.expectedLogCount {
		t.Fatalf("wire fallback or envelope integrity changed: %v/%v", ae, be)
	}
	var ar, br corepb.TransactionRet
	if err := proto.Unmarshal(a.payload, &ar); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(b.payload, &br); err != nil {
		t.Fatal(err)
	}
	// Protobuf maps have nondeterministic output order. Both envelopes must
	// authenticate and describe exactly the same message, including unknowns.
	if !proto.Equal(&ar, &br) {
		t.Fatal("decoded envelope changed")
	}
}

func TestPreparedReceiptPreservesMapsUnknownsAndFallback(t *testing.T) {
	block, hashes := testChainDBEventLogBlockWithTransactions(7, 1)
	body, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	for _, logs := range []int{0, 1, 1024} {
		for _, external := range []bool{false, true} {
			info := &corepb.TransactionInfo{Id: hashes[0][:], BlockNumber: 7, CancelUnfreezeV2Amount: map[string]int64{"ENERGY": 17, "BANDWIDTH": 21, "future": 29}}
			if logs != 0 {
				info.Log = []*corepb.TransactionInfo_Log{{Address: []byte{1}, Data: bytes.Repeat([]byte{3}, logs)}}
			}
			info.ProtoReflect().SetUnknown(receiptReuseBytesField(1000, []byte{4, 5, 6}))
			ret := &corepb.TransactionRet{BlockNumber: 7, Transactioninfo: []*corepb.TransactionInfo{info}}
			ret.ProtoReflect().SetUnknown(receiptReuseBytesField(1001, []byte{7, 8, 9}))
			data, err := proto.Marshal(ret)
			if err != nil {
				t.Fatal(err)
			}
			before := bytes.Clone(data)
			for i := 0; i < 16; i++ {
				want, we := prepareAncientV2ReceiptReference(7, block.Transactions(), data, body, external)
				got, ge := PrepareAncientV2TransactionInfos(7, block.Transactions(), data, body, external)
				assertPreparedReceiptEquivalent(t, want, got, we, ge)
				if !bytes.Equal(before, data) {
					t.Fatal("receipt source changed")
				}
			}
		}
	}
}

func FuzzPreparedReceiptMatchesReference(f *testing.F) {
	block, hashes := testChainDBEventLogBlockWithTransactions(7, 1)
	body, err := block.Marshal()
	if err != nil {
		f.Fatal(err)
	}
	data, err := proto.Marshal(&corepb.TransactionRet{BlockNumber: 7, Transactioninfo: []*corepb.TransactionInfo{{Id: hashes[0][:], BlockNumber: 7, Log: []*corepb.TransactionInfo_Log{{Address: []byte{1}, Data: bytes.Repeat([]byte{2}, 512)}}}}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(uint8(7), data, body, true)
	f.Add(uint8(0), []byte{}, body, true)
	f.Fuzz(func(t *testing.T, n uint8, data, body []byte, external bool) {
		if len(data)+len(body) > 128<<10 {
			t.Skip()
		}
		block, err := types.UnmarshalBlockBorrowed(body)
		if err != nil {
			t.Skip()
		}
		beforeData, beforeBody := bytes.Clone(data), bytes.Clone(body)
		want, we := prepareAncientV2ReceiptReference(uint64(n), block.Transactions(), data, body, external)
		got, ge := PrepareAncientV2TransactionInfos(uint64(n), block.Transactions(), data, body, external)
		assertPreparedReceiptEquivalent(t, want, got, we, ge)
		if !bytes.Equal(beforeData, data) || !bytes.Equal(beforeBody, body) {
			t.Fatal("source mutated")
		}
	})
}
