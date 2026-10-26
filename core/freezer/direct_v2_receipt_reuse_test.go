package freezer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Keep the complete former composition as an independent strict-validation
// and codec oracle; do not replace this with the new preparation entry point.
func directV2ReceiptReference(number uint64, data, body []byte, externalize bool) ([]byte, error) {
	block, err := decodeDirectV2BlockBorrowed(number, body)
	if err != nil {
		return nil, err
	}
	if err := validateFreezerTransactionInfosRaw(number, block, data); err != nil {
		return nil, err
	}
	if externalize {
		return rawdb.CompactAncientV2RecordWithExternalLogs(rawdbAncientTxInfos, number, data, body)
	}
	return rawdb.CompactAncientV2Record(rawdbAncientTxInfos, number, data, body)
}

func directV2ReceiptReuse(number uint64, data, body []byte, externalize bool) ([]byte, error) {
	return directV2Transform(context.Background(), number, make([]tcommon.Hash, 1), externalize)(rawdbAncientTxInfos, number, data, body)
}

func receiptReuseField(n protowire.Number, data []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, n, protowire.BytesType), data)
}

func TestDirectV2ReceiptReuseMatchesStrictReference(t *testing.T) {
	chain := newDirectV2PreparedFixture(t, 2, 2)
	defer chain.db.Close()
	body, data := chain.blockRaw[1], chain.txInfosRaw[1]
	var ret corepb.TransactionRet
	if err := proto.Unmarshal(data, &ret); err != nil {
		t.Fatal(err)
	}
	marshal := func(v proto.Message) []byte {
		t.Helper()
		b, err := proto.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	withInfo := func(info []byte) []byte {
		out := marshal(&corepb.TransactionRet{BlockNumber: 1})
		out = append(out, receiptReuseField(3, info)...)
		return append(out, receiptReuseField(3, marshal(ret.Transactioninfo[1]))...)
	}
	changedInfo := func(change func(*corepb.TransactionInfo)) []byte {
		info := proto.Clone(ret.Transactioninfo[0]).(*corepb.TransactionInfo)
		change(info)
		return withInfo(marshal(info))
	}
	info := marshal(ret.Transactioninfo[0])
	unknown := receiptReuseField(1000, []byte{0xff, 0, 0x80})
	wrongID := bytes.Repeat([]byte{0xee}, 32)
	cases := []struct {
		name       string
		number     uint64
		data, body []byte
	}{
		{"ordinary", 1, data, body},
		{"unknown_outer", 1, append(bytes.Clone(data), unknown...), body},
		{"unknown_info", 1, withInfo(append(bytes.Clone(info), unknown...)), body},
		{"id_first_empty", 1, withInfo(append(receiptReuseField(1, nil), info...)), body},
		{"id_last_empty", 1, withInfo(append(bytes.Clone(info), receiptReuseField(1, nil)...)), body},
		{"id_first_wrong", 1, withInfo(append(receiptReuseField(1, wrongID), info...)), body},
		{"id_last_wrong", 1, withInfo(append(bytes.Clone(info), receiptReuseField(1, wrongID)...)), body},
		{"id_short", 1, changedInfo(func(i *corepb.TransactionInfo) { i.Id = []byte{1} }), body},
		{"id_absent", 1, changedInfo(func(i *corepb.TransactionInfo) { i.Id = nil }), body},
		{"info_number_wrong", 1, changedInfo(func(i *corepb.TransactionInfo) { i.BlockNumber = 2 }), body},
		{"info_number_negative", 1, changedInfo(func(i *corepb.TransactionInfo) { i.BlockNumber = -1 }), body},
		{"info_number_zero", 1, changedInfo(func(i *corepb.TransactionInfo) { i.BlockNumber = 0 }), body},
		{"info_malformed", 1, withInfo(append(bytes.Clone(info), 0x80)), body},
		{"ret_malformed", 1, append(bytes.Clone(data), 0x80), body},
		{"missing", 1, nil, body},
		{"coverage_short", 1, marshal(&corepb.TransactionRet{BlockNumber: 1, Transactioninfo: ret.Transactioninfo[:1]}), body},
		{"coverage_extra", 1, append(bytes.Clone(data), receiptReuseField(3, info)...), body},
		{"ret_number_wrong", 1, append(bytes.Clone(data), 0x08, 2), body},
		{"body_number_wrong", 1, data, chain.blockRaw[0]},
		{"body_malformed", 1, data, append(bytes.Clone(body), 0x80)},
		{"genesis_no_receipts", 0, nil, chain.blockRaw[0]},
		{"genesis_empty_proto", 0, []byte{0x10, 1}, chain.blockRaw[0]},
	}
	// Exceptional transaction raw_data must retain the typed coverage check and
	// the codec's separate wire-derived hashes. Use no-ID receipts so the strict
	// check can succeed even when the encodings hash differently.
	for name, txWire := range map[string][]byte{
		"duplicate_raw_data": append(receiptReuseField(1, []byte{0x70, 1}), receiptReuseField(1, []byte{0x70, 2})...),
		"duplicate_scalar":   receiptReuseField(1, []byte{0x70, 1, 0x70, 2}),
		"nonminimal_varint":  receiptReuseField(1, []byte{0x70, 0x81, 0}),
	} {
		var block corepb.Block
		if err := proto.Unmarshal(body, &block); err != nil {
			t.Fatal(err)
		}
		block.Transactions = nil
		wire := append(marshal(&block), receiptReuseField(1, txWire)...)
		row := marshal(&corepb.TransactionRet{BlockNumber: 1, Transactioninfo: []*corepb.TransactionInfo{{BlockNumber: 1, Log: ret.Transactioninfo[0].Log}}})
		cases = append(cases, struct {
			name       string
			number     uint64
			data, body []byte
		}{name, 1, row, wire})
	}
	for _, tc := range cases {
		for _, external := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/external_%t", tc.name, external), func(t *testing.T) {
				beforeData, beforeBody := bytes.Clone(tc.data), bytes.Clone(tc.body)
				want, wantErr := directV2ReceiptReference(tc.number, tc.data, tc.body, external)
				got, gotErr := directV2ReceiptReuse(tc.number, tc.data, tc.body, external)
				if !bytes.Equal(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
					t.Fatalf("output/error changed: %d/%d bytes, %v/%v", len(got), len(want), gotErr, wantErr)
				}
				if !bytes.Equal(beforeData, tc.data) || !bytes.Equal(beforeBody, tc.body) {
					t.Fatal("source bytes mutated")
				}
			})
		}
	}
}

func TestDirectV2ReceiptReuseKeepsTypedAndWireIdentitySeparate(t *testing.T) {
	chain := newDirectV2PreparedFixture(t, 2, 1)
	defer chain.db.Close()
	var pb corepb.Block
	if err := proto.Unmarshal(chain.blockRaw[1], &pb); err != nil {
		t.Fatal(err)
	}
	pb.Transactions = nil
	header, err := proto.Marshal(&pb)
	if err != nil {
		t.Fatal(err)
	}
	firstRaw := []byte{0x70, 1} // timestamp=1; the typed decoder merges timestamp=2 below.
	txWire := append(receiptReuseField(1, firstRaw), receiptReuseField(1, []byte{0x70, 2})...)
	body := append(header, receiptReuseField(1, txWire)...)
	block, err := decodeDirectV2BlockBorrowed(1, body)
	if err != nil {
		t.Fatal(err)
	}
	typedHash := block.Transactions()[0].Hash()
	wireHash := sha256.Sum256(firstRaw)
	if bytes.Equal(typedHash[:], wireHash[:]) {
		t.Fatal("fixture does not distinguish typed and wire identity")
	}
	for _, external := range []bool{false, true} {
		for name, id := range map[string][]byte{"typed": typedHash[:], "wire": wireHash[:]} {
			data, err := proto.Marshal(&corepb.TransactionRet{BlockNumber: 1, Transactioninfo: []*corepb.TransactionInfo{{Id: id, BlockNumber: 1, Log: []*corepb.TransactionInfo_Log{{Address: []byte{1}, Data: bytes.Repeat([]byte{2}, 512)}}}}})
			if err != nil {
				t.Fatal(err)
			}
			want, we := directV2ReceiptReference(1, data, body, external)
			got, ge := directV2ReceiptReuse(1, data, body, external)
			if !bytes.Equal(want, got) || fmt.Sprint(we) != fmt.Sprint(ge) {
				t.Fatalf("%s/external_%t changed: %v/%v", name, external, we, ge)
			}
			if name == "typed" && (ge != nil || !bytes.Equal(got, data)) {
				t.Fatal("typed ID should pass strict validation and retain its original wire row")
			}
			if name == "wire" && ge == nil {
				t.Fatal("wire ID must not replace the strict typed identity check")
			}
		}
	}
}

func BenchmarkDirectV2ReceiptPreparationReuse(b *testing.B) {
	chain := newDirectV2PreparedFixture(b, 2, 128)
	defer chain.db.Close()
	data, body := chain.txInfosRaw[1], chain.blockRaw[1]
	want, err := directV2ReceiptReference(1, data, body, true)
	if err != nil {
		b.Fatal(err)
	}
	for _, arm := range []struct {
		name string
		run  func(uint64, []byte, []byte, bool) ([]byte, error)
	}{{"reference", directV2ReceiptReference}, {"reuse", directV2ReceiptReuse}} {
		b.Run(arm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data) + len(body)))
			for i := 0; i < b.N; i++ {
				got, err := arm.run(1, data, body, true)
				if err != nil || !bytes.Equal(got, want) {
					b.Fatalf("output differs: %v", err)
				}
			}
		})
	}
}
