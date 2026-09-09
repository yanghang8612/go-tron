package rawdb

import (
	"bytes"
	"fmt"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Retain the former composition as an independent wire/error oracle and a
// same-input benchmark. In particular it re-hashes the body and decodes the
// compact receipt separately for identity checking and externalization.
func compactExternalLogsSeparatePasses(kind string, number uint64, data, body []byte) ([]byte, error) {
	compact, err := CompactAncientV2Record(kind, number, data, body)
	if err != nil || kind != ancientTxInfos || number == 0 && len(compact) == 0 {
		return compact, err
	}
	if !transactionInfoLogsMatchBody(number, compact, body) {
		return compact, nil
	}
	external, _, err := ExternalizeTransactionInfoLogs(compact)
	return external, err
}

func receiptReuseBytesField(number protowire.Number, data []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), data)
}

func TestCompactExternalLogsReusesDecodeWithoutChangingWire(t *testing.T) {
	block, hashes := testChainDBEventLogBlockWithTransactions(7, 1)
	body, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	mustMarshal := func(message proto.Message) []byte {
		t.Helper()
		data, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	info := &corepb.TransactionInfo{Id: hashes[0][:], BlockNumber: 7, Fee: 17,
		Log: []*corepb.TransactionInfo_Log{{Address: []byte{1}, Data: bytes.Repeat([]byte{0xa5}, 512)}}}
	infoWire := mustMarshal(info)
	retWith := func(payload []byte) []byte {
		out := mustMarshal(&corepb.TransactionRet{BlockNumber: 7, BlockTimeStamp: 1234})
		return append(out, receiptReuseBytesField(3, payload)...)
	}
	ret := retWith(infoWire)
	wrongID := bytes.Repeat([]byte{0x7f}, 32)
	unknown := receiptReuseBytesField(1000, []byte{0, 0xff, 0x80})
	wrongNumber := proto.Clone(info).(*corepb.TransactionInfo)
	wrongNumber.BlockNumber = 8
	negativeNumber := proto.Clone(info).(*corepb.TransactionInfo)
	negativeNumber.BlockNumber = -1
	logless := proto.Clone(info).(*corepb.TransactionInfo)
	logless.Log = nil
	tinyLog := proto.Clone(info).(*corepb.TransactionInfo)
	tinyLog.Log = []*corepb.TransactionInfo_Log{{Address: []byte{1}}}
	emptyBlock, _ := testChainDBEventLogBlockWithTransactions(8, 0)
	emptyBody, err := emptyBlock.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	enveloped, _, err := ExternalizeTransactionInfoLogs(ret)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		number uint64
		data   []byte
		body   []byte
	}{
		{"ordinary", 7, ret, body},
		{"unknown_outer", 7, append(bytes.Clone(ret), unknown...), body},
		{"unknown_info", 7, retWith(append(bytes.Clone(infoWire), unknown...)), body},
		{"duplicate_id_first_wrong", 7, retWith(append(receiptReuseBytesField(1, wrongID), infoWire...)), body},
		{"duplicate_id_last_wrong", 7, retWith(append(bytes.Clone(infoWire), receiptReuseBytesField(1, wrongID)...)), body},
		{"duplicate_id_first_empty", 7, retWith(append(receiptReuseBytesField(1, nil), infoWire...)), body},
		{"duplicate_id_last_empty", 7, retWith(append(bytes.Clone(infoWire), receiptReuseBytesField(1, nil)...)), body},
		{"wrong_info_number", 7, retWith(mustMarshal(wrongNumber)), body},
		{"negative_info_number", 7, retWith(mustMarshal(negativeNumber)), body},
		{"wrong_ret_number", 8, ret, body},
		{"logless", 7, retWith(mustMarshal(logless)), body},
		{"tiny_log", 7, retWith(mustMarshal(tinyLog)), body},
		{"already_enveloped", 7, enveloped, body},
		{"malformed_ret", 7, append(bytes.Clone(ret), 0x80), body},
		{"malformed_info", 7, retWith(append(bytes.Clone(infoWire), 0x80)), body},
		{"malformed_body", 7, ret, append(bytes.Clone(body), 0x80)},
		{"empty_genesis", 0, nil, body},
		{"empty_genesis_bad_body", 0, nil, []byte{0x80}},
		{"genesis_no_receipts", 0, mustMarshal(&corepb.TransactionRet{BlockTimeStamp: 1}), body},
		{"empty_non_genesis_no_transactions", 8, nil, emptyBody},
		{"missing_receipts", 7, nil, body},
		{"extra_receipt", 7, append(bytes.Clone(ret), receiptReuseBytesField(3, infoWire)...), body},
	}
	// Different raw_data encodings must still use wire hashes in both passes.
	// This intentionally avoids assuming that typed transaction hashes match.
	for name, transaction := range map[string][]byte{
		"duplicate_raw_data":   append(receiptReuseBytesField(1, []byte{0x08, 1}), receiptReuseBytesField(1, []byte{0x08, 2})...),
		"duplicate_raw_scalar": receiptReuseBytesField(1, []byte{0x08, 1, 0x08, 2}),
		"nonminimal_varint":    receiptReuseBytesField(1, []byte{0x08, 0x81, 0x00}),
	} {
		cases = append(cases, struct {
			name       string
			number     uint64
			data, body []byte
		}{name, 7, ret, receiptReuseBytesField(1, transaction)})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input, blockInput := bytes.Clone(tc.data), bytes.Clone(tc.body)
			want, wantErr := compactExternalLogsSeparatePasses(ancientTxInfos, tc.number, tc.data, tc.body)
			got, gotErr := CompactAncientV2RecordWithExternalLogs(ancientTxInfos, tc.number, tc.data, tc.body)
			if !bytes.Equal(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("result differs: lengths %d/%d, errors %v/%v", len(got), len(want), gotErr, wantErr)
			}
			if !bytes.Equal(input, tc.data) || !bytes.Equal(blockInput, tc.body) {
				t.Fatal("input wire bytes mutated")
			}
		})
	}
	for _, kind := range []string{ancientBlocks, ancientStateRoots, "unknown"} {
		got, err := CompactAncientV2RecordWithExternalLogs(kind, 7, ret, nil)
		if err != nil || !bytes.Equal(got, ret) {
			t.Fatalf("non-receipt %s changed: %v", kind, err)
		}
	}
}

func TestCompactExternalLogsReusePreservesMapAndUnknownFields(t *testing.T) {
	block, hashes := testChainDBEventLogBlockWithTransactions(7, 1)
	body, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	info := &corepb.TransactionInfo{Id: hashes[0][:], BlockNumber: 7,
		CancelUnfreezeV2Amount: map[string]int64{"ENERGY": 17, "BANDWIDTH": 21, "future": 29},
		Log:                    []*corepb.TransactionInfo_Log{{Address: []byte{1}, Data: bytes.Repeat([]byte{3}, 1024)}}}
	info.ProtoReflect().SetUnknown(receiptReuseBytesField(1000, []byte{4, 5, 6}))
	ret := &corepb.TransactionRet{BlockNumber: 7, Transactioninfo: []*corepb.TransactionInfo{info}}
	ret.ProtoReflect().SetUnknown(receiptReuseBytesField(1001, []byte{7, 8, 9}))
	data, err := proto.Marshal(ret)
	if err != nil {
		t.Fatal(err)
	}
	want, err := compactExternalLogsSeparatePasses(ancientTxInfos, 7, data, body)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := decodeTransactionRetStorage(want)
	if err != nil || !baseline.externalLogs {
		t.Fatalf("baseline envelope: %+v/%v", baseline, err)
	}
	var expected corepb.TransactionRet
	if err := proto.Unmarshal(baseline.payload, &expected); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		got, err := CompactAncientV2RecordWithExternalLogs(ancientTxInfos, 7, data, body)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := decodeTransactionRetStorage(got)
		if err != nil || !stored.externalLogs || stored.expectedLogCount != baseline.expectedLogCount {
			t.Fatalf("envelope integrity/identity differs: %+v/%v", stored, err)
		}
		var decoded corepb.TransactionRet
		if err := proto.Unmarshal(stored.payload, &decoded); err != nil || !proto.Equal(&decoded, &expected) {
			t.Fatalf("map or unknown fields changed: %v", err)
		}
	}
}

func BenchmarkCompactExternalLogsReuse(b *testing.B) {
	block, hashes := testChainDBEventLogBlockWithTransactions(7, 128)
	body, err := block.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	infos := make([]*corepb.TransactionInfo, len(hashes))
	for i := range infos {
		infos[i] = &corepb.TransactionInfo{Id: hashes[i][:], BlockNumber: 7, Fee: int64(i),
			Log: []*corepb.TransactionInfo_Log{{Address: []byte{1}, Data: bytes.Repeat([]byte{byte(i)}, 512)}}}
	}
	data, err := proto.Marshal(&corepb.TransactionRet{BlockNumber: 7, Transactioninfo: infos})
	if err != nil {
		b.Fatal(err)
	}
	want, err := compactExternalLogsSeparatePasses(ancientTxInfos, 7, data, body)
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		run  func(string, uint64, []byte, []byte) ([]byte, error)
	}{{"separate", compactExternalLogsSeparatePasses}, {"reuse", CompactAncientV2RecordWithExternalLogs}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data) + len(body)))
			for i := 0; i < b.N; i++ {
				got, err := tc.run(ancientTxInfos, 7, data, body)
				if err != nil || !bytes.Equal(got, want) {
					b.Fatalf("output mismatch: %v", err)
				}
			}
			b.ReportMetric(float64(len(want)), "output-bytes")
		})
	}
}
