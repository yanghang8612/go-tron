package rawdb

import (
	"bytes"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

type recordingProtoValueWriter struct {
	key      []byte
	value    []byte
	direct   bool
	fallback bool
}

func (w *recordingProtoValueWriter) Put(key, value []byte) error {
	w.fallback = true
	w.key = append([]byte(nil), key...)
	w.value = append([]byte(nil), value...)
	return nil
}

func (*recordingProtoValueWriter) Delete([]byte) error { return nil }

func (w *recordingProtoValueWriter) PutValueFunc(key []byte, valueLen int, fill func([]byte) error) error {
	w.direct = true
	w.key = append([]byte(nil), key...)
	w.value = make([]byte, valueLen)
	return fill(w.value)
}

func TestWriteTransactionInfosByBlockEncodesDirectlyIntoBatchValue(t *testing.T) {
	infos := []*corepb.TransactionInfo{{
		Id:             bytes.Repeat([]byte{0x42}, 32),
		BlockNumber:    7,
		BlockTimeStamp: 99,
		ContractResult: [][]byte{bytes.Repeat([]byte{0xab}, 4096)},
	}}
	w := new(recordingProtoValueWriter)
	if err := WriteTransactionInfosByBlock(w, 7, infos); err != nil {
		t.Fatal(err)
	}
	if !w.direct || w.fallback {
		t.Fatalf("writer paths = direct %v fallback %v, want true/false", w.direct, w.fallback)
	}
	want, err := proto.Marshal(&corepb.TransactionRet{
		BlockNumber:     7,
		BlockTimeStamp:  99,
		Transactioninfo: infos,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(w.key, txInfoBlockKey(7)) || !bytes.Equal(w.value, want) {
		t.Fatalf("direct transaction info row differs: key %x value len %d, want key %x value len %d", w.key, len(w.value), txInfoBlockKey(7), len(want))
	}
}

func TestWriteBlockBalanceTraceEncodesDirectlyIntoBatchValue(t *testing.T) {
	trace := &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   bytes.Repeat([]byte{0x24}, 32),
			Number: 7,
		},
		Timestamp: 99,
	}
	w := new(recordingProtoValueWriter)
	if err := WriteBlockBalanceTrace(w, 7, trace); err != nil {
		t.Fatal(err)
	}
	if !w.direct || w.fallback {
		t.Fatalf("writer paths = direct %v fallback %v, want true/false", w.direct, w.fallback)
	}
	want, err := proto.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(w.key, balanceTraceKey(7)) || !bytes.Equal(w.value, want) {
		t.Fatalf("direct balance trace row differs: key %x value len %d, want key %x value len %d", w.key, len(w.value), balanceTraceKey(7), len(want))
	}
}
