package types

import (
	"encoding/hex"
	"testing"

	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestTransactionHash(t *testing.T) {
	transfer := &contractpb.TransferContract{
		OwnerAddress: []byte{0x41, 0x01},
		ToAddress:    []byte{0x41, 0x02},
		Amount:       1000000,
	}
	anyParam, _ := anypb.New(transfer)

	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferContract,
					Parameter: anyParam,
				},
			},
			Timestamp:  12345,
			Expiration: 99999,
		},
	}

	tx := NewTransactionFromPB(pb)
	h := tx.Hash()
	if h.IsEmpty() {
		t.Fatal("tx hash should not be empty")
	}
	h2 := tx.Hash()
	if h != h2 {
		t.Fatal("tx hash not deterministic")
	}
}

func TestTransactionContractType(t *testing.T) {
	transfer := &contractpb.TransferContract{
		OwnerAddress: []byte{0x41, 0x01},
		ToAddress:    []byte{0x41, 0x02},
		Amount:       100,
	}
	anyParam, _ := anypb.New(transfer)

	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferContract,
					Parameter: anyParam,
				},
			},
		},
	}

	tx := NewTransactionFromPB(pb)
	ct := tx.ContractType()
	if ct != corepb.Transaction_Contract_TransferContract {
		t.Fatalf("expected TransferContract, got %v", ct)
	}
}

// TestRecoverSigners_TolerateTrailingBytes pins parity with java-tron's
// TransactionCapsule.checkWeight (size >= 65) + Rsv.fromSignature (decode
// bytes [0:65], drop the rest). The fixture is Nile testnet tx
// 8cc541fdad2c576291387462a9ec9f50594b13f05fa03b7d7916f348649a4909 at block
// 1,793,761: a real on-chain SUCCESS tx whose two signatures are 66 bytes
// each with a stray trailing 0x02. Refusing it desyncs go-tron from java.
func TestRecoverSigners_TolerateTrailingBytes(t *testing.T) {
	rawDataHex := "0a025edf22080676873b0a339fee40a8a4a2a7f62d5a66080112620a2d747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e5472616e73666572436f6e747261637412310a15411563915e194d8cfba1943570603f7606a31155081215419cf784b4cc7531f1598c4c322de9afdc597fe76018f80a70bfde9ea7f62d"
	sigsHex := []string{
		"c668cccaa6a194e305ed4b43090cbaa07f171c7efc97e2610e6ab803fb1612274f53783162f60f150dd7fb348eb5edeca38d5033631823aecabbd8b0bd96d3a00102",
		"a7e196759699ac9731cff8b4434392a8075c1f672f3f5e1a2604fc99ce047b72178aa267a6e32873d4eeb7657106ec3cab7358729cebcfbf3b9c80d66f4c84ca0102",
	}
	wantBase58 := []string{
		"TBvJUBXorwBPzqvV38vjDgegj5Eh6g2Tsq",
		"TJRabPrwbZy45sbavfcjinPJC18kjpRTv8",
	}
	const wantTxID = "8cc541fdad2c576291387462a9ec9f50594b13f05fa03b7d7916f348649a4909"

	rawData, err := hex.DecodeString(rawDataHex)
	if err != nil {
		t.Fatalf("decode rawData: %v", err)
	}
	rawPB := &corepb.TransactionRaw{}
	if err := proto.Unmarshal(rawData, rawPB); err != nil {
		t.Fatalf("unmarshal rawData: %v", err)
	}
	sigs := make([][]byte, len(sigsHex))
	for i, sh := range sigsHex {
		b, err := hex.DecodeString(sh)
		if err != nil {
			t.Fatalf("decode sig[%d]: %v", i, err)
		}
		if len(b) != 66 {
			t.Fatalf("sig[%d] len=%d, want 66 (this fixture is precisely about the +1-byte case)", i, len(b))
		}
		sigs[i] = b
	}

	tx := NewTransactionFromPB(&corepb.Transaction{
		RawData:   rawPB,
		Signature: sigs,
	})
	if got := tx.Hash(); hex.EncodeToString(got[:]) != wantTxID {
		t.Fatalf("txID mismatch: got %x, want %s", got, wantTxID)
	}

	addrs, err := tx.RecoverSigners()
	if err != nil {
		t.Fatalf("RecoverSigners: %v", err)
	}
	if len(addrs) != len(wantBase58) {
		t.Fatalf("addrs len=%d, want %d", len(addrs), len(wantBase58))
	}
	for i, a := range addrs {
		got := crypto.AddressToBase58(a)
		if got != wantBase58[i] {
			t.Fatalf("addrs[%d] = %s, want %s", i, got, wantBase58[i])
		}
	}
}

// TestRecoverSigners_RejectsShortSignature pins the lower bound: java's
// checkWeight throws SignatureFormatException on sig.size() < 65, and so
// must we.
func TestRecoverSigners_RejectsShortSignature(t *testing.T) {
	tx := NewTransactionFromPB(&corepb.Transaction{
		RawData:   &corepb.TransactionRaw{Timestamp: 1, Expiration: 2},
		Signature: [][]byte{make([]byte, 64)},
	})
	if _, err := tx.RecoverSigners(); err != ErrBadSignatureLength {
		t.Fatalf("err = %v, want ErrBadSignatureLength", err)
	}
}

func TestTransactionMarshalRoundTrip(t *testing.T) {
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  42,
			Expiration: 100,
		},
	}
	tx := NewTransactionFromPB(pb)
	data, err := tx.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := UnmarshalTransaction(data)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(tx.Proto(), tx2.Proto()) {
		t.Fatal("round trip failed")
	}
}
