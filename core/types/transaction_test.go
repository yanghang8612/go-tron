package types

import (
	"bytes"
	"encoding/hex"
	"errors"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/common"
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

func TestTransactionDecodedContractMemoized(t *testing.T) {
	transfer := &contractpb.TransferContract{
		OwnerAddress: []byte{0x41, 0x01},
		ToAddress:    []byte{0x41, 0x02},
		Amount:       100,
	}
	parameter, err := anypb.New(transfer)
	if err != nil {
		t.Fatal(err)
	}
	tx := NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{
		Contract: []*corepb.Transaction_Contract{{
			Type:      corepb.Transaction_Contract_TransferContract,
			Parameter: parameter,
		}},
	}})

	const readers = 32
	results := make(chan proto.Message, readers)
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg, err := tx.DecodedContract()
			results <- msg
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("DecodedContract: %v", err)
		}
	}
	var first proto.Message
	for msg := range results {
		if first == nil {
			first = msg
			continue
		}
		if msg != first {
			t.Fatal("DecodedContract returned different message instances")
		}
	}
	got, ok := first.(*contractpb.TransferContract)
	if !ok {
		t.Fatalf("decoded type = %T, want *TransferContract", first)
	}
	if !proto.Equal(got, transfer) {
		t.Fatalf("decoded contract = %v, want %v", got, transfer)
	}
}

func TestTransactionDecodedContractMemoizesError(t *testing.T) {
	tx := NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{
		Contract: []*corepb.Transaction_Contract{{
			Type: corepb.Transaction_Contract_TransferContract,
			Parameter: &anypb.Any{
				TypeUrl: "type.googleapis.com/protocol.DoesNotExist",
				Value:   []byte{1, 2, 3},
			},
		}},
	}})

	msg1, err1 := tx.DecodedContract()
	msg2, err2 := tx.DecodedContract()
	if err1 == nil || err2 == nil {
		t.Fatalf("errors = (%v, %v), want both non-nil", err1, err2)
	}
	if msg1 != nil || msg2 != nil {
		t.Fatalf("messages = (%T, %T), want nil", msg1, msg2)
	}
	if err1 != err2 {
		t.Fatal("DecodedContract did not memoize the error instance")
	}
}

var decodedContractBenchmarkSink proto.Message

var transactionHashBenchmarkSink common.Hash

func benchmarkTransactionPB(b testing.TB) *corepb.Transaction {
	b.Helper()
	transfer := &contractpb.TransferContract{
		OwnerAddress: bytes.Repeat([]byte{0x41}, common.AddressLength),
		ToAddress:    bytes.Repeat([]byte{0x42}, common.AddressLength),
		Amount:       1_000_000,
	}
	parameter, err := anypb.New(transfer)
	if err != nil {
		b.Fatal(err)
	}
	return &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: parameter,
			}},
			RefBlockBytes: []byte{0x12, 0x34},
			RefBlockHash:  bytes.Repeat([]byte{0x56}, 8),
			Expiration:    1_800_000_000_000,
			Timestamp:     1_799_999_940_000,
		},
		Signature: [][]byte{bytes.Repeat([]byte{0x78}, 65)},
		Ret: []*corepb.Transaction_Result{{
			Fee:         100_000,
			ContractRet: corepb.Transaction_Result_SUCCESS,
		}},
	}
}

func BenchmarkTransactionHashCold(b *testing.B) {
	pb := benchmarkTransactionPB(b)
	b.ReportAllocs()
	for b.Loop() {
		tx := NewTransactionFromPB(pb)
		transactionHashBenchmarkSink = tx.Hash()
	}
}

func BenchmarkTransactionDecodedContract(b *testing.B) {
	transfer := &contractpb.TransferContract{
		OwnerAddress: make([]byte, common.AddressLength),
		ToAddress:    make([]byte, common.AddressLength),
		Amount:       1_000_000,
	}
	parameter, err := anypb.New(transfer)
	if err != nil {
		b.Fatal(err)
	}
	tx := NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{
		Contract: []*corepb.Transaction_Contract{{
			Type:      corepb.Transaction_Contract_TransferContract,
			Parameter: parameter,
		}},
	}})
	if _, err := tx.DecodedContract(); err != nil {
		b.Fatal(err)
	}

	b.Run("AnyUnmarshalNew", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decodedContractBenchmarkSink, _ = parameter.UnmarshalNew()
		}
	})
	b.Run("Memoized", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decodedContractBenchmarkSink, _ = tx.DecodedContract()
		}
	})
}

// TestRecoverSigners_JavaSignatureV pins java-tron Rsv.fromSignature parity for
// 65-byte signatures whose v byte is already 27/28. The fixture is Nile block
// 3,595,432 tx 76d86fa20262de881670ff502e4164fb99f6b39f9652a00f0c173ab60aa2ae10;
// its signature ends in 0x1c. go-ethereum expects recovery id 0/1, so passing
// the raw signature directly rejects a block java-tron accepts.
func TestRecoverSigners_JavaSignatureV(t *testing.T) {
	rawDataHex := "0a02dc902208d3638f92df8a2be94090a0eafb8a2e5a69080112650a2d747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e5472616e73666572436f6e747261637412340a1541b0e03d96eec5aba4037e4fca2431da6fdba85068121541b03c7de5f60a49a2f3098463691ca2e137d822d618e0d691d01270bfa9cdd28a2e"
	sigHex := "c37ebd8ba2abfdcd2945d3f63994165bf778fe35415dd6aed37b77446ef9783170f0742aa17d7fa56fb98c65e67db7190b927272a95e2de72dee1cb9d166c6441c"
	const wantBase58 = "TS6SZL6hBF4pVU6oyvSJubZqKfi73BV48F"
	const wantTxID = "76d86fa20262de881670ff502e4164fb99f6b39f9652a00f0c173ab60aa2ae10"

	rawData, err := hex.DecodeString(rawDataHex)
	if err != nil {
		t.Fatalf("decode rawData: %v", err)
	}
	rawPB := &corepb.TransactionRaw{}
	if err := proto.Unmarshal(rawData, rawPB); err != nil {
		t.Fatalf("unmarshal rawData: %v", err)
	}
	sig, err := hex.DecodeString(sigHex)
	if err != nil {
		t.Fatalf("decode sig: %v", err)
	}
	if len(sig) != 65 {
		t.Fatalf("sig len=%d, want 65", len(sig))
	}
	if sig[64] != 28 {
		t.Fatalf("sig v=%d, want 28", sig[64])
	}

	tx := NewTransactionFromPB(&corepb.Transaction{
		RawData:   rawPB,
		Signature: [][]byte{sig},
	})
	if got := tx.Hash(); hex.EncodeToString(got[:]) != wantTxID {
		t.Fatalf("txID mismatch: got %x, want %s", got, wantTxID)
	}
	addrs, err := tx.RecoverSigners()
	if err != nil {
		t.Fatalf("RecoverSigners: %v", err)
	}
	if len(addrs) != 1 {
		t.Fatalf("addrs len=%d, want 1", len(addrs))
	}
	if got := crypto.AddressToBase58(addrs[0]); got != wantBase58 {
		t.Fatalf("addr = %s, want %s", got, wantBase58)
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

func TestSignatureForRecoveryBorrowsCanonicalInput(t *testing.T) {
	canonical := make([]byte, 66)
	canonical[64] = 1
	canonical[65] = 0x7f
	got, err := signatureForRecovery(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 65 || got[64] != 1 {
		t.Fatalf("canonical recovery signature = %x", got)
	}
	if &got[0] != &canonical[0] {
		t.Fatal("canonical v=0/1 signature should borrow immutable protobuf bytes")
	}

	javaStyle := append([]byte(nil), canonical...)
	javaStyle[64] = 28
	normalized, err := signatureForRecovery(javaStyle)
	if err != nil {
		t.Fatal(err)
	}
	if normalized[64] != 1 {
		t.Fatalf("normalized recovery id = %d, want 1", normalized[64])
	}
	if &normalized[0] == &javaStyle[0] {
		t.Fatal("Java-style v=27/28 signature must use a normalized copy")
	}
	if javaStyle[64] != 28 {
		t.Fatal("normalization mutated protobuf signature")
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
	// The memoized error must be byte-for-byte the same on the second call —
	// the parallel pre-pass warms this and the serial path must observe the
	// identical rejection.
	if _, err := tx.RecoverSigners(); err != ErrBadSignatureLength {
		t.Fatalf("memoized err = %v, want ErrBadSignatureLength", err)
	}
}

// TestRecoverSigners_MemoIsIdentical proves the RecoverSigners memo returns a
// value identical to a fresh, un-memoized recovery — the invariant the parallel
// pre-verification pass relies on (warm a memo, serial path reads it, identical
// accept/reject). Uses the same golden vector as TestRecoverSigners_JavaSignatureV.
func TestRecoverSigners_MemoIsIdentical(t *testing.T) {
	rawDataHex := "0a02dc902208d3638f92df8a2be94090a0eafb8a2e5a69080112650a2d747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e5472616e73666572436f6e747261637412340a1541b0e03d96eec5aba4037e4fca2431da6fdba85068121541b03c7de5f60a49a2f3098463691ca2e137d822d618e0d691d01270bfa9cdd28a2e"
	sigHex := "c37ebd8ba2abfdcd2945d3f63994165bf778fe35415dd6aed37b77446ef9783170f0742aa17d7fa56fb98c65e67db7190b927272a95e2de72dee1cb9d166c6441c"
	rawData, _ := hex.DecodeString(rawDataHex)
	rawPB := &corepb.TransactionRaw{}
	if err := proto.Unmarshal(rawData, rawPB); err != nil {
		t.Fatalf("unmarshal rawData: %v", err)
	}
	sig, _ := hex.DecodeString(sigHex)

	mk := func() *Transaction {
		return NewTransactionFromPB(&corepb.Transaction{RawData: proto.Clone(rawPB).(*corepb.TransactionRaw), Signature: [][]byte{append([]byte(nil), sig...)}})
	}
	// Reference: a fresh tx, first (cold) recovery.
	want, err := mk().RecoverSigners()
	if err != nil {
		t.Fatalf("reference recovery: %v", err)
	}

	// A second tx: warm it once, then read it again — both reads must equal the
	// cold reference. (Same instance is what Block.Transactions() now returns.)
	tx := mk()
	warm1, err := tx.RecoverSigners()
	if err != nil {
		t.Fatalf("warm recovery: %v", err)
	}
	warm2, _ := tx.RecoverSigners()
	if len(warm1) != len(want) || len(warm2) != len(want) {
		t.Fatalf("len mismatch: cold=%d warm1=%d warm2=%d", len(want), len(warm1), len(warm2))
	}
	for i := range want {
		if warm1[i] != want[i] || warm2[i] != want[i] {
			t.Fatalf("addr[%d] mismatch: cold=%x warm1=%x warm2=%x", i, want[i], warm1[i], warm2[i])
		}
	}
}

// TestRecoverSigners_ConcurrentWarmIsRaceFree exercises the sync.Once memo under
// many concurrent callers (the parallel pre-pass shape) — run with -race. All
// goroutines must observe the same recovered address.
func TestRecoverSigners_ConcurrentWarmIsRaceFree(t *testing.T) {
	rawDataHex := "0a02dc902208d3638f92df8a2be94090a0eafb8a2e5a69080112650a2d747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e5472616e73666572436f6e747261637412340a1541b0e03d96eec5aba4037e4fca2431da6fdba85068121541b03c7de5f60a49a2f3098463691ca2e137d822d618e0d691d01270bfa9cdd28a2e"
	sigHex := "c37ebd8ba2abfdcd2945d3f63994165bf778fe35415dd6aed37b77446ef9783170f0742aa17d7fa56fb98c65e67db7190b927272a95e2de72dee1cb9d166c6441c"
	rawData, _ := hex.DecodeString(rawDataHex)
	rawPB := &corepb.TransactionRaw{}
	if err := proto.Unmarshal(rawData, rawPB); err != nil {
		t.Fatalf("unmarshal rawData: %v", err)
	}
	sig, _ := hex.DecodeString(sigHex)
	tx := NewTransactionFromPB(&corepb.Transaction{RawData: rawPB, Signature: [][]byte{sig}})

	const n = 32
	results := make([][]common.Address, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			addrs, err := tx.RecoverSigners()
			if err != nil {
				t.Errorf("goroutine %d: %v", i, err)
				return
			}
			results[i] = addrs
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if len(results[i]) != len(results[0]) {
			t.Fatalf("goroutine %d len mismatch", i)
		}
		for j := range results[0] {
			if results[i][j] != results[0][j] {
				t.Fatalf("goroutine %d addr[%d] diverged: %x vs %x", i, j, results[i][j], results[0][j])
			}
		}
	}
}

func TestCanonicalSignatureKey_NormalizesLikeJavaTron(t *testing.T) {
	sig0 := make([]byte, 66)
	for i := range 64 {
		sig0[i] = byte(i + 1)
	}
	sig0[64] = 1
	sig0[65] = 2
	sig28 := append([]byte(nil), sig0...)
	sig28[64] = 28
	sig28[65] = 3

	key0, err := CanonicalSignatureKey(sig0)
	if err != nil {
		t.Fatalf("CanonicalSignatureKey(v=1): %v", err)
	}
	key28, err := CanonicalSignatureKey(sig28)
	if err != nil {
		t.Fatalf("CanonicalSignatureKey(v=28): %v", err)
	}
	if key0 != key28 {
		t.Fatal("v=1 and v=28 should dedupe to the same java-tron signature key")
	}
	if len(key0) != 65 {
		t.Fatalf("key len=%d, want 65", len(key0))
	}
	if key0[0] != 28 {
		t.Fatalf("key header=%d, want 28", key0[0])
	}

	bad := append([]byte(nil), sig0...)
	bad[64] = 26
	if _, err := CanonicalSignatureKey(bad); !errors.Is(err, ErrBadSignatureRecoveryID) {
		t.Fatalf("bad recovery id err=%v, want ErrBadSignatureRecoveryID", err)
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
