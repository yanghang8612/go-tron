package types

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var benchmarkBlockHash common.Hash
var benchmarkBlockBytes []byte
var benchmarkDecodedBlock *Block
var benchmarkDecodedProtoBlock *corepb.Block
var benchmarkDecodedContract *corepb.Transaction_Contract
var benchmarkBlockTransactions []*Transaction

func BenchmarkBlockTransactionsTriggerDecodeLargeData(b *testing.B) {
	const txCount = 256
	parameter, err := anypb.New(&contractpb.TriggerSmartContract{
		OwnerAddress:    bytes.Repeat([]byte{0x41}, common.AddressLength),
		ContractAddress: bytes.Repeat([]byte{0x42}, common.AddressLength),
		Data:            bytes.Repeat([]byte{0xaa}, 132),
	})
	if err != nil {
		b.Fatal(err)
	}
	pb := &corepb.Block{Transactions: make([]*corepb.Transaction, txCount)}
	for i := range pb.Transactions {
		pb.Transactions[i] = &corepb.Transaction{RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TriggerSmartContract,
				Parameter: parameter,
			}},
		}}
	}
	b.ReportAllocs()
	b.SetBytes(txCount * 132)
	b.ResetTimer()
	for b.Loop() {
		txs := NewBlockFromPB(pb).Transactions()
		for _, tx := range txs {
			if _, err := tx.DecodedContract(); err != nil {
				b.Fatal(err)
			}
		}
		benchmarkBlockTransactions = txs
	}
}

func blockHashRawTestBlock(txCount, dataSize int) *Block {
	txs := make([]*corepb.Transaction, txCount)
	for i := range txs {
		txs[i] = &corepb.Transaction{
			RawData: &corepb.TransactionRaw{
				RefBlockBytes: []byte{byte(i >> 8), byte(i)},
				Data:          bytes.Repeat([]byte{byte(i)}, dataSize),
				Timestamp:     int64(i + 1),
			},
			Signature: [][]byte{bytes.Repeat([]byte{byte(i + 1)}, 65)},
		}
	}
	raw := &corepb.BlockHeaderRaw{
		Number:           12_345_678,
		Timestamp:        1_700_000_000_000,
		TxTrieRoot:       bytes.Repeat([]byte{0x11}, common.HashLength),
		ParentHash:       bytes.Repeat([]byte{0x22}, common.HashLength),
		WitnessAddress:   append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x33}, common.AccountIDLength)...),
		Version:          34,
		AccountStateRoot: bytes.Repeat([]byte{0x44}, common.HashLength),
	}
	rawUnknown := protowire.AppendTag(nil, 100, protowire.BytesType)
	rawUnknown = protowire.AppendBytes(rawUnknown, []byte("raw-unknown"))
	raw.ProtoReflect().SetUnknown(rawUnknown)
	return NewBlockFromPB(&corepb.Block{
		Transactions: txs,
		BlockHeader: &corepb.BlockHeader{
			RawData:          raw,
			WitnessSignature: bytes.Repeat([]byte{0x55}, 65),
		},
	})
}

func BenchmarkBlockHashFromRaw(b *testing.B) {
	block := blockHashRawTestBlock(200, 256)
	data, err := block.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.Run("full-unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			decoded, err := UnmarshalBlock(data)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkBlockHash = decoded.Hash()
		}
	})
	b.Run("raw-header-scan", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkBlockHash, err = BlockHashFromRaw(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBlockIDFromRaw(b *testing.B) {
	block := blockDecodeReserveTestBlock(200)
	data, err := block.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.Run("full-unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decoded, err := UnmarshalBlock(data)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkBlockHash = decoded.Hash()
		}
	})
	b.Run("header-only", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			id, err := BlockIDFromRaw(data)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkBlockHash = id.Hash
		}
	})
}

func TestBlockIDFromRawMatchesFullDecode(t *testing.T) {
	wantBlock := blockDecodeReserveTestBlock(4)
	data, err := wantBlock.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	want := wantBlock.ID()
	got, err := BlockIDFromRaw(data)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("BlockIDFromRaw() = %+v, want %+v", got, want)
	}

	// Singular message fields merge when they occur more than once. Keep that
	// generated-protobuf behavior on the header-only path rather than assuming
	// canonical one-field wire input.
	firstHeader, err := proto.Marshal(&corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
		Number:     42,
		ParentHash: bytes.Repeat([]byte{0x11}, common.HashLength),
	}})
	if err != nil {
		t.Fatal(err)
	}
	secondHeader, err := proto.Marshal(&corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
		Timestamp:      123456,
		WitnessAddress: bytes.Repeat([]byte{0x22}, common.AddressLength),
	}})
	if err != nil {
		t.Fatal(err)
	}
	mergedWire := protowire.AppendTag(nil, 2, protowire.BytesType)
	mergedWire = protowire.AppendBytes(mergedWire, firstHeader)
	mergedWire = protowire.AppendTag(mergedWire, 2, protowire.BytesType)
	mergedWire = protowire.AppendBytes(mergedWire, secondHeader)
	var mergedPB corepb.Block
	if err := proto.Unmarshal(mergedWire, &mergedPB); err != nil {
		t.Fatal(err)
	}
	mergedWant := NewBlockFromPB(&mergedPB).ID()
	mergedGot, err := BlockIDFromRaw(mergedWire)
	if err != nil {
		t.Fatal(err)
	}
	if mergedGot != mergedWant {
		t.Fatalf("merged BlockIDFromRaw() = %+v, want %+v", mergedGot, mergedWant)
	}
}

func TestBlockIDFromRawRejectsMalformedHeader(t *testing.T) {
	wrongWire := protowire.AppendTag(nil, 2, protowire.VarintType)
	wrongWire = protowire.AppendVarint(wrongWire, 1)
	if _, err := BlockIDFromRaw(wrongWire); err == nil {
		t.Fatal("BlockIDFromRaw accepted a block_header with the wrong wire type")
	}

	missingRaw := protowire.AppendTag(nil, 2, protowire.BytesType)
	missingRaw = protowire.AppendBytes(missingRaw, nil)
	if _, err := BlockIDFromRaw(missingRaw); err == nil {
		t.Fatal("BlockIDFromRaw accepted a block_header without raw_data")
	}
}

func blockDecodeReserveTestBlock(txCount int) *Block {
	block := blockHashRawTestBlock(txCount, 64)
	for i, tx := range block.Proto().Transactions {
		tx.RawData.Data = bytes.Repeat([]byte{byte(i + 1)}, 20)
		tx.RawData.Contract = []*corepb.Transaction_Contract{{
			Type: corepb.Transaction_Contract_TransferContract,
			Parameter: &anypb.Any{
				TypeUrl: "type.googleapis.com/protocol.TransferContract",
				Value:   bytes.Repeat([]byte{byte(i)}, 64),
			},
			PermissionId: int32(i % 3),
		}}
		tx.Ret = []*corepb.Transaction_Result{{
			Fee:         int64(i + 1),
			ContractRet: corepb.Transaction_Result_SUCCESS,
		}}
	}
	return block
}

func TestCanonicalAnyTypeURLRegistry(t *testing.T) {
	const url = "type.googleapis.com/protocol.TransferContract"
	if got := canonicalAnyTypeURLs[url]; got != url {
		t.Fatalf("canonical Any URL not interned: got %q", got)
	}
}

func FuzzUnmarshalTransactionContractReservedEquivalent(f *testing.F) {
	canonical, err := proto.Marshal(&corepb.Transaction_Contract{
		Type: corepb.Transaction_Contract_TriggerSmartContract,
		Parameter: &anypb.Any{
			TypeUrl: "type.googleapis.com/protocol.TriggerSmartContract",
			Value:   []byte{1, 2, 3, 4},
		},
		Provider:     []byte("provider"),
		ContractName: []byte("contract"),
		PermissionId: 2,
	})
	if err != nil {
		f.Fatal(err)
	}
	duplicateAny, err := proto.Marshal(&anypb.Any{
		TypeUrl: "custom.example/protocol.TriggerSmartContract",
		Value:   []byte{5, 6},
	})
	if err != nil {
		f.Fatal(err)
	}
	duplicate := append(bytes.Clone(canonical), protowire.AppendTag(nil, 2, protowire.BytesType)...)
	duplicate = protowire.AppendBytes(duplicate, duplicateAny)
	unknown := append(bytes.Clone(canonical), protowire.AppendTag(nil, 100, protowire.Fixed32Type)...)
	unknown = protowire.AppendFixed32(unknown, 0x12345678)
	invalidTypeURL := protowire.AppendTag(nil, 2, protowire.BytesType)
	invalidAny := protowire.AppendTag(nil, 1, protowire.BytesType)
	invalidAny = protowire.AppendBytes(invalidAny, []byte{0xff})
	invalidTypeURL = protowire.AppendBytes(invalidTypeURL, invalidAny)
	f.Add([]byte(nil))
	f.Add(canonical)
	f.Add(duplicate)
	f.Add(unknown)
	f.Add(invalidTypeURL)
	f.Fuzz(func(t *testing.T, data []byte) {
		var got corepb.Transaction_Contract
		var inline anypb.Any
		gotErr := unmarshalTransactionContractReserved(data, &got, &inline)
		var want corepb.Transaction_Contract
		wantErr := proto.Unmarshal(data, &want)
		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("error mismatch: reserved=%v generated=%v, wire=%x", gotErr, wantErr, data)
		}
		if gotErr == nil && !proto.Equal(&got, &want) {
			t.Fatalf("decode mismatch: reserved=%v generated=%v, wire=%x", &got, &want, data)
		}
	})
}

func BenchmarkUnmarshalBlockReserved(b *testing.B) {
	data, err := blockDecodeReserveTestBlock(200).Marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.Run("generated", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			decoded := new(corepb.Block)
			if err := proto.Unmarshal(data, decoded); err != nil {
				b.Fatal(err)
			}
			benchmarkDecodedProtoBlock = decoded
		}
	})
	b.Run("reserved", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkDecodedBlock, err = UnmarshalBlock(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("borrowed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkDecodedBlock, err = UnmarshalBlockBorrowed(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkUnmarshalTransactionContractReserved(b *testing.B) {
	data, err := proto.Marshal(&corepb.Transaction_Contract{
		Type: corepb.Transaction_Contract_TriggerSmartContract,
		Parameter: &anypb.Any{
			TypeUrl: "type.googleapis.com/protocol.TriggerSmartContract",
			Value:   bytes.Repeat([]byte{0xaa}, 196),
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.Run("generated", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decoded := new(decodedWireTransaction)
			decoded.contract.Parameter = &decoded.parameter
			if err := blockMergeUnmarshal.Unmarshal(data, &decoded.contract); err != nil {
				b.Fatal(err)
			}
			benchmarkDecodedContract = &decoded.contract
		}
	})
	b.Run("interned-type-url", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decoded := new(decodedWireTransaction)
			if err := unmarshalTransactionContractReserved(data, &decoded.contract, &decoded.parameter); err != nil {
				b.Fatal(err)
			}
			benchmarkDecodedContract = &decoded.contract
		}
	})
}

func TestUnmarshalBlockReservedMatchesGenerated(t *testing.T) {
	if !blockDecodeReserveLayoutOK {
		t.Fatal("protobuf layout guard unexpectedly disabled reserved decoder")
	}
	want := blockDecodeReserveTestBlock(4).Proto()
	data, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	unknown := protowire.AppendTag(nil, 100, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, []byte("block-unknown"))
	data = append(data, unknown...)

	got, err := unmarshalBlockReserved(data)
	if err != nil {
		t.Fatal(err)
	}
	var generated corepb.Block
	if err := proto.Unmarshal(data, &generated); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.Proto(), &generated) {
		t.Fatalf("reserved decoder differs from generated decoder\nreserved: %v\ngenerated: %v", got.Proto(), &generated)
	}
	for i, tx := range got.Proto().Transactions {
		if cap(tx.Signature) != 1 || cap(tx.Ret) != 1 || cap(tx.RawData.Contract) != 1 {
			t.Fatalf("transaction %d did not retain inline slots: signature=%d result=%d contract=%d", i, cap(tx.Signature), cap(tx.Ret), cap(tx.RawData.Contract))
		}
	}
}

func TestUnmarshalBlockReservedKeepsMultipleParametersIndependent(t *testing.T) {
	block := blockDecodeReserveTestBlock(1).Proto()
	first := block.Transactions[0].RawData.Contract[0]
	second := proto.Clone(first).(*corepb.Transaction_Contract)
	second.Type = corepb.Transaction_Contract_TriggerSmartContract
	second.Parameter.TypeUrl = "type.googleapis.com/protocol.TriggerSmartContract"
	second.Parameter.Value = []byte{9, 8, 7}
	block.Transactions[0].RawData.Contract = append(block.Transactions[0].RawData.Contract, second)
	wire, err := proto.Marshal(block)
	if err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalBlockReserved(wire)
	if err != nil {
		t.Fatal(err)
	}
	contracts := got.Proto().Transactions[0].RawData.Contract
	if len(contracts) != 2 || contracts[0].Parameter == contracts[1].Parameter {
		t.Fatalf("decoded contract parameters alias: %+v", contracts)
	}
	if cap(contracts[0].Parameter.Value) != len(contracts[0].Parameter.Value) ||
		cap(contracts[1].Parameter.Value) != len(contracts[1].Parameter.Value) {
		t.Fatal("decoded Any values expose adjacent arena capacity")
	}
	if !proto.Equal(got.Proto(), block) {
		t.Fatalf("reserved decoder differs for repeated contracts\nreserved: %v\ngenerated: %v", got.Proto(), block)
	}
}

func TestUnmarshalBlockReservedOwnsByteValues(t *testing.T) {
	block := blockDecodeReserveTestBlock(3)
	for i, transaction := range block.Proto().Transactions {
		transaction.RawData.Scripts = []byte{byte(i + 11), byte(i + 12)}
		transaction.RawData.Contract[0].Provider = []byte{byte(i + 21), byte(i + 22)}
		transaction.RawData.Contract[0].ContractName = []byte{byte(i + 31), byte(i + 32)}
	}
	wire, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalBlockReserved(wire)
	if err != nil {
		t.Fatal(err)
	}
	wantBlock := proto.Clone(got.Proto()).(*corepb.Block)
	want := make([][]byte, len(got.Proto().Transactions))
	wantData := make([][]byte, len(got.Proto().Transactions))
	wantScripts := make([][]byte, len(got.Proto().Transactions))
	for i, transaction := range got.Proto().Transactions {
		want[i] = bytes.Clone(transaction.RawData.Contract[0].Parameter.Value)
		wantData[i] = bytes.Clone(transaction.RawData.Data)
		wantScripts[i] = bytes.Clone(transaction.RawData.Scripts)
	}
	clear(wire)
	if !proto.Equal(got.Proto(), wantBlock) {
		t.Fatal("decoded block byte values alias the input wire buffer")
	}
	for i, transaction := range got.Proto().Transactions {
		for j, signature := range transaction.Signature {
			if cap(signature) != len(signature) {
				t.Fatalf("transaction %d signature %d exposes adjacent arena capacity", i, j)
			}
		}
		if value := transaction.RawData.Contract[0].Parameter.Value; !bytes.Equal(value, want[i]) {
			t.Fatalf("transaction %d Any value aliases wire input: got %x, want %x", i, value, want[i])
		}
		if !bytes.Equal(transaction.RawData.Data, wantData[i]) {
			t.Fatalf("transaction %d raw data aliases wire input: got %x, want %x", i, transaction.RawData.Data, wantData[i])
		}
		if cap(transaction.RawData.Data) != len(transaction.RawData.Data) {
			t.Fatalf("transaction %d raw data exposes adjacent arena capacity", i)
		}
		if !bytes.Equal(transaction.RawData.Scripts, wantScripts[i]) {
			t.Fatalf("transaction %d raw scripts alias wire input: got %x, want %x", i, transaction.RawData.Scripts, wantScripts[i])
		}
		if cap(transaction.RawData.Scripts) != len(transaction.RawData.Scripts) {
			t.Fatalf("transaction %d raw scripts expose adjacent arena capacity", i)
		}
		contract := transaction.RawData.Contract[0]
		if cap(contract.Provider) != len(contract.Provider) || cap(contract.ContractName) != len(contract.ContractName) {
			t.Fatalf("transaction %d contract metadata exposes adjacent arena capacity", i)
		}
	}
}

func TestUnmarshalBlockBorrowedAliasesImmutableByteValues(t *testing.T) {
	block := blockDecodeReserveTestBlock(1)
	tx := block.Proto().Transactions[0]
	tx.RawData.Data = []byte("unique-borrowed-raw-data")
	tx.RawData.Scripts = []byte("unique-borrowed-script-data")
	tx.Signature = [][]byte{[]byte("unique-borrowed-signature-value")}
	tx.RawData.Contract[0].Provider = []byte("unique-borrowed-provider-value")
	tx.RawData.Contract[0].ContractName = []byte("unique-borrowed-contract-name")
	tx.RawData.Contract[0].Parameter.Value = []byte("unique-borrowed-contract-value")
	wire, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalBlockBorrowed(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.Proto(), block.Proto()) {
		t.Fatalf("borrowed decoder differs from source\nborrowed: %v\nsource: %v", got.Proto(), block.Proto())
	}
	if len(got.wireBacking) != len(wire) {
		t.Fatalf("borrowed wire backing = %d bytes, want %d", len(got.wireBacking), len(wire))
	}

	decoded := got.Proto().Transactions[0]
	for name, value := range map[string][]byte{
		"signature":      decoded.Signature[0],
		"raw data":       decoded.RawData.Data,
		"raw scripts":    decoded.RawData.Scripts,
		"provider":       decoded.RawData.Contract[0].Provider,
		"contract name":  decoded.RawData.Contract[0].ContractName,
		"contract value": decoded.RawData.Contract[0].Parameter.Value,
	} {
		if cap(value) != len(value) {
			t.Fatalf("%s exposes borrowed adjacent capacity: len=%d cap=%d", name, len(value), cap(value))
		}
		if bytes.Count(wire, value) != 1 {
			t.Fatalf("%s marker occurs %d times in wire, want 1", name, bytes.Count(wire, value))
		}
		offset := bytes.Index(wire, value)
		original := value[0]
		wire[offset] ^= 0xff
		if value[0] != original^0xff {
			t.Fatalf("%s does not alias immutable wire input", name)
		}
		wire[offset] = original
	}
}

func TestBlockTransactionsBorrowImmutableLargeTriggerData(t *testing.T) {
	data := bytes.Repeat([]byte{0xab}, triggerDataInlineSize+64)
	parameter, err := anypb.New(&contractpb.TriggerSmartContract{
		OwnerAddress:    bytes.Repeat([]byte{0x41}, common.AddressLength),
		ContractAddress: bytes.Repeat([]byte{0x42}, common.AddressLength),
		Data:            data,
	})
	if err != nil {
		t.Fatal(err)
	}
	block := NewBlockFromPB(&corepb.Block{Transactions: []*corepb.Transaction{{
		RawData: &corepb.TransactionRaw{Contract: []*corepb.Transaction_Contract{{
			Type:      corepb.Transaction_Contract_TriggerSmartContract,
			Parameter: parameter,
		}}},
	}}})
	message, err := block.Transactions()[0].DecodedContract()
	if err != nil {
		t.Fatal(err)
	}
	got := message.(*contractpb.TriggerSmartContract)
	var wireData []byte
	for wire := parameter.Value; len(wire) != 0; {
		fieldData := wire
		field, wireType, n := protowire.ConsumeField(fieldData)
		if n < 0 {
			t.Fatalf("malformed test parameter: %v", protowire.ParseError(n))
		}
		wire = wire[n:]
		if field == 4 && wireType == protowire.BytesType {
			wireData, _ = bytesFieldValue(fieldData[:n])
		}
	}
	if len(wireData) == 0 || len(got.Data) == 0 || &got.Data[0] != &wireData[0] {
		t.Fatal("block transaction did not borrow its immutable Any calldata")
	}
	if cap(got.Data) != len(got.Data) || !bytes.Equal(got.Data, data) {
		t.Fatalf("borrowed data = len %d cap %d value %x", len(got.Data), cap(got.Data), got.Data)
	}
}

func TestUnmarshalBlockReservedOwnsRawReferenceBytes(t *testing.T) {
	block := blockDecodeReserveTestBlock(2).Proto()
	block.Transactions[0].RawData.RefBlockBytes = []byte{0x11, 0x12}
	block.Transactions[0].RawData.RefBlockHash = []byte{0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a}
	// Exercise the independent-copy fallback alongside the canonical inline
	// storage. These unusual lengths remain protobuf-valid historical input.
	block.Transactions[1].RawData.RefBlockBytes = []byte{0x21, 0x22, 0x23}
	block.Transactions[1].RawData.RefBlockHash = []byte{0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c}
	wire, err := proto.Marshal(block)
	if err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalBlockReserved(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.Proto(), block) {
		t.Fatalf("reserved decoder differs for raw references\nreserved: %v\ngenerated: %v", got.Proto(), block)
	}
	want := proto.Clone(got.Proto()).(*corepb.Block)
	clear(wire)
	if !proto.Equal(got.Proto(), want) {
		t.Fatal("decoded raw reference bytes alias the input wire buffer")
	}
	txs := got.Proto().Transactions
	if cap(txs[0].RawData.RefBlockBytes) != len(txs[0].RawData.RefBlockBytes) ||
		cap(txs[0].RawData.RefBlockHash) != len(txs[0].RawData.RefBlockHash) {
		t.Fatal("canonical raw reference fields expose spare inline capacity")
	}
	txs[0].RawData.RefBlockBytes[0] ^= 0xff
	txs[0].RawData.RefBlockHash[0] ^= 0xff
	if !bytes.Equal(txs[1].RawData.RefBlockBytes, want.Transactions[1].RawData.RefBlockBytes) ||
		!bytes.Equal(txs[1].RawData.RefBlockHash, want.Transactions[1].RawData.RefBlockHash) {
		t.Fatal("decoded raw reference fields alias across transactions")
	}
}

func TestUnmarshalBlockReservedKeepsSignaturesIndependent(t *testing.T) {
	block := blockDecodeReserveTestBlock(2).Proto()
	block.Transactions[0].Signature = [][]byte{
		bytes.Repeat([]byte{0x11}, 65),
		bytes.Repeat([]byte{0x22}, 66),
	}
	block.Transactions[1].Signature = [][]byte{bytes.Repeat([]byte{0x33}, 65)}
	wire, err := proto.Marshal(block)
	if err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalBlockReserved(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.Proto(), block) {
		t.Fatalf("reserved decoder differs for signatures\nreserved: %v\ngenerated: %v", got.Proto(), block)
	}
	signatures := got.Proto().Transactions
	signatures[0].Signature[0][0] = 0xff
	if signatures[0].Signature[1][0] != 0x22 || signatures[1].Signature[0][0] != 0x33 {
		t.Fatal("decoded transaction signatures alias")
	}
}

func FuzzUnmarshalBlockReservedEquivalent(f *testing.F) {
	canonical, err := blockDecodeReserveTestBlock(2).Marshal()
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte(nil))
	f.Add(canonical)
	f.Add(append(bytes.Clone(canonical), protowire.AppendTag(nil, 100, protowire.Fixed32Type)...))
	// A field number above protowire.MaxValidNumber. ConsumeField accepts it,
	// while protobuf's generated decoder correctly rejects it.
	f.Add([]byte{0xe0, 0xe0, 0xe0, 0xe0, 0x30, 0x30})
	f.Fuzz(func(t *testing.T, data []byte) {
		got, gotErr := unmarshalBlockReserved(data)
		var want corepb.Block
		wantErr := proto.Unmarshal(data, &want)
		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("error mismatch: reserved=%v generated=%v, wire=%x", gotErr, wantErr, data)
		}
		if gotErr == nil && !proto.Equal(got.Proto(), &want) {
			t.Fatalf("decode mismatch: reserved=%v generated=%v, wire=%x", got.Proto(), &want, data)
		}
	})
}

func BenchmarkBlockMarshalReusable(b *testing.B) {
	block := blockHashRawTestBlock(200, 256)
	raw, err := block.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(raw)))
	b.Run("fresh", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkBlockBytes, err = block.Marshal()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("owned-scratch", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			block.AdoptMarshalScratch(raw)
			benchmarkBlockBytes, err = block.MarshalReusable()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestNewBlock(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    100,
				Timestamp: 1000000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	if b.Number() != 100 {
		t.Fatalf("expected number 100, got %d", b.Number())
	}
	if b.Timestamp() != 1000000 {
		t.Fatalf("expected timestamp 1000000, got %d", b.Timestamp())
	}
}

func TestBlockHash(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	h := b.Hash()
	if h.IsEmpty() {
		t.Fatal("hash should not be empty")
	}
	h2 := b.Hash()
	if h != h2 {
		t.Fatal("hash not deterministic")
	}
}

func TestBlockHashFromRawMatchesBlockHash(t *testing.T) {
	block := blockHashRawTestBlock(8, 64)
	data, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := BlockHashFromRaw(data)
	if err != nil {
		t.Fatalf("BlockHashFromRaw: %v", err)
	}
	if want := block.Hash(); got != want {
		t.Fatalf("raw block hash = %x, want %x", got, want)
	}
}

func TestBlockHashFromRawRejectsMalformedOrMissingHeader(t *testing.T) {
	wrongWire := protowire.AppendTag(nil, 2, protowire.VarintType)
	wrongWire = protowire.AppendVarint(wrongWire, 1)
	missingRaw, err := proto.Marshal(&corepb.Block{BlockHeader: &corepb.BlockHeader{}})
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"empty":       nil,
		"truncated":   {0x12, 0x80},
		"wrong-wire":  wrongWire,
		"missing-raw": missingRaw,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BlockHashFromRaw(data); err == nil {
				t.Fatal("expected malformed/missing header error")
			}
		})
	}
}

func TestUnmarshalBlockPreservesPrePQLegacyFieldCollision(t *testing.T) {
	// Exact transaction wire from mainnet block 10,476,461, transaction 33.
	// Its field 6 contains a legacy 32-byte value which is not a valid
	// PQAuthSig submessage. java-tron's pre-PQ schema retains it as unknown.
	txWire, err := hex.DecodeString(
		"0a95010a02db92220835f4db4ae8bd08d940a0b898c2b92d5a71081f126d" +
			"0a31747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c" +
			"2e54726967676572536d617274436f6e747261637412380a1541bf2e397b" +
			"1c8ac1d0893e217eb9daeed5fe01b0ee121541b3a9835ce8d9a67a44dea9" +
			"a9078ada92154a889e18c0843d2204d0e30db0709af794c2b92d788094eb" +
			"dc031241cc1e75aba66a874cd19477b9cb3eb4e862fa1a9712b0e3b6e801" +
			"fa2cdcd7127d25f46b9902f79d3309215de9d013bb06140071e5047066f5" +
			"c59b628e860dcfdc012a02180a3220c938c250417b4ed60e484176026717" +
			"9a163436290db817973146aae87bc8af283a95010a02db92220835f4db4ae8" +
			"bd08d940a0b898c2b92d5a71081f126d0a31747970652e676f6f676c6561" +
			"7069732e636f6d2f70726f746f636f6c2e54726967676572536d61727443" +
			"6f6e747261637412380a1541bf2e397b1c8ac1d0893e217eb9daeed5fe01" +
			"b0ee121541b3a9835ce8d9a67a44dea9a9078ada92154a889e18c0843d22" +
			"04d0e30db0709af794c2b92d788094ebdc03")
	if err != nil {
		t.Fatal(err)
	}
	raw := rawBlockWithTransaction(t, txWire, 10_476_461, 8)
	var strict corepb.Block
	if err := proto.Unmarshal(raw, &strict); err == nil {
		t.Fatal("PQ-aware protobuf unexpectedly accepted legacy field collision")
	}

	block, err := UnmarshalBlock(raw)
	if err != nil {
		t.Fatalf("compat unmarshal: %v", err)
	}
	if got := block.Number(); got != 10_476_461 {
		t.Fatalf("block number = %d", got)
	}
	tx := block.Proto().GetTransactions()[0]
	if got := len(tx.GetPqAuthSig()); got != 0 {
		t.Fatalf("legacy field decoded as %d PQ signatures", got)
	}
	legacyOffset := bytes.Index(txWire, []byte{0x32, 0x20, 0xc9, 0x38})
	if legacyOffset < 0 {
		t.Fatal("legacy fields missing from fixture")
	}
	legacyFields := txWire[legacyOffset:]
	if got := tx.ProtoReflect().GetUnknown(); !bytes.Equal(got, legacyFields) {
		t.Fatalf("unknown fields = %x, want %x", got, legacyFields)
	}
	roundTrip, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundTrip, raw) {
		t.Fatal("compat decode did not preserve historical block wire bytes")
	}
	if _, err := UnmarshalBlock(roundTrip); err != nil {
		t.Fatalf("compat round-trip no longer decodes: %v", err)
	}
	owned, err := UnmarshalBlockOwned(bytes.Clone(raw))
	if err != nil {
		t.Fatalf("owned compat unmarshal: %v", err)
	}
	reused, err := owned.MarshalReusable()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reused, raw) {
		t.Fatal("owned scratch round trip changed historical block wire bytes")
	}
}

func TestUnmarshalBlockRejectsMalformedPQAtPQVersion(t *testing.T) {
	tx := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 1}}
	txWire, err := proto.Marshal(tx)
	if err != nil {
		t.Fatal(err)
	}
	txWire = protowire.AppendTag(txWire, 6, protowire.BytesType)
	txWire = protowire.AppendBytes(txWire, []byte{0xff})
	raw := rawBlockWithTransaction(t, txWire, 1, int32(firstPQBlockVersion))
	if _, err := UnmarshalBlock(raw); err == nil {
		t.Fatal("PQ-version block accepted malformed PQAuthSig")
	}
}

func TestUnmarshalBlockPreservesPrePQHeaderFieldCollision(t *testing.T) {
	header := &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 2, Version: 36}}
	headerWire, err := proto.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	legacyField := protowire.AppendTag(nil, 3, protowire.BytesType)
	legacyField = protowire.AppendBytes(legacyField, []byte{0xff})
	headerWire = append(headerWire, legacyField...)
	raw := protowire.AppendTag(nil, 2, protowire.BytesType)
	raw = protowire.AppendBytes(raw, headerWire)

	block, err := UnmarshalBlock(raw)
	if err != nil {
		t.Fatalf("compat unmarshal: %v", err)
	}
	if got := block.Proto().GetBlockHeader().ProtoReflect().GetUnknown(); !bytes.Equal(got, legacyField) {
		t.Fatalf("header unknown = %x, want %x", got, legacyField)
	}
	roundTrip, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundTrip, raw) {
		t.Fatal("header collision wire changed after round trip")
	}
}

func rawBlockWithTransaction(t *testing.T, txWire []byte, number int64, version int32) []byte {
	t.Helper()
	headerWire, err := proto.Marshal(&corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
		Number: number, Version: version,
	}})
	if err != nil {
		t.Fatal(err)
	}
	raw := protowire.AppendTag(nil, 1, protowire.BytesType)
	raw = protowire.AppendBytes(raw, txWire)
	raw = protowire.AppendTag(raw, 2, protowire.BytesType)
	return protowire.AppendBytes(raw, headerWire)
}

func TestBlockSerialize(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    42,
				Timestamp: 9000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	data, err := b.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	b2, err := UnmarshalBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	if b2.Number() != 42 {
		t.Fatalf("expected 42, got %d", b2.Number())
	}
}

func TestBlockID(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number: 5,
			},
		},
	}
	b := NewBlockFromPB(pb)
	id := b.ID()
	num := id.Number()
	if num != 5 {
		t.Fatalf("expected block number 5 from ID, got %d", num)
	}
}

func TestBlockParentHash(t *testing.T) {
	parent := common.HexToHash("aabbccdd")
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				ParentHash: parent.Bytes(),
			},
		},
	}
	b := NewBlockFromPB(pb)
	if b.ParentHash() != parent {
		t.Fatal("parent hash mismatch")
	}
}

func TestBlockProtoRoundTrip(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         999,
				Timestamp:      123456789,
				WitnessAddress: []byte{0x41, 0x01, 0x02},
				Version:        34,
			},
		},
	}
	b := NewBlockFromPB(pb)
	pb2 := b.Proto()
	if !proto.Equal(pb, pb2) {
		t.Fatal("proto round trip not equal")
	}
}

func TestBlock_SetWitnessSignature(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000},
		},
	})

	sig := make([]byte, 65)
	sig[0] = 0xAA
	block.SetWitnessSignature(sig)

	if got := block.WitnessSignature(); len(got) != 65 || got[0] != 0xAA {
		t.Fatalf("unexpected signature: %x", got)
	}
}

func TestBlock_SetAccountStateRoot(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1},
		},
	})

	var root common.Hash
	root[0] = 0xBB
	block.SetAccountStateRoot(root)

	if block.AccountStateRoot() != root {
		t.Fatalf("expected root %x, got %x", root, block.AccountStateRoot())
	}
}

func TestBlock_ResetHash(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000},
		},
	})

	hash1 := block.Hash()

	block.Proto().BlockHeader.RawData.Timestamp = 6000
	if block.Hash() != hash1 {
		t.Fatal("hash should be cached")
	}

	block.ResetHash()
	hash2 := block.Hash()
	if hash2 == hash1 {
		t.Fatal("hash should change after ResetHash + modified RawData")
	}
}

// TestBlock_TransactionsAreStable verifies Transactions() memoizes the wrapped
// slice and returns the SAME *Transaction instances every call. This identity
// is what lets the parallel pre-pass warm a tx's signers memo and have the
// serial execution path (which re-fetches via Transactions()) read the warm
// result.
func TestBlock_TransactionsAreStable(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000}},
		Transactions: []*corepb.Transaction{
			{RawData: &corepb.TransactionRaw{Timestamp: 1}},
			{RawData: &corepb.TransactionRaw{Timestamp: 2}},
		},
	})
	a := block.Transactions()
	b := block.Transactions()
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("len: a=%d b=%d, want 2", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("Transactions()[%d] not stable: %p vs %p", i, a[i], b[i])
		}
		if a[i].Proto() != block.Proto().Transactions[i] {
			t.Fatalf("Transactions()[%d] wraps the wrong protobuf", i)
		}
	}
	if a[0] == a[1] {
		t.Fatal("distinct protobuf transactions share one wrapper")
	}
	if a[0].Hash() == a[1].Hash() {
		t.Fatal("distinct transaction wrappers share a hash cache")
	}
}

// TestBlock_CachedRecoveredWitness verifies the witness-recovery memo: the
// supplied recover func runs exactly once, the cached (addr, err) is returned
// thereafter, and SetWitnessSignature / ResetHash invalidate it so a re-signed
// block re-derives.
func TestBlock_CachedRecoveredWitness(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000}},
	})
	var calls int
	want := common.Address{0x41, 0x07}
	rec := func(*Block) (common.Address, error) { calls++; return want, nil }

	if got, _ := block.CachedRecoveredWitness(rec); got != want {
		t.Fatalf("addr = %x, want %x", got, want)
	}
	if got, _ := block.CachedRecoveredWitness(rec); got != want {
		t.Fatalf("cached addr = %x, want %x", got, want)
	}
	if calls != 1 {
		t.Fatalf("recover called %d times, want 1 (memoized)", calls)
	}

	// SetWitnessSignature must invalidate the memo (re-sign re-derives).
	block.SetWitnessSignature(make([]byte, 65))
	if _, _ = block.CachedRecoveredWitness(rec); calls != 2 {
		t.Fatalf("recover called %d times after SetWitnessSignature, want 2", calls)
	}

	// ResetHash must invalidate too.
	block.ResetHash()
	if _, _ = block.CachedRecoveredWitness(rec); calls != 3 {
		t.Fatalf("recover called %d times after ResetHash, want 3", calls)
	}
}
