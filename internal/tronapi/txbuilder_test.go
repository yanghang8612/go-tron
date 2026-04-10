package tronapi

import (
	"encoding/binary"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestBuildTransaction(t *testing.T) {
	headBlockNum := uint64(12345)
	headBlockHash := make([]byte, 32)
	for i := range headBlockHash {
		headBlockHash[i] = byte(i)
	}
	headBlockTimestamp := int64(1700000000000)

	tc := &contractpb.TransferContract{
		OwnerAddress: []byte{0x41, 0x01},
		ToAddress:    []byte{0x41, 0x02},
		Amount:       1000000,
	}

	tx, err := BuildTransaction(headBlockNum, headBlockHash, headBlockTimestamp,
		corepb.Transaction_Contract_TransferContract, tc, 0)
	if err != nil {
		t.Fatalf("BuildTransaction failed: %v", err)
	}

	raw := tx.RawData
	if raw == nil {
		t.Fatal("RawData is nil")
	}

	// ref_block_bytes: bytes 6..7 of block number (big-endian)
	numBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(numBytes, headBlockNum)
	if raw.RefBlockBytes[0] != numBytes[6] || raw.RefBlockBytes[1] != numBytes[7] {
		t.Fatalf("ref_block_bytes: got %x, want %x", raw.RefBlockBytes, numBytes[6:8])
	}

	// ref_block_hash: bytes 8..15 of block hash
	for i := 0; i < 8; i++ {
		if raw.RefBlockHash[i] != headBlockHash[8+i] {
			t.Fatalf("ref_block_hash mismatch at byte %d", i)
		}
	}

	// expiration
	expectedExp := headBlockTimestamp + txExpirationSeconds*1000
	if raw.Expiration != expectedExp {
		t.Fatalf("expiration: got %d, want %d", raw.Expiration, expectedExp)
	}

	// timestamp should be recent
	if raw.Timestamp == 0 {
		t.Fatal("timestamp is zero")
	}

	// contract
	if len(raw.Contract) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(raw.Contract))
	}
	if raw.Contract[0].Type != corepb.Transaction_Contract_TransferContract {
		t.Fatalf("wrong contract type: %v", raw.Contract[0].Type)
	}

	// fee_limit should be 0 (not set)
	if raw.FeeLimit != 0 {
		t.Fatalf("fee_limit should be 0, got %d", raw.FeeLimit)
	}
}

func TestBuildTransaction_WithFeeLimit(t *testing.T) {
	tc := &contractpb.TransferContract{
		OwnerAddress: []byte{0x41, 0x01},
		ToAddress:    []byte{0x41, 0x02},
		Amount:       100,
	}

	tx, err := BuildTransaction(100, make([]byte, 32), 1000,
		corepb.Transaction_Contract_TransferContract, tc, 50_000_000)
	if err != nil {
		t.Fatalf("BuildTransaction failed: %v", err)
	}
	if tx.RawData.FeeLimit != 50_000_000 {
		t.Fatalf("fee_limit: got %d, want 50000000", tx.RawData.FeeLimit)
	}
}
