package tronapi

import (
	"encoding/binary"
	"time"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const txExpirationSeconds = 60

// BuildTransaction creates an unsigned Transaction wrapping the given contract.
// Exported so core/tron_backend.go can use it.
func BuildTransaction(
	headBlockNum uint64,
	headBlockHash []byte,
	headBlockTimestamp int64,
	contractType corepb.Transaction_Contract_ContractType,
	contractMsg proto.Message,
	feeLimit int64,
) (*corepb.Transaction, error) {
	paramAny, err := anypb.New(contractMsg)
	if err != nil {
		return nil, err
	}

	numBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(numBytes, headBlockNum)
	refBlockBytes := numBytes[6:8]

	var refBlockHash []byte
	if len(headBlockHash) >= 16 {
		refBlockHash = headBlockHash[8:16]
	}

	now := time.Now().UnixMilli()
	expiration := headBlockTimestamp + txExpirationSeconds*1000

	rawData := &corepb.TransactionRaw{
		RefBlockBytes: refBlockBytes,
		RefBlockHash:  refBlockHash,
		Expiration:    expiration,
		Timestamp:     now,
		Contract: []*corepb.Transaction_Contract{{
			Type:      contractType,
			Parameter: paramAny,
		}},
	}

	if feeLimit > 0 {
		rawData.FeeLimit = feeLimit
	}

	return &corepb.Transaction{
		RawData: rawData,
	}, nil
}
