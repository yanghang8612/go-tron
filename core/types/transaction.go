package types

import (
	"crypto/sha256"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type Transaction struct {
	pb       *corepb.Transaction
	hash     common.Hash
	hashOnce sync.Once
}

func NewTransactionFromPB(pb *corepb.Transaction) *Transaction {
	return &Transaction{pb: pb}
}

func (tx *Transaction) Proto() *corepb.Transaction { return tx.pb }

func (tx *Transaction) Hash() common.Hash {
	tx.hashOnce.Do(func() {
		if tx.pb.RawData == nil {
			return
		}
		data, err := proto.Marshal(tx.pb.RawData)
		if err != nil {
			return
		}
		tx.hash = sha256.Sum256(data)
	})
	return tx.hash
}

func (tx *Transaction) ContractType() corepb.Transaction_Contract_ContractType {
	if tx.pb.RawData == nil || len(tx.pb.RawData.Contract) == 0 {
		return -1
	}
	return tx.pb.RawData.Contract[0].Type
}

func (tx *Transaction) Contract() *corepb.Transaction_Contract {
	if tx.pb.RawData == nil || len(tx.pb.RawData.Contract) == 0 {
		return nil
	}
	return tx.pb.RawData.Contract[0]
}

func (tx *Transaction) Timestamp() int64 {
	if tx.pb.RawData == nil {
		return 0
	}
	return tx.pb.RawData.Timestamp
}

func (tx *Transaction) Expiration() int64 {
	if tx.pb.RawData == nil {
		return 0
	}
	return tx.pb.RawData.Expiration
}

func (tx *Transaction) Signatures() [][]byte {
	return tx.pb.Signature
}

func (tx *Transaction) Size() int {
	return proto.Size(tx.pb)
}

func (tx *Transaction) Marshal() ([]byte, error) {
	return proto.Marshal(tx.pb)
}

func UnmarshalTransaction(data []byte) (*Transaction, error) {
	pb := &corepb.Transaction{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	return NewTransactionFromPB(pb), nil
}
