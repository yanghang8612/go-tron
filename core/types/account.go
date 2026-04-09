package types

import (
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type Account struct {
	pb *corepb.Account
}

func NewAccountFromPB(pb *corepb.Account) *Account {
	return &Account{pb: pb}
}

func NewAccount(addr common.Address, accType corepb.AccountType) *Account {
	return &Account{
		pb: &corepb.Account{
			Address: addr.Bytes(),
			Type:    accType,
		},
	}
}

func (a *Account) Proto() *corepb.Account  { return a.pb }
func (a *Account) Address() common.Address  { return common.BytesToAddress(a.pb.Address) }
func (a *Account) Balance() int64           { return a.pb.Balance }
func (a *Account) SetBalance(b int64)       { a.pb.Balance = b }
func (a *Account) Type() corepb.AccountType { return a.pb.Type }
func (a *Account) IsWitness() bool          { return a.pb.IsWitness }
func (a *Account) SetIsWitness(v bool)      { a.pb.IsWitness = v }
func (a *Account) CreateTime() int64        { return a.pb.CreateTime }
func (a *Account) SetCreateTime(t int64)    { a.pb.CreateTime = t }

func (a *Account) Marshal() ([]byte, error) {
	return proto.Marshal(a.pb)
}

func UnmarshalAccount(data []byte) (*Account, error) {
	pb := &corepb.Account{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	return NewAccountFromPB(pb), nil
}
