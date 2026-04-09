package types

import (
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type Witness struct {
	pb *corepb.Witness
}

func NewWitnessFromPB(pb *corepb.Witness) *Witness {
	return &Witness{pb: pb}
}

func NewWitness(addr common.Address, url string) *Witness {
	return &Witness{
		pb: &corepb.Witness{
			Address: addr.Bytes(),
			Url:     url,
		},
	}
}

func (w *Witness) Proto() *corepb.Witness    { return w.pb }
func (w *Witness) Address() common.Address    { return common.BytesToAddress(w.pb.Address) }
func (w *Witness) VoteCount() int64           { return w.pb.VoteCount }
func (w *Witness) SetVoteCount(v int64)       { w.pb.VoteCount = v }
func (w *Witness) URL() string                { return w.pb.Url }
func (w *Witness) TotalProduced() int64       { return w.pb.TotalProduced }
func (w *Witness) SetTotalProduced(v int64)   { w.pb.TotalProduced = v }
func (w *Witness) TotalMissed() int64         { return w.pb.TotalMissed }
func (w *Witness) SetTotalMissed(v int64)     { w.pb.TotalMissed = v }
func (w *Witness) IsJobs() bool               { return w.pb.IsJobs }
func (w *Witness) SetIsJobs(v bool)           { w.pb.IsJobs = v }

func (w *Witness) Marshal() ([]byte, error) {
	return proto.Marshal(w.pb)
}

func UnmarshalWitness(data []byte) (*Witness, error) {
	pb := &corepb.Witness{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	return NewWitnessFromPB(pb), nil
}
