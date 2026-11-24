package tronapi

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var ErrWitnessMaintenance = errors.New("Service temporarily unavailable during maintenance period. Please try again later.")

func WitnessPage(witnesses []*WitnessInfo, offset, limit int64) []*WitnessInfo {
	if offset < 0 || limit <= 0 || offset >= int64(len(witnesses)) {
		return nil
	}
	limit = min(limit, 1000, int64(len(witnesses))-offset)
	return witnesses[offset : offset+limit]
}

func (w *WitnessInfo) Proto() *corepb.Witness {
	return &corepb.Witness{
		Address: common.FromHex(w.Address), PubKey: common.FromHex(w.PubKey),
		VoteCount: w.VoteCount, Url: w.URL, IsJobs: w.IsJobs,
		TotalProduced: w.TotalProduced, TotalMissed: w.TotalMissed,
		LatestBlockNum: w.LatestBlockNum, LatestSlotNum: w.LatestSlotNum,
	}
}
