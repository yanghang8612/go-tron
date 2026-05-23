package state

import (
	"encoding/binary"
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func (s *StateDB) readSystemReward(key []byte) ([]byte, bool) {
	raw, ok, err := s.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, key)
	if err != nil || !ok {
		return nil, false
	}
	return raw, true
}

func (s *StateDB) writeSystemReward(key, value []byte) error {
	return s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, value)
}

func (s *StateDB) ReadCycleReward(cycle int64, addr []byte) int64 {
	raw, ok := s.readSystemReward(rawdb.CycleRewardStateKey(cycle, addr))
	if !ok || len(raw) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(raw))
}

func (s *StateDB) WriteCycleReward(cycle int64, addr []byte, value int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	return s.writeSystemReward(rawdb.CycleRewardStateKey(cycle, addr), buf[:])
}

func (s *StateDB) AddCycleReward(cycle int64, addr []byte, delta int64) error {
	return s.WriteCycleReward(cycle, addr, s.ReadCycleReward(cycle, addr)+delta)
}

func (s *StateDB) ReadCycleVote(cycle int64, addr []byte) int64 {
	raw, ok := s.readSystemReward(rawdb.CycleVoteStateKey(cycle, addr))
	if !ok || len(raw) != 8 {
		return rawdb.RewardRemark
	}
	return int64(binary.BigEndian.Uint64(raw))
}

func (s *StateDB) WriteCycleVote(cycle int64, addr []byte, value int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	return s.writeSystemReward(rawdb.CycleVoteStateKey(cycle, addr), buf[:])
}

func (s *StateDB) ReadWitnessVI(cycle int64, addr []byte) *big.Int {
	raw, ok := s.readSystemReward(rawdb.WitnessVIStateKey(cycle, addr))
	if !ok || len(raw) == 0 {
		return new(big.Int)
	}
	return new(big.Int).SetBytes(raw)
}

func (s *StateDB) WriteWitnessVI(cycle int64, addr []byte, vi *big.Int) error {
	if vi == nil {
		vi = new(big.Int)
	}
	return s.writeSystemReward(rawdb.WitnessVIStateKey(cycle, addr), vi.Bytes())
}

func (s *StateDB) ReadCycleBrokerage(cycle int64, addr []byte) int {
	raw, ok := s.readSystemReward(rawdb.CycleBrokerageStateKey(cycle, addr))
	if !ok || len(raw) != 4 {
		return rawdb.DefaultBrokerage
	}
	return int(int32(binary.BigEndian.Uint32(raw)))
}

func (s *StateDB) WriteCycleBrokerage(cycle int64, addr []byte, rate int) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(int32(rate)))
	return s.writeSystemReward(rawdb.CycleBrokerageStateKey(cycle, addr), buf[:])
}

func (s *StateDB) ReadCycleAccountVote(cycle int64, addr []byte) []byte {
	raw, ok := s.readSystemReward(rawdb.CycleAccountVoteStateKey(cycle, addr))
	if !ok || len(raw) == 0 {
		return nil
	}
	return raw
}

func (s *StateDB) WriteCycleAccountVote(cycle int64, addr, proto []byte) error {
	return s.writeSystemReward(rawdb.CycleAccountVoteStateKey(cycle, addr), proto)
}

func (s *StateDB) ReadBeginCycle(addr []byte) int64 {
	raw, ok := s.readSystemReward(rawdb.BeginCycleStateKey(addr))
	if !ok || len(raw) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(raw))
}

func (s *StateDB) WriteBeginCycle(addr []byte, cycle int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(cycle))
	return s.writeSystemReward(rawdb.BeginCycleStateKey(addr), buf[:])
}

func (s *StateDB) ReadEndCycle(addr []byte) int64 {
	raw, ok := s.readSystemReward(rawdb.EndCycleStateKey(addr))
	if !ok || len(raw) != 8 {
		return rawdb.RewardRemark
	}
	return int64(binary.BigEndian.Uint64(raw))
}

func (s *StateDB) WriteEndCycle(addr []byte, cycle int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(cycle))
	return s.writeSystemReward(rawdb.EndCycleStateKey(addr), buf[:])
}
