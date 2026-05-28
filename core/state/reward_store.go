package state

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"sort"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// CycleRewardSink can intercept block-final writes to the current-cycle reward
// pool. BlockChain uses it to batch the hot dl-<cycle>-<witness>-reward keys
// outside the rooted SystemReward domain until the next maintenance boundary.
type CycleRewardSink interface {
	AddCycleReward(cycle int64, addr tcommon.Address, delta int64) (bool, error)
	AddCycleRewards(cycle int64, deltas map[tcommon.Address]int64) (bool, error)
	PendingCycleReward(cycle int64, addr tcommon.Address) (int64, bool)
}

func (s *StateDB) SetCycleRewardSink(sink CycleRewardSink) {
	s.cycleRewardSink = sink
}

func (s *StateDB) readSystemReward(key []byte) ([]byte, bool) {
	raw, ok, err := s.readSystemRewardWithError(key)
	if err != nil || !ok {
		return nil, false
	}
	return raw, true
}

func (s *StateDB) readSystemRewardWithError(key []byte) ([]byte, bool, error) {
	return s.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, key)
}

func (s *StateDB) writeSystemReward(key, value []byte) error {
	return s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, value)
}

func (s *StateDB) ReadCycleReward(cycle int64, addr []byte) int64 {
	raw, ok := s.readSystemReward(rawdb.CycleRewardStateKey(cycle, addr))
	base := int64(0)
	if !ok || len(raw) != 8 {
		base = 0
	} else {
		base = int64(binary.BigEndian.Uint64(raw))
	}
	if s.cycleRewardSink != nil {
		if pending, ok := s.cycleRewardSink.PendingCycleReward(cycle, tcommon.BytesToAddress(addr)); ok {
			base += pending
		}
	}
	return base
}

func (s *StateDB) ReadCycleRewards(cycle int64, addrs []tcommon.Address) map[tcommon.Address]int64 {
	keys := make([][]byte, 0, len(addrs))
	for _, addr := range addrs {
		keys = append(keys, rawdb.CycleRewardStateKey(cycle, addr.Bytes()))
	}
	values, err := s.GetAccountKVBatch(tcommon.SystemAccountAddress, kvdomains.SystemReward, keys)
	out := make(map[tcommon.Address]int64, len(addrs))
	if err != nil {
		return out
	}
	for i, addr := range addrs {
		raw := values[string(keys[i])]
		if len(raw) == 8 {
			out[addr] = int64(binary.BigEndian.Uint64(raw))
		}
		if s.cycleRewardSink != nil {
			if pending, ok := s.cycleRewardSink.PendingCycleReward(cycle, addr); ok {
				out[addr] += pending
			}
		}
	}
	return out
}

func (s *StateDB) WriteCycleReward(cycle int64, addr []byte, value int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	return s.writeSystemReward(rawdb.CycleRewardStateKey(cycle, addr), buf[:])
}

func (s *StateDB) WriteCycleRewardFinal(cycle int64, addr []byte, value int64) error {
	key := rawdb.CycleRewardStateKey(cycle, addr)
	raw, exists, err := s.readSystemRewardWithError(key)
	if err != nil {
		return err
	}
	return s.writeCycleRewardFinalWithPrev(key, raw, exists, value)
}

func (s *StateDB) writeCycleRewardFinalWithPrev(key, prev []byte, prevExists bool, value int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	return s.setAccountKVFinalWithPrev(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, prev, buf[:], prevExists)
}

func decodeCycleReward(raw []byte, exists bool) int64 {
	if !exists || len(raw) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(raw))
}

func (s *StateDB) AddCycleReward(cycle int64, addr []byte, delta int64) error {
	return s.WriteCycleReward(cycle, addr, s.ReadCycleReward(cycle, addr)+delta)
}

func (s *StateDB) AddCycleRewardFinal(cycle int64, addr []byte, delta int64) error {
	if s.cycleRewardSink != nil {
		handled, err := s.cycleRewardSink.AddCycleReward(cycle, tcommon.BytesToAddress(addr), delta)
		if err != nil || handled {
			return err
		}
	}
	key := rawdb.CycleRewardStateKey(cycle, addr)
	raw, exists, err := s.readSystemRewardWithError(key)
	if err != nil {
		return err
	}
	return s.writeCycleRewardFinalWithPrev(key, raw, exists, decodeCycleReward(raw, exists)+delta)
}

func (s *StateDB) AddCycleRewards(cycle int64, deltas map[tcommon.Address]int64) error {
	return s.addCycleRewards(cycle, deltas, false)
}

func (s *StateDB) AddCycleRewardsFinal(cycle int64, deltas map[tcommon.Address]int64) error {
	return s.addCycleRewards(cycle, deltas, true)
}

func (s *StateDB) addCycleRewards(cycle int64, deltas map[tcommon.Address]int64, final bool) error {
	if len(deltas) == 0 {
		return nil
	}
	if final && s.cycleRewardSink != nil {
		handled, err := s.cycleRewardSink.AddCycleRewards(cycle, deltas)
		if err != nil || handled {
			return err
		}
	}
	addrs := make([]tcommon.Address, 0, len(deltas))
	for addr, delta := range deltas {
		if delta != 0 {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		return nil
	}
	sort.Slice(addrs, func(i, j int) bool {
		return bytes.Compare(addrs[i].Bytes(), addrs[j].Bytes()) < 0
	})
	keys := make([][]byte, 0, len(addrs))
	for _, addr := range addrs {
		keys = append(keys, rawdb.CycleRewardStateKey(cycle, addr.Bytes()))
	}
	current, err := s.GetAccountKVBatch(tcommon.SystemAccountAddress, kvdomains.SystemReward, keys)
	if err != nil {
		return err
	}
	for i, addr := range addrs {
		var err error
		key := keys[i]
		raw, exists := current[string(key)]
		next := decodeCycleReward(raw, exists) + deltas[addr]
		if final {
			err = s.writeCycleRewardFinalWithPrev(key, raw, exists, next)
		} else {
			err = s.WriteCycleReward(cycle, addr.Bytes(), next)
		}
		if err != nil {
			return err
		}
	}
	return nil
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
