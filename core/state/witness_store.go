package state

import (
	"encoding/binary"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
)

func (s *StateDB) readWitnessCapsule(addr tcommon.Address) (*types.Witness, error) {
	raw, ok, err := s.GetAccountKV(addr, kvdomains.WitnessCapsule, rawdb.WitnessCapsuleStateKey(addr))
	if err != nil || !ok {
		return nil, err
	}
	w, err := types.UnmarshalWitness(raw)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// SetWitnessCapsule stages a full witness capsule in the witness-owned
// WitnessCapsule domain. It is the native typed replacement for writing the
// legacy w- compatibility key.
func (s *StateDB) SetWitnessCapsule(w *types.Witness) error {
	if w == nil {
		return nil
	}
	addr := w.Address()
	s.journalWitness(addr)
	s.witnesses[addr] = w.Copy()
	data, err := w.Marshal()
	if err != nil {
		return err
	}
	return s.SetAccountKV(addr, kvdomains.WitnessCapsule, rawdb.WitnessCapsuleStateKey(addr), data)
}

// ReadWitnessLatestBlock returns the native rooted latest-produced-block cursor
// for a witness.
func (s *StateDB) ReadWitnessLatestBlock(addr tcommon.Address) int64 {
	raw, ok, err := s.GetAccountKV(addr, kvdomains.WitnessCapsule, rawdb.WitnessLatestBlockStateKey(addr))
	if err != nil || !ok || len(raw) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(raw))
}

// WriteWitnessLatestBlock stages the native rooted latest-produced-block cursor.
func (s *StateDB) WriteWitnessLatestBlock(addr tcommon.Address, num int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(num))
	return s.setAccountKVFinalNoRead(addr, kvdomains.WitnessCapsule, rawdb.WitnessLatestBlockStateKey(addr), buf[:])
}

// ReadWitnessBrokerage returns the current brokerage rate set by
// UpdateBrokerage. Missing rows default to java-tron's DEFAULT_BROKERAGE (20).
func (s *StateDB) ReadWitnessBrokerage(addr tcommon.Address) int64 {
	raw, ok, err := s.GetAccountKV(addr, kvdomains.WitnessCapsule, rawdb.WitnessBrokerageStateKey(addr))
	if err != nil || !ok || len(raw) != 8 {
		return int64(rawdb.DefaultBrokerage)
	}
	return int64(binary.BigEndian.Uint64(raw))
}

// WriteWitnessBrokerage stages the current brokerage rate for a witness.
func (s *StateDB) WriteWitnessBrokerage(addr tcommon.Address, brokerage int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(brokerage))
	return s.SetAccountKV(addr, kvdomains.WitnessCapsule, rawdb.WitnessBrokerageStateKey(addr), buf[:])
}
