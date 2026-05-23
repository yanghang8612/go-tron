package state

import (
	"encoding/binary"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// Phase 3c roots the global witness schedule into the reserved system account's
// SystemWitnessSchedule KV so it rewinds with the full state root. Two keys live
// in the domain:
//
//   - witnessScheduleActiveKey: the active (current-maintenance) witness list,
//     java-tron's WitnessScheduleStore.active_witnesses.
//   - witnessScheduleIndexKey:  the enumeration of every registered witness,
//     iterated at maintenance/reward time and grown by WitnessCreateActuator.
//
// Per-witness capsules (vote counts, is_jobs, URL) are rooted separately under
// the witness-owned WitnessCapsule domain via the RootedStore legacy view.
// The shuffled witness schedule is reserved for this domain too.
//
// The value encoding reuses the existing on-disk wire format verbatim —
// 4-byte big-endian count followed by N×21-byte addresses — so no new encoding
// lineage is introduced.
var (
	witnessScheduleActiveKey = []byte("ActiveWitnesses")
	witnessScheduleIndexKey  = []byte("WitnessIndex")
)

// encodeAddressList packs addresses as 4-byte BE count || N×AddressLength bytes.
func encodeAddressList(addrs []tcommon.Address) []byte {
	buf := make([]byte, 4+len(addrs)*tcommon.AddressLength)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(addrs)))
	for i, a := range addrs {
		copy(buf[4+i*tcommon.AddressLength:], a.Bytes())
	}
	return buf
}

// decodeAddressList reverses encodeAddressList. Malformed/short data → nil,
// matching the prior rawdb reader's defensive behavior.
func decodeAddressList(data []byte) []tcommon.Address {
	if len(data) < 4 {
		return nil
	}
	count := int(binary.BigEndian.Uint32(data[:4]))
	if count == 0 || len(data) < 4+count*tcommon.AddressLength {
		return nil
	}
	out := make([]tcommon.Address, count)
	for i := 0; i < count; i++ {
		out[i] = tcommon.BytesToAddress(data[4+i*tcommon.AddressLength : 4+(i+1)*tcommon.AddressLength])
	}
	return out
}

// readAddressList resolves a witness-schedule key, propagating the KV error so
// callers that do read-modify-write (AppendWitnessIndex) never truncate on a
// transient trie error.
func (s *StateDB) readAddressList(key []byte) ([]tcommon.Address, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemWitnessSchedule, key)
	if err != nil || !ok {
		return nil, err
	}
	return decodeAddressList(raw), nil
}

// ReadActiveWitnesses returns the rooted active witness list (nil if unset). A
// KV error is swallowed to nil, matching the prior rawdb reader and 3b's Load.
func (s *StateDB) ReadActiveWitnesses() []tcommon.Address {
	v, _ := s.readAddressList(witnessScheduleActiveKey)
	return v
}

// WriteActiveWitnesses stages the active witness list into the system-KV. The
// error is non-nil only for an unregistered domain (a programmer error), since
// SystemWitnessSchedule is registered at init.
func (s *StateDB) WriteActiveWitnesses(addrs []tcommon.Address) error {
	return s.SystemKVPut(kvdomains.SystemWitnessSchedule, witnessScheduleActiveKey, encodeAddressList(addrs))
}

// ReadWitnessIndex returns the rooted witness index (nil if unset). KV error
// swallowed to nil — drop-in for the prior rawdb reader's consumers.
func (s *StateDB) ReadWitnessIndex() []tcommon.Address {
	v, _ := s.readAddressList(witnessScheduleIndexKey)
	return v
}

// WriteWitnessIndex stages the full witness index into the system-KV.
func (s *StateDB) WriteWitnessIndex(addrs []tcommon.Address) error {
	return s.SystemKVPut(kvdomains.SystemWitnessSchedule, witnessScheduleIndexKey, encodeAddressList(addrs))
}

// AppendWitnessIndex adds addr to the witness index if absent. The read error is
// propagated (not swallowed) so a transient trie failure aborts the append
// instead of overwriting the index with a truncated list.
func (s *StateDB) AppendWitnessIndex(addr tcommon.Address) error {
	existing, err := s.readAddressList(witnessScheduleIndexKey)
	if err != nil {
		return err
	}
	for _, a := range existing {
		if a == addr {
			return nil
		}
	}
	return s.WriteWitnessIndex(append(existing, addr))
}
