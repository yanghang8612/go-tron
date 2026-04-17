package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// WriteShuffledWitnesses stores the per-maintenance-cycle shuffled block
// production order. Mirrors java-tron WitnessScheduleStore's
// `current_shuffled_witnesses` key. Value encoding: concatenated 21-byte
// addresses in shuffle order.
func WriteShuffledWitnesses(db ethdb.KeyValueWriter, witnesses []tcommon.Address) error {
	buf := make([]byte, 0, len(witnesses)*tcommon.AddressLength)
	for _, w := range witnesses {
		buf = append(buf, w[:]...)
	}
	return db.Put(shuffledWitnessesKey, buf)
}

// ReadShuffledWitnesses returns the persisted shuffle order, or nil if none.
func ReadShuffledWitnesses(db ethdb.KeyValueReader) []tcommon.Address {
	data, err := db.Get(shuffledWitnessesKey)
	if err != nil || len(data) == 0 {
		return nil
	}
	if len(data)%tcommon.AddressLength != 0 {
		// Corrupted; refuse to fabricate a partial list.
		return nil
	}
	count := len(data) / tcommon.AddressLength
	out := make([]tcommon.Address, count)
	for i := range out {
		copy(out[i][:], data[i*tcommon.AddressLength:(i+1)*tcommon.AddressLength])
	}
	return out
}

// DeleteShuffledWitnesses clears the shuffled witness list.
func DeleteShuffledWitnesses(db ethdb.KeyValueWriter) error {
	return db.Delete(shuffledWitnessesKey)
}
