package rawdb

import "encoding/binary"

var (
	headBlockKey      = []byte("LastBlock")
	headSolidBlockKey = []byte("LastSolidBlock")

	blockPrefix     = []byte("b-")
	blockHashPrefix = []byte("bh-")
	txPrefix        = []byte("tx-")
	txInfoPrefix    = []byte("ti-")
	accountPrefix   = []byte("a-")
	witnessPrefix   = []byte("w-")
	votesPrefix     = []byte("v-")
	proposalPrefix  = []byte("p-")
	codePrefix      = []byte("c-")
	contractPrefix  = []byte("ct-")
	storagePrefix   = []byte("s-")
	dynPropPrefix   = []byte("dp-")

	witnessScheduleKey = []byte("ws")

	activeWitnessesKey = []byte("ActiveWitnesses")
	witnessIndexKey    = []byte("WitnessIndex")
)

func blockKey(number uint64) []byte {
	k := make([]byte, len(blockPrefix)+8)
	copy(k, blockPrefix)
	binary.BigEndian.PutUint64(k[len(blockPrefix):], number)
	return k
}

func blockHashKey(hash []byte) []byte {
	return append(append([]byte{}, blockHashPrefix...), hash...)
}

func txKey(hash []byte) []byte {
	return append(append([]byte{}, txPrefix...), hash...)
}

func accountKey(addr []byte) []byte {
	return append(append([]byte{}, accountPrefix...), addr...)
}

func witnessKey(addr []byte) []byte {
	return append(append([]byte{}, witnessPrefix...), addr...)
}

func dynPropKey(name string) []byte {
	return append(append([]byte{}, dynPropPrefix...), []byte(name)...)
}
