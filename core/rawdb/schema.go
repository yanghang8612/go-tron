package rawdb

import "encoding/binary"

var (
	headBlockKey      = []byte("LastBlock")
	headSolidBlockKey = []byte("LastSolidBlock")

	blockPrefix     = []byte("b-")
	blockHashPrefix = []byte("bh-")
	txPrefix        = []byte("tx-")
	txInfoPrefix    = []byte("ti-")
	txInfoBlockPrefix = []byte("tib-")
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

	proposalIndexKey = []byte("propi")

	delegationPrefix      = []byte("dr-")
	delegationIndexPrefix = []byte("dri-")
	brokeragePrefix       = []byte("wb-")

	assetPrefix          = []byte("ast-")   // tokenID big-endian 8B → AssetIssueContract proto bytes
	assetNamePrefix      = []byte("astn-")  // token name bytes → tokenID big-endian 8B
	assetOwnerPrefix     = []byte("asto-")  // owner address 21B → tokenID big-endian 8B
	assetIssueTimePrefix = []byte("asti-")  // tokenID big-endian 8B → issue timestamp ms big-endian 8B

	marketOrderPrefix        = []byte("mo-")
	marketAccountOrderPrefix = []byte("mao-")
	marketOrderBookPrefix    = []byte("mop-")
	marketPriceListPrefix    = []byte("mpl-")

	exchangePrefix = []byte("ex-")
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

func txInfoKey(hash []byte) []byte {
	return append(append([]byte{}, txInfoPrefix...), hash...)
}

func txInfoBlockKey(number uint64) []byte {
	k := make([]byte, len(txInfoBlockPrefix)+8)
	copy(k, txInfoBlockPrefix)
	binary.BigEndian.PutUint64(k[len(txInfoBlockPrefix):], number)
	return k
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

func proposalKey(id int64) []byte {
	k := make([]byte, len(proposalPrefix)+8)
	copy(k, proposalPrefix)
	binary.BigEndian.PutUint64(k[len(proposalPrefix):], uint64(id))
	return k
}

func delegationKey(from, to []byte) []byte {
	k := make([]byte, len(delegationPrefix)+len(from)+len(to))
	copy(k, delegationPrefix)
	copy(k[len(delegationPrefix):], from)
	copy(k[len(delegationPrefix)+len(from):], to)
	return k
}

func delegationIndexKey(from []byte) []byte {
	return append(append([]byte{}, delegationIndexPrefix...), from...)
}

func brokerageKey(addr []byte) []byte {
	return append(append([]byte{}, brokeragePrefix...), addr...)
}

func assetKey(tokenID int64) []byte {
	k := make([]byte, len(assetPrefix)+8)
	copy(k, assetPrefix)
	binary.BigEndian.PutUint64(k[len(assetPrefix):], uint64(tokenID))
	return k
}

func assetNameKey(name []byte) []byte {
	return append(append([]byte{}, assetNamePrefix...), name...)
}

func assetOwnerKey(ownerAddr []byte) []byte {
	return append(append([]byte{}, assetOwnerPrefix...), ownerAddr...)
}

func assetIssueTimeKey(tokenID int64) []byte {
	k := make([]byte, len(assetIssueTimePrefix)+8)
	copy(k, assetIssueTimePrefix)
	binary.BigEndian.PutUint64(k[len(assetIssueTimePrefix):], uint64(tokenID))
	return k
}

func gcdInt64(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// PriceKey normalizes a {sellQty, buyQty} pair by GCD and encodes as 16 bytes.
func PriceKey(sellQty, buyQty int64) [16]byte {
	g := gcdInt64(sellQty, buyQty)
	var k [16]byte
	binary.BigEndian.PutUint64(k[:8], uint64(sellQty/g))
	binary.BigEndian.PutUint64(k[8:], uint64(buyQty/g))
	return k
}

func marketOrderKey(orderID []byte) []byte {
	return append(append([]byte{}, marketOrderPrefix...), orderID...)
}

func marketAccountOrderKey(ownerAddr []byte) []byte {
	return append(append([]byte{}, marketAccountOrderPrefix...), ownerAddr...)
}

func marketOrderBookKey(sellTokenID, buyTokenID []byte, pk [16]byte) []byte {
	k := append(append([]byte{}, marketOrderBookPrefix...), sellTokenID...)
	k = append(k, '|')
	k = append(k, buyTokenID...)
	k = append(k, '|')
	return append(k, pk[:]...)
}

func marketPriceListKey(sellTokenID, buyTokenID []byte) []byte {
	k := append(append([]byte{}, marketPriceListPrefix...), sellTokenID...)
	k = append(k, '|')
	return append(k, buyTokenID...)
}

func exchangeKey(id int64) []byte {
	k := make([]byte, len(exchangePrefix)+8)
	copy(k, exchangePrefix)
	binary.BigEndian.PutUint64(k[len(exchangePrefix):], uint64(id))
	return k
}
