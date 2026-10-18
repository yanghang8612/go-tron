package rawdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

const MaxStateGenesisRecipeBytes = 4 << 20

var stateGenesisRecipeMagic = []byte("GTRGEN\x00\x01")

func WriteStateGenesisRecipe(db ethdb.KeyValueWriter, wireHash common.Hash, payload []byte) error {
	if len(payload) == 0 || len(payload) > MaxStateGenesisRecipeBytes {
		return fmt.Errorf("invalid genesis recipe size")
	}
	out := append([]byte(nil), stateGenesisRecipeMagic...)
	out = append(out, wireHash[:]...)
	sum := sha256.Sum256(payload)
	out = append(out, sum[:]...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(payload)))
	out = append(out, payload...)
	return db.Put(stateGenesisRecipeKey, out)
}
func ReadStateGenesisRecipe(db ethdb.KeyValueReader) (common.Hash, []byte, bool, error) {
	var hash common.Hash
	data, present, err := readPresentValue(db, stateGenesisRecipeKey, "genesis recipe")
	if err != nil || !present {
		return hash, nil, present, err
	}
	if len(data) < 76 || !bytes.Equal(data[:8], stateGenesisRecipeMagic) {
		return hash, nil, false, fmt.Errorf("invalid genesis recipe version/length")
	}
	size := int(binary.BigEndian.Uint32(data[72:76]))
	if size == 0 || size > MaxStateGenesisRecipeBytes || len(data) != 76+size {
		return hash, nil, false, fmt.Errorf("invalid genesis recipe size")
	}
	sum := sha256.Sum256(data[76:])
	if !bytes.Equal(sum[:], data[40:72]) {
		return hash, nil, false, fmt.Errorf("genesis recipe checksum mismatch")
	}
	copy(hash[:], data[8:40])
	return hash, bytes.Clone(data[76:]), true, nil
}
