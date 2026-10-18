package rawdb

import (
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

// StateExecutionProperties are the four derived current-state execution cursors.
// They accompany a same-cut snapshot; they are not new MuHash source elements.
type StateExecutionProperties struct {
	BlockNum, Timestamp, SolidifiedBlockNum int64
	BlockHash                               common.Hash
}

func ReadStateExecutionProperties(db ethdb.KeyValueReader) (StateExecutionProperties, error) {
	var out StateExecutionProperties
	for _, field := range []struct {
		name     string
		dst      *int64
		optional bool
	}{{"latest_block_header_number", &out.BlockNum, false}, {"latest_block_header_timestamp", &out.Timestamp, false}, {"latest_solidified_block_num", &out.SolidifiedBlockNum, true}} {
		value, present, err := ReadDynamicPropertyStrict(db, field.name)
		if err != nil {
			return out, err
		}
		if !present && field.optional {
			continue
		}
		if !present || len(value) != 8 {
			return out, fmt.Errorf("snapshot execution property %s missing/malformed", field.name)
		}
		*field.dst = int64(binary.BigEndian.Uint64(value))
	}
	hash, present, err := ReadDynamicPropertyStrict(db, "latest_block_header_hash")
	if err != nil {
		return out, err
	}
	if !present || len(hash) != 32 {
		return out, fmt.Errorf("snapshot execution block hash missing/malformed")
	}
	copy(out.BlockHash[:], hash)
	if out.BlockNum < 0 || out.Timestamp < 0 || out.SolidifiedBlockNum < 0 || out.SolidifiedBlockNum > out.BlockNum {
		return out, fmt.Errorf("snapshot execution properties outside cut")
	}
	return out, nil
}
func WriteStateExecutionProperties(db ethdb.KeyValueWriter, p StateExecutionProperties) error {
	if p.BlockNum < 0 || p.Timestamp < 0 || p.SolidifiedBlockNum < 0 || p.SolidifiedBlockNum > p.BlockNum {
		return fmt.Errorf("invalid snapshot execution properties")
	}
	for _, field := range []struct {
		name  string
		value int64
	}{{"latest_block_header_number", p.BlockNum}, {"latest_block_header_timestamp", p.Timestamp}, {"latest_solidified_block_num", p.SolidifiedBlockNum}} {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(field.value))
		if err := db.Put(dynPropKey(field.name), encoded[:]); err != nil {
			return err
		}
	}
	return db.Put(dynPropKey("latest_block_header_hash"), p.BlockHash[:])
}
