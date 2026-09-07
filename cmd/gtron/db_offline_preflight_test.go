package main

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func seedOfflineBoundary(t *testing.T, db ethdb.KeyValueStore) common.Hash {
	t.Helper()
	for _, n := range []uint64{9, 10} {
		block, _ := dbRebuildTxIndexBlock(t, n, 0)
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		if n != 10 {
			continue
		}
		rawdb.WriteHeadBlockHash(db, block.Hash())
		rawdb.WriteDynamicProperty(db, "latest_block_header_hash", block.Hash().Bytes())
		for name, value := range map[string]uint64{"latest_block_header_number": 10, "latest_solidified_block_num": 9} {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, value)
			rawdb.WriteDynamicProperty(db, name, buf)
		}
		for _, stage := range []rawdb.StageID{rawdb.StageExecution, rawdb.StageFinish} {
			if err := rawdb.WriteStageProgressWithHash(db, stage, 10, block.Hash()); err != nil {
				t.Fatal(err)
			}
		}
		return block.Hash()
	}
	panic("unreachable")
}

func TestOfflineChainBoundaryRejectsUnsafePointers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(ethdb.KeyValueStore)
		want   string
	}{
		{"valid", func(ethdb.KeyValueStore) {}, ""},
		{"missing solid", func(db ethdb.KeyValueStore) { rawdb.WriteDynamicProperty(db, "latest_solidified_block_num", nil) }, "8-byte"},
		{"negative solid", func(db ethdb.KeyValueStore) {
			rawdb.WriteDynamicProperty(db, "latest_solidified_block_num", []byte{255, 255, 255, 255, 255, 255, 255, 255})
		}, "negative"},
		{"future solid", func(db ethdb.KeyValueStore) {
			rawdb.WriteDynamicProperty(db, "latest_solidified_block_num", []byte{0, 0, 0, 0, 0, 0, 0, 11})
		}, "outside"},
		{"unbound Finish", func(db ethdb.KeyValueStore) { _ = rawdb.WriteStageProgress(db, rawdb.StageFinish, 10) }, "Finish"},
		{"changed head property", func(db ethdb.KeyValueStore) {
			rawdb.WriteDynamicProperty(db, "latest_block_header_hash", make([]byte, 32))
		}, "hash mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			head := seedOfflineBoundary(t, db)
			tc.mutate(db)
			boundary, err := readOfflineChainBoundary(db)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("got %v, want %q", err, tc.want)
				}
				return
			}
			if err != nil || boundary.HeadHash != head || boundary.HeadBlock != 10 || boundary.SolidifiedBlock != 9 {
				t.Fatalf("boundary=%+v error=%v", boundary, err)
			}
		})
	}
}
