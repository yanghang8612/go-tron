package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type rewardTraceColdReaderStub struct {
	blockHash      tcommon.Hash
	blockNum       uint64
	accountOwner   []byte
	accountBlock   int64
	accountBalance int64
	sectionBloom   []byte
	coveredLogs    []rawdb.EventLog
}

func (r *rewardTraceColdReaderStub) BlockNumberByHash(hash tcommon.Hash) (uint64, bool, error) {
	if hash != r.blockHash {
		return 0, false, nil
	}
	return r.blockNum, true, nil
}

func (r *rewardTraceColdReaderStub) TransactionBlockNumberByHash(tcommon.Hash) (uint64, bool, error) {
	return 0, false, nil
}

func (r *rewardTraceColdReaderStub) BlockBalanceTrace(blockNum int64) (*contractpb.BlockBalanceTrace, bool, error) {
	if blockNum != r.accountBlock {
		return nil, false, nil
	}
	return &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{Number: blockNum},
	}, true, nil
}

func (r *rewardTraceColdReaderStub) AccountTraceAtOrBefore(owner []byte, blockNum int64) (int64, int64, bool, error) {
	if blockNum < r.accountBlock || !bytes.Equal(owner, r.accountOwner) {
		return 0, 0, false, nil
	}
	return r.accountBlock, r.accountBalance, true, nil
}

func (r *rewardTraceColdReaderStub) SectionBloom(section, bitIndex uint64) ([]byte, bool, error) {
	if section != 3 || bitIndex != 42 {
		return nil, false, nil
	}
	return append([]byte(nil), r.sectionBloom...), true, nil
}

func (r *rewardTraceColdReaderStub) EventLogRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	return fromBlock == 7 && toBlock == 7, nil
}

func (r *rewardTraceColdReaderStub) IterateEventLogs(fromBlock, toBlock uint64, _ rawdb.EventLogFilter, fn func(rawdb.EventLog) (bool, error)) error {
	for _, row := range r.coveredLogs {
		if row.BlockNum < fromBlock || row.BlockNum > toBlock {
			continue
		}
		keepGoing, err := fn(row)
		if err != nil || !keepGoing {
			return err
		}
	}
	return nil
}

func TestAttachRewardTraceColdReaders(t *testing.T) {
	chainDB := rawdb.NewMemoryChainDB()
	blockHash := tcommon.BytesToHash(bytes.Repeat([]byte{0x11}, tcommon.HashLength))
	owner := testRewardTraceAddress(0x22).Bytes()
	coldLog := rawdb.EventLog{
		BlockNum: 7,
		Log: &corepb.TransactionInfo_Log{
			Address: testRewardTraceAddress(0x33).Bytes(),
		},
	}
	reader := &rewardTraceColdReaderStub{
		blockHash:      blockHash,
		blockNum:       7,
		accountOwner:   append([]byte(nil), owner...),
		accountBlock:   7,
		accountBalance: 1234,
		sectionBloom:   []byte{0xaa, 0xbb},
		coveredLogs:    []rawdb.EventLog{coldLog},
	}

	attachRewardTraceColdReaders(chainDB, reader)

	if got := rawdb.ReadBlockNumber(chainDB, blockHash); got == nil || *got != 7 {
		t.Fatalf("ReadBlockNumber cold chain-index = %v, want 7", got)
	}
	if got, ok, err := rawdb.ReadAccountTraceStrict(chainDB, owner, 7); err != nil || !ok || got != 1234 {
		t.Fatalf("ReadAccountTraceStrict cold = %d/%v/%v, want 1234/true/nil", got, ok, err)
	}
	if got := rawdb.ReadBlockBalanceTrace(chainDB, 7); got == nil || got.GetBlockIdentifier().GetNumber() != 7 {
		t.Fatalf("ReadBlockBalanceTrace cold = %+v, want block 7", got)
	}
	if got, ok, err := rawdb.ReadSectionBloomStrict(chainDB, 3, 42); err != nil || !ok || !bytes.Equal(got, []byte{0xaa, 0xbb}) {
		t.Fatalf("ReadSectionBloomStrict cold = %x/%v/%v, want aabb/true/nil", got, ok, err)
	}
	if covered, err := chainDB.EventLogRangeCovered(7, 7); err != nil || !covered {
		t.Fatalf("EventLogRangeCovered cold = %v/%v, want true/nil", covered, err)
	}
	var logs []rawdb.EventLog
	if err := chainDB.IterateEventLogs(7, 7, rawdb.EventLogFilter{}, func(row rawdb.EventLog) (bool, error) {
		logs = append(logs, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs cold: %v", err)
	}
	if len(logs) != 1 || logs[0].BlockNum != 7 {
		t.Fatalf("IterateEventLogs cold rows = %+v, want block 7", logs)
	}
}

func TestReadRewardTraceBlockReportsCorruptColdBlock(t *testing.T) {
	chainDB := rawdb.NewChainDB(rawdb.NewMemoryDatabase(), corruptRewardTraceAncient{})

	got, err := readRewardTraceBlock(chainDB, 7)
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("readRewardTraceBlock = %+v/%v, want cold block decode error", got, err)
	}
}

func TestReadRewardTraceBlockNumberReportsColdIndexError(t *testing.T) {
	chainDB := rawdb.NewMemoryChainDB()
	chainDB.SetChainIndexReader(rewardTraceErrChainIndex{err: errors.New("cold chain index corrupt")})

	_, _, err := readRewardTraceBlockNumber(chainDB, tcommon.BytesToHash(bytes.Repeat([]byte{0x77}, tcommon.HashLength)))
	if err == nil || !strings.Contains(err.Error(), "cold chain index corrupt") {
		t.Fatalf("readRewardTraceBlockNumber error = %v, want cold chain-index corruption", err)
	}
}

func TestReadRewardTraceBlockStateRootReportsCorruptColdRoot(t *testing.T) {
	chainDB := rawdb.NewChainDB(rawdb.NewMemoryDatabase(), corruptRewardTraceStateRootAncient{})
	chainDB.SetChainIndexReader(rewardTraceStaticChainIndex{blockNum: 7})

	_, _, err := readRewardTraceBlockStateRoot(chainDB, tcommon.BytesToHash(bytes.Repeat([]byte{0x88}, tcommon.HashLength)))
	if err == nil || !strings.Contains(err.Error(), "block state root") {
		t.Fatalf("readRewardTraceBlockStateRoot error = %v, want corrupt cold state-root error", err)
	}
}

type rewardTraceErrChainIndex struct {
	err error
}

func (r rewardTraceErrChainIndex) BlockNumberByHash(tcommon.Hash) (uint64, bool, error) {
	return 0, false, r.err
}

func (r rewardTraceErrChainIndex) TransactionBlockNumberByHash(tcommon.Hash) (uint64, bool, error) {
	return 0, false, r.err
}

type rewardTraceStaticChainIndex struct {
	blockNum uint64
}

func (r rewardTraceStaticChainIndex) BlockNumberByHash(tcommon.Hash) (uint64, bool, error) {
	return r.blockNum, true, nil
}

func (r rewardTraceStaticChainIndex) TransactionBlockNumberByHash(tcommon.Hash) (uint64, bool, error) {
	return r.blockNum, true, nil
}

type corruptRewardTraceAncient struct{}

func (corruptRewardTraceAncient) Ancient(kind string, _ uint64) ([]byte, error) {
	if kind != rawdb.AncientBlocksTable {
		return nil, rawdb.ErrNotInAncient
	}
	return []byte{0xff}, nil
}

func (corruptRewardTraceAncient) AncientRange(string, uint64, uint64, uint64) ([][]byte, error) {
	return nil, rawdb.ErrNotInAncient
}

func (corruptRewardTraceAncient) AncientCount(string) (uint64, error) {
	return 0, nil
}

func (corruptRewardTraceAncient) HasAncient(kind string, _ uint64) (bool, error) {
	return kind == rawdb.AncientBlocksTable, nil
}

type corruptRewardTraceStateRootAncient struct{}

func (corruptRewardTraceStateRootAncient) Ancient(kind string, _ uint64) ([]byte, error) {
	if kind != rawdb.AncientStateRootsTable {
		return nil, rawdb.ErrNotInAncient
	}
	return []byte{0x01}, nil
}

func (corruptRewardTraceStateRootAncient) AncientRange(string, uint64, uint64, uint64) ([][]byte, error) {
	return nil, rawdb.ErrNotInAncient
}

func (corruptRewardTraceStateRootAncient) AncientCount(string) (uint64, error) {
	return 0, nil
}

func (corruptRewardTraceStateRootAncient) HasAncient(kind string, _ uint64) (bool, error) {
	return kind == rawdb.AncientStateRootsTable, nil
}

func testRewardTraceAddress(fill byte) tcommon.Address {
	raw := make([]byte, tcommon.AddressLength)
	raw[0] = tcommon.AddressPrefixMainnet
	for i := 1; i < len(raw); i++ {
		raw[i] = fill
	}
	return tcommon.BytesToAddress(raw)
}
