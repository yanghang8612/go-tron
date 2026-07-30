package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// SetHistoricalLatestView redirects StateDB flat-latest reads through a
// PersistentHistoryReader at blockNum. This is intended for read-only archive
// execution, such as Solidity/PBFT constant calls, where opening an old
// commitment root is not enough: the flat latest account/KV mirrors point at
// head unless callers explicitly supply an as-of latest view.
func (s *StateDB) SetHistoricalLatestView(reader *PersistentHistoryReader, blockNum uint64) {
	if s == nil || reader == nil {
		return
	}
	view := historicalLatestView{reader: reader, blockNum: blockNum}
	s.flatLatestReader = view
	s.setAccountKVLatestView(view, view)
}

type historicalLatestView struct {
	reader   *PersistentHistoryReader
	blockNum uint64
}

func (v historicalLatestView) AccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	if v.reader == nil {
		return nil, false, nil
	}
	return v.reader.readStateAccountLatestAsOf(owner, v.blockNum, v.reader.headNum)
}

func (v historicalLatestView) KVGeneration(owner tcommon.Address) (uint64, bool, error) {
	if v.reader == nil {
		return 0, false, nil
	}
	targetTxNum, err := v.reader.stateTxNumAtBlockEnd(v.blockNum)
	if err != nil {
		return 0, false, err
	}
	headTxNum, err := v.reader.stateTxNumAtBlockEnd(v.reader.headNum)
	if err != nil {
		return 0, false, err
	}
	return v.reader.readStateKVGenerationAsOfTxNum(owner, targetTxNum, headTxNum)
}

func (v historicalLatestView) GetLatest(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	generation, ok, err := v.KVGeneration(owner)
	if err != nil || !ok {
		return nil, false, err
	}
	return v.KVLatest(owner, generation, domain, key)
}

func (v historicalLatestView) KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if v.reader == nil {
		return nil, false, nil
	}
	return v.reader.readStateKVAsOf(owner, generation, domain, key, v.blockNum, v.reader.headNum)
}

func (v historicalLatestView) DomainIterate(owner tcommon.Address, domain kvdomains.KVDomain, prefix []byte, fn statedomains.IterateFunc) error {
	if v.reader == nil {
		return nil
	}
	return v.reader.AccountKVPrefixAt(owner, domain, prefix, v.blockNum, fn)
}

func (v historicalLatestView) KVLatestPrefix(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	if v.reader == nil {
		return nil
	}
	return v.reader.KVLatestPrefixAt(owner, generation, domain, prefix, v.blockNum, fn)
}
