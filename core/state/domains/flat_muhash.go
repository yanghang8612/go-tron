package domains

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// Standalone commitment mutations stage source+HEAD/root through one ephemeral
// layer, then one backend atomic batch. Canonical chain callers use StateDB.
func (s *FlatStore) mutateMuHash(owner common.Address, domain kvdomains.KVDomain, key, value []byte, kind MutationKind) error {
	if err := validateDomain(domain); err != nil {
		return err
	}
	if err := rawdb.InitializeStateCommitment(s.db); err != nil {
		return err
	}
	parent, present, err := rawdb.ReadStateCommitmentHead(s.db)
	if err != nil {
		return err
	}
	if !present || parent.BlockHash != (common.Hash{}) {
		return errors.New("standalone commitment mutation requires an unbound state context")
	}
	generation := s.defaultGeneration
	if s.resolveGeneration != nil {
		generation, err = s.resolveGeneration(owner)
		if err != nil {
			return err
		}
	}
	buffer := blockbuffer.New(s.db)
	buffer.SetBaseReadCacheSize(0)
	buffer.BeginBlock(common.Hash{}, 0)
	var deltas []rawdb.StateCommitmentDelta
	capture := func(logical, after []byte, exists bool) error {
		physical := rawdb.StateKVLatestCommitmentKey(owner, generation, domain, logical)
		before, present, err := rawdb.ReadStateCommitmentSource(buffer, physical)
		if err != nil {
			return err
		}
		deltas = append(deltas, rawdb.StateCommitmentDelta{Key: physical, BeforeExists: present, Before: before, AfterExists: exists, After: after})
		return nil
	}
	latest := newRawDBLatestKVStore(buffer)
	switch kind {
	case MutationPut:
		encoded := rawdb.EncodeStateKVLatestValue(value)
		if err = capture(key, encoded, true); err == nil {
			err = latest.PutLatest(owner, generation, domain, key, value)
		}
	case MutationDel:
		if err = capture(key, nil, false); err == nil {
			err = latest.DeleteLatest(owner, generation, domain, key)
		}
	case MutationDelPrefix:
		err = latest.IterateLatest(owner, generation, domain, key, func(logical, _ []byte) (bool, error) { return true, capture(logical, nil, false) })
		if err == nil {
			err = latest.DeleteLatestPrefix(owner, generation, domain, key)
		}
	default:
		return errors.New("unknown standalone source mutation")
	}
	if err != nil {
		return err
	}
	if _, err = ApplyMuHashDeltas(buffer, deltas, CommitmentIdentity{}, common.Hash{}); err != nil {
		return err
	}
	buffer.CommitBlock()
	return buffer.Flush(s.db)
}
