package state

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// DomainChangeStage is the execution-stage publisher for flat temporal
// StateDomainChange rows. It is intentionally range-planner friendly: callers
// pass the already planned StateTxRange, then transaction execution asks the
// stage to flush journal deltas at ordinal txNums.
type DomainChangeStage struct {
	state     *StateDB
	publisher StateDomainChangePublisher
	tx        rawdb.StateTxRange
}

func (s *StateDB) BeginDomainChangeStage(writer ethdb.KeyValueWriter, txRange *rawdb.StateTxRange) (*DomainChangeStage, error) {
	if s == nil || writer == nil || txRange == nil {
		return nil, nil
	}
	if txRange.EndTxNum < txRange.BeginTxNum {
		return nil, fmt.Errorf("state domain change stage: invalid tx range for block %d: [%d,%d]", txRange.BlockNum, txRange.BeginTxNum, txRange.EndTxNum)
	}
	s.BeginDomainChangeJournalCapture(writer, txRange.BlockNum, tcommon.Hash(txRange.BlockHash), txRange.BeginTxNum, txRange.EndTxNum)
	return &DomainChangeStage{
		state:     s,
		publisher: defaultStateDomainChangeRunner(writer),
		tx:        *txRange,
	}, nil
}

func (s *DomainChangeStage) JournalMark() int {
	if s == nil || s.state == nil {
		return 0
	}
	return s.state.DomainChangeJournalMark()
}

func (s *DomainChangeStage) TxNumAtOrdinal(ordinal uint64) (uint64, error) {
	if s == nil {
		return rawdb.StateTxNumAt(0, ordinal)
	}
	return rawdb.StateTxNumAt(s.tx.BeginTxNum, ordinal)
}

func (s *DomainChangeStage) FlushOrdinal(mark int, ordinal uint64) error {
	if s == nil || s.state == nil {
		return nil
	}
	txNum, err := s.TxNumAtOrdinal(ordinal)
	if err != nil {
		return err
	}
	return s.state.publishDomainChangesSince(s.publisher, mark, txNum)
}

func (s *DomainChangeStage) FlushFinal() error {
	if s == nil || s.state == nil {
		return nil
	}
	return s.state.publishDomainChangesSince(s.publisher, s.state.changeSet.journalMark, s.tx.EndTxNum)
}

func (s *DomainChangeStage) EndTxNum() uint64 {
	if s == nil {
		return 0
	}
	return s.tx.EndTxNum
}

// StateDomainChangePublisher is the stage boundary between execution journal
// capture and physical hot-history/index publication.
type StateDomainChangePublisher interface {
	PublishStateDomainChanges(changes []*rawdb.StateDomainChange) error
}

// StateDomainChangePublicationConfig registers the hot-history publication
// steps for one temporal history domain. The default config writes rawdb
// tx-ranges, rows, and inverse indexes; future configs can add or replace
// accessors without changing execution journal capture.
type StateDomainChangePublicationConfig struct {
	Name              string
	WriteTxRange      func(ethdb.KeyValueWriter, uint64, tcommon.Hash, uint64, uint64) error
	WriteRow          func(ethdb.KeyValueWriter, *rawdb.StateDomainChange) error
	WriteInverseIndex func(ethdb.KeyValueWriter, *rawdb.StateDomainChange) error
}

func DefaultStateDomainChangePublicationConfig() StateDomainChangePublicationConfig {
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok {
		return StateDomainChangePublicationConfig{}
	}
	return StateDomainChangePublicationConfigFromDomain(cfg)
}

func StateDomainChangePublicationConfigFromDomain(cfg snapshots.DomainCfg) StateDomainChangePublicationConfig {
	return StateDomainChangePublicationConfig{
		Name:              cfg.Name,
		WriteTxRange:      cfg.WriteHotHistoryTxRange,
		WriteRow:          cfg.WriteHotHistoryRow,
		WriteInverseIndex: cfg.WriteHotHistoryIndex,
	}
}

func defaultStateDomainChangeRunner(writer ethdb.KeyValueWriter) StateDomainChangeRunner {
	return NewStateDomainChangeRunner(writer, DefaultStateDomainChangePublicationConfig())
}

type StateDomainChangeRunner struct {
	writer ethdb.KeyValueWriter
	cfg    StateDomainChangePublicationConfig
}

func NewStateDomainChangeRunner(writer ethdb.KeyValueWriter, cfg StateDomainChangePublicationConfig) StateDomainChangeRunner {
	return StateDomainChangeRunner{writer: writer, cfg: cfg}
}

func (r StateDomainChangeRunner) PublishStateTxRange(blockNum uint64, blockHash tcommon.Hash, beginTxNum, endTxNum uint64) error {
	if r.writer == nil {
		return fmt.Errorf("state domain change stage: nil publisher")
	}
	if r.cfg.Name == "" {
		return fmt.Errorf("state domain change stage: unnamed publication config")
	}
	if r.cfg.WriteTxRange == nil {
		return fmt.Errorf("state domain change stage: incomplete publication config %q", r.cfg.Name)
	}
	return r.cfg.WriteTxRange(r.writer, blockNum, blockHash, beginTxNum, endTxNum)
}

func (r StateDomainChangeRunner) PublishStateDomainChanges(changes []*rawdb.StateDomainChange) error {
	if len(changes) == 0 {
		return nil
	}
	if r.writer == nil {
		return fmt.Errorf("state domain change stage: nil publisher")
	}
	if r.cfg.Name == "" {
		return fmt.Errorf("state domain change stage: unnamed publication config")
	}
	if r.cfg.WriteRow == nil || r.cfg.WriteInverseIndex == nil {
		return fmt.Errorf("state domain change stage: incomplete publication config %q", r.cfg.Name)
	}
	for _, change := range changes {
		if err := r.cfg.WriteRow(r.writer, change); err != nil {
			return err
		}
		if err := r.cfg.WriteInverseIndex(r.writer, change); err != nil {
			return err
		}
	}
	return nil
}
