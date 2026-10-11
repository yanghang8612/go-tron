package snapshots

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const historyCDCMigrationJournalFile = ".history-cdc-migration.json"

// HistoryCDCMigrationOptions selects complete trios within inclusive tx bounds.
// Zero bounds mean unbounded. MaxTrios also counts candidates rejected for size.
// The node must be stopped and its exclusive database lock held by the caller.
type HistoryCDCMigrationOptions struct {
	MaxTrios, FromTxNum, ToTxNum uint64
	MinSavingsPercent            uint64
	DryRun                       bool
	R1Only                       bool
	AllowGrowth                  bool // bypass the savings threshold for explicit format unification
	BeforeTrio                   func([]SegmentRef) error
	OnProgress                   func(HistoryReferenceMigrationProgress)
}

func (o *HistoryCDCMigrationOptions) includes(r SegmentRef) bool {
	return (o.FromTxNum == 0 || r.FromTxNum >= o.FromTxNum) && (o.ToTxNum == 0 || r.ToTxNum <= o.ToTxNum)
}

// MigrateHistoryCDCContext uses the same durable publication/reclamation
// protocol as R1 migration, with a separate journal and an explicit CDC codec.
// It never changes logical bytes, chain identity, coverage or progress fields.
func MigrateHistoryCDCContext(ctx context.Context, dir string, opts HistoryCDCMigrationOptions) (*HistoryReferenceMigrationResult, error) {
	if opts.MinSavingsPercent > 100 || opts.ToTxNum != 0 && opts.FromTxNum > opts.ToTxNum {
		return nil, errors.New("snapshots: invalid CDC selection/savings threshold")
	}
	return MigrateHistoryReferenceContext(ctx, dir, HistoryReferenceMigrationOptions{MaxTrios: opts.MaxTrios, DryRun: opts.DryRun, BeforeTrio: opts.BeforeTrio, OnProgress: opts.OnProgress, cdc: &opts})
}

func (m *historyReferenceMigration) journalFile() string {
	if m.opts.cdc != nil {
		return historyCDCMigrationJournalFile
	}
	return historyReferenceMigrationJournalFile
}
func (m *historyReferenceMigration) loadJournal() (*historyReferenceMigrationJournal, error) {
	other := historyCDCMigrationJournalFile
	if m.opts.cdc != nil {
		other = historyReferenceMigrationJournalFile
	}
	if _, err := os.Lstat(filepath.Join(m.dir, other)); err == nil || !os.IsNotExist(err) {
		return nil, fmt.Errorf("snapshots: finish pending %s before another migration", other)
	}
	return loadHistoryMigrationJournal(m.dir, m.journalFile())
}
func (m *historyReferenceMigration) isCurrent(ref SegmentRef) (bool, error) {
	if m.opts.cdc != nil {
		return historyCDCIsCurrent(m.dir, ref)
	}
	return historyReferenceMigrationIsCurrent(m.dir, ref)
}
func (m *historyReferenceMigration) reencode(refs []SegmentRef) ([]SegmentRef, error) {
	if m.opts.cdc != nil {
		return reencodeHistoryCDC(m.ctx, m.dir, refs, m.opts.cdc.MinSavingsPercent, m.opts.cdc.AllowGrowth)
	}
	out, _, err := ReencodeHistoryReferenceTrioContext(m.ctx, m.dir, refs)
	return out, err
}
func (m *historyReferenceMigration) verifyNew(refs []SegmentRef) error {
	if m.opts.cdc == nil {
		return historyReferenceMigrationVerifyNew(m.ctx, m.dir, refs)
	}
	h, i, a, _, err := historyReferenceTranscodeIdentity(refs)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		if err = historyReferenceMigrationFile(m.dir, ref.Path, false); err != nil {
			return err
		}
	}
	current, err := historyCDCIsCurrent(m.dir, h)
	if err != nil {
		return err
	}
	if !current {
		return errors.New("snapshots: CDC journal output is not CDC")
	}
	return verifyStateDomainChangeBinaryCompanionChecksumsContext(m.ctx, m.dir, h, i, a)
}
func historyCDCIsCurrent(dir string, ref SegmentRef) (bool, error) {
	if err := historyReferenceMigrationFile(dir, ref.Path, false); err != nil {
		return false, err
	}
	f, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return false, err
	}
	defer f.Close()
	var h [16]byte
	if _, err = io.ReadFull(f, h[:]); err != nil {
		return false, err
	}
	if string(h[:8]) == historyReferenceMagic {
		return false, nil
	}
	if string(h[:8]) != compressedBlockMagic {
		return false, errors.New("snapshots: CDC migration requires R1 or compressed V2/V3 input")
	}
	switch binary.BigEndian.Uint32(h[8:12]) {
	case compressedBlockCDCVersion:
		return true, nil
	case compressedBlockFooterVersion:
		return false, nil
	default:
		return false, errors.New("snapshots: CDC migration only supports R1/V2/V3 containers")
	}
}
