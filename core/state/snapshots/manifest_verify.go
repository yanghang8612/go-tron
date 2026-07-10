package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type VerifyManifestOptions struct {
	ExpectedChain     *ChainIdentity
	CheckRetired      bool
	RequireRegistered bool
	RequireChecksums  bool
}

type ManifestVerificationReport struct {
	ActiveSegments  int
	RetiredSegments int
}

type RestoreVerifiedLatestResult struct {
	Verification  ManifestVerificationReport
	RestoredTxNum uint64
}

type RestoreVerifiedHistoryResult struct {
	Verification     ManifestVerificationReport
	FromTxNum        uint64
	ToTxNum          uint64
	ChangesRestored  uint64
	TxRangesRestored uint64
}

type RestoreVerifiedSnapshotResult struct {
	Verification     ManifestVerificationReport
	RestoredTxNum    uint64
	ChangesRestored  uint64
	TxRangesRestored uint64
	Progress         *Progress
}

type VerifyRestoredSnapshotBoundaryOptions struct {
	RequireIndependentCommitmentRoot bool
	RebuildCommitmentRoot            func() (common.Hash, error)
}

type RestoreVerifiedSnapshotOptions struct {
	Boundary VerifyRestoredSnapshotBoundaryOptions
	ETL      RestoreETLOptions
}

// VerifyManifestFiles loads the production manifest in dir and verifies every
// active segment file it references. Registered segment families are checked by
// their format-aware checker; unregistered legacy refs fall back to path, size,
// and optional checksum validation unless RequireRegistered is set.
func VerifyManifestFiles(dir string, opts VerifyManifestOptions) (*ManifestVerificationReport, error) {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	return VerifyLoadedManifestFiles(dir, manifest, opts)
}

// VerifyRemoteManifestFiles is the strict pre-install check for Erigon-style
// snapshot bootstrap catalogs. It rejects manifests that lack chain identity,
// reference unknown segment families, or omit per-file checksums.
func VerifyRemoteManifestFiles(dir string, expected ChainIdentity) (*ManifestVerificationReport, error) {
	_, report, err := VerifyRemoteProductionManifest(dir, expected)
	return report, err
}

func VerifyRemoteProductionManifest(dir string, expected ChainIdentity) (*Manifest, *ManifestVerificationReport, error) {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, nil, err
	}
	report, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{
		ExpectedChain:     &expected,
		RequireRegistered: true,
		RequireChecksums:  true,
	})
	if err != nil {
		return nil, nil, err
	}
	return manifest, report, nil
}

// RestoreLatestFromVerifiedManifest performs the strict remote-manifest
// pre-install verification and restores all latest-domain segments visible at
// the manifest boundary into db. It is intentionally limited to latest-domain
// state; callers still own chain/freezer installation and sync resumption.
func RestoreLatestFromVerifiedManifest(db ethdb.KeyValueWriter, dir string, expected ChainIdentity) (*RestoreVerifiedLatestResult, error) {
	return RestoreLatestFromVerifiedManifestWithOptions(db, dir, expected, RestoreVerifiedSnapshotOptions{})
}

func RestoreLatestFromVerifiedManifestWithOptions(db ethdb.KeyValueWriter, dir string, expected ChainIdentity, opts RestoreVerifiedSnapshotOptions) (*RestoreVerifiedLatestResult, error) {
	manifest, report, err := VerifyRemoteProductionManifest(dir, expected)
	if err != nil {
		return nil, err
	}
	return restoreLatestFromVerifiedManifestWithOptions(db, dir, manifest, *report, opts)
}

func restoreLatestFromVerifiedManifestWithOptions(db ethdb.KeyValueWriter, dir string, manifest *Manifest, report ManifestVerificationReport, opts RestoreVerifiedSnapshotOptions) (*RestoreVerifiedLatestResult, error) {
	mgr, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		return nil, err
	}
	manifest = mgr.Manifest()
	if manifest == nil {
		return nil, fmt.Errorf("snapshots: manifest missing after verification")
	}
	if err := mgr.RestoreLatestWithOptions(db, manifest.VisibleTxEnd, opts.ETL); err != nil {
		return nil, err
	}
	reader, ok := db.(ethdb.KeyValueReader)
	if !ok {
		return nil, fmt.Errorf("snapshots: restored snapshot boundary cannot be verified with write-only database")
	}
	if err := VerifyRestoredSnapshotBoundaryWithOptions(reader, mgr, manifest, opts.Boundary); err != nil {
		return nil, err
	}
	return &RestoreVerifiedLatestResult{
		Verification:  report,
		RestoredTxNum: manifest.VisibleTxEnd,
	}, nil
}

// RestoreHistoryFromVerifiedManifest performs the strict remote-manifest
// pre-install verification and restores state-domain history rows, inverse
// indexes, and block tx-range rows derivable from the history segments.
func RestoreHistoryFromVerifiedManifest(db ethdb.KeyValueWriter, dir string, expected ChainIdentity) (*RestoreVerifiedHistoryResult, error) {
	return RestoreHistoryFromVerifiedManifestWithOptions(db, dir, expected, RestoreVerifiedSnapshotOptions{})
}

func RestoreHistoryFromVerifiedManifestWithOptions(db ethdb.KeyValueWriter, dir string, expected ChainIdentity, opts RestoreVerifiedSnapshotOptions) (*RestoreVerifiedHistoryResult, error) {
	manifest, report, err := VerifyRemoteProductionManifest(dir, expected)
	if err != nil {
		return nil, err
	}
	return restoreHistoryFromVerifiedManifestWithOptions(db, dir, manifest, *report, opts)
}

func restoreHistoryFromVerifiedManifestWithOptions(db ethdb.KeyValueWriter, dir string, manifest *Manifest, report ManifestVerificationReport, opts RestoreVerifiedSnapshotOptions) (*RestoreVerifiedHistoryResult, error) {
	mgr, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		return nil, err
	}
	manifest = mgr.Manifest()
	if manifest == nil {
		return nil, fmt.Errorf("snapshots: manifest missing after verification")
	}
	restored, err := mgr.RestoreStateDomainHistoryWithOptions(db, manifest.VisibleTxStart, manifest.VisibleTxEnd, opts.ETL)
	if err != nil {
		return nil, err
	}
	return &RestoreVerifiedHistoryResult{
		Verification:     report,
		FromTxNum:        restored.FromTxNum,
		ToTxNum:          restored.ToTxNum,
		ChangesRestored:  restored.ChangesRestored,
		TxRangesRestored: restored.TxRangesRestored,
	}, nil
}

// RestoreSnapshotFromVerifiedManifest performs the strict remote-manifest
// pre-install verification, restores latest state and state-domain history into
// db, then records the installed snapshot txNum boundary in stage progress.
func RestoreSnapshotFromVerifiedManifest(db ethdb.KeyValueWriter, dir string, expected ChainIdentity) (*RestoreVerifiedSnapshotResult, error) {
	return RestoreSnapshotFromVerifiedManifestWithOptions(db, dir, expected, RestoreVerifiedSnapshotOptions{})
}

func RestoreSnapshotFromVerifiedManifestWithOptions(db ethdb.KeyValueWriter, dir string, expected ChainIdentity, opts RestoreVerifiedSnapshotOptions) (*RestoreVerifiedSnapshotResult, error) {
	manifest, report, err := VerifyRemoteProductionManifest(dir, expected)
	if err != nil {
		return nil, err
	}
	return restoreSnapshotFromVerifiedManifestWithOptions(db, dir, manifest, *report, opts)
}

func restoreSnapshotFromVerifiedManifestWithOptions(db ethdb.KeyValueWriter, dir string, manifest *Manifest, report ManifestVerificationReport, opts RestoreVerifiedSnapshotOptions) (*RestoreVerifiedSnapshotResult, error) {
	mgr, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		return nil, err
	}
	manifest = mgr.Manifest()
	if manifest == nil {
		return nil, fmt.Errorf("snapshots: manifest missing after verification")
	}
	if err := mgr.RestoreLatestWithOptions(db, manifest.VisibleTxEnd, opts.ETL); err != nil {
		return nil, err
	}
	reader, ok := db.(ethdb.KeyValueReader)
	if !ok {
		return nil, fmt.Errorf("snapshots: restored snapshot boundary cannot be verified with write-only database")
	}
	if err := VerifyRestoredSnapshotBoundaryWithOptions(reader, mgr, manifest, opts.Boundary); err != nil {
		return nil, err
	}
	restoredHistory, err := mgr.RestoreStateDomainHistoryWithOptions(db, manifest.VisibleTxStart, manifest.VisibleTxEnd, opts.ETL)
	if err != nil {
		return nil, err
	}
	progress, err := WriteSnapshotInstallProgress(db, manifest)
	if err != nil {
		return nil, err
	}
	return &RestoreVerifiedSnapshotResult{
		Verification:     report,
		RestoredTxNum:    manifest.VisibleTxEnd,
		ChangesRestored:  restoredHistory.ChangesRestored,
		TxRangesRestored: restoredHistory.TxRangesRestored,
		Progress:         progress,
	}, nil
}

func VerifyRestoredSnapshotBoundary(db ethdb.KeyValueReader, mgr *Manager, manifest *Manifest) error {
	return VerifyRestoredSnapshotBoundaryWithOptions(db, mgr, manifest, VerifyRestoredSnapshotBoundaryOptions{})
}

func VerifyRestoredSnapshotBoundaryWithOptions(db ethdb.KeyValueReader, mgr *Manager, manifest *Manifest, opts VerifyRestoredSnapshotBoundaryOptions) error {
	if db == nil || mgr == nil || manifest == nil {
		return nil
	}
	if manifestHasLatestDataset(manifest, SegmentDatasetCommitmentRoot) {
		expected, ok, err := mgr.GetCommitmentRoot(manifest.VisibleTxEnd)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("snapshots: commitment root segment present but no root at txNum %d", manifest.VisibleTxEnd)
		}
		got, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("snapshots: restored snapshot missing latest commitment root at txNum %d", manifest.VisibleTxEnd)
		}
		if got != expected {
			return fmt.Errorf("snapshots: restored latest commitment root %x, want %x at txNum %d", got, expected, manifest.VisibleTxEnd)
		}
		if opts.RebuildCommitmentRoot == nil {
			if opts.RequireIndependentCommitmentRoot {
				return fmt.Errorf("snapshots: restored snapshot commitment root cannot be independently rebuilt without a rebuilder")
			}
		} else {
			rebuilt, err := opts.RebuildCommitmentRoot()
			if err != nil {
				return err
			}
			if rebuilt != expected {
				return fmt.Errorf("snapshots: rebuilt latest commitment root %x, want snapshot root %x at txNum %d", rebuilt, expected, manifest.VisibleTxEnd)
			}
		}
	}
	if manifestHasLatestDataset(manifest, SegmentDatasetCommitmentCheckpoint) {
		expected, ok, err := mgr.getLatestValue(SegmentDatasetCommitmentCheckpoint, 0, rawdb.LatestStateCommitmentCheckpointLogicalKey(), manifest.VisibleTxEnd)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("snapshots: commitment checkpoint segment present but no latest checkpoint pointer at txNum %d", manifest.VisibleTxEnd)
		}
		got, ok, err := rawdb.ReadLatestStateCommitmentCheckpoint(db)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("snapshots: restored snapshot missing latest commitment checkpoint at txNum %d", manifest.VisibleTxEnd)
		}
		encoded, err := rawdb.EncodeStateCommitmentCheckpointValue(got)
		if err != nil {
			return err
		}
		if !bytes.Equal(encoded, expected) {
			return fmt.Errorf("snapshots: restored latest commitment checkpoint does not match snapshot boundary at txNum %d", manifest.VisibleTxEnd)
		}
	}
	return nil
}

func manifestHasLatestDataset(manifest *Manifest, dataset SegmentDataset) bool {
	if manifest == nil {
		return false
	}
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentLatest && ref.NormalizedDataset() == dataset {
			return true
		}
	}
	return false
}

// VerifyLoadedManifestFiles verifies an already-loaded manifest against files
// rooted at dir. This is useful for callers that fetched a manifest object from
// a catalog and have already performed signature/authentication checks.
func VerifyLoadedManifestFiles(dir string, manifest *Manifest, opts VerifyManifestOptions) (*ManifestVerificationReport, error) {
	if manifest == nil {
		return nil, fmt.Errorf("snapshots: nil manifest")
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	if err := manifest.ValidateProduction(); err != nil {
		return nil, err
	}
	if opts.ExpectedChain != nil {
		if err := manifest.ValidateChainIdentity(*opts.ExpectedChain); err != nil {
			return nil, err
		}
	}
	report := &ManifestVerificationReport{}
	for _, ref := range manifest.Segments {
		if err := verifyManifestSegmentRef(dir, ref, opts); err != nil {
			return nil, err
		}
		report.ActiveSegments++
	}
	if err := verifyManifestChainFreezerSidecars(dir, manifest); err != nil {
		return nil, err
	}
	if err := verifyManifestStateDomainHistorySidecars(dir, manifest); err != nil {
		return nil, err
	}
	if err := verifyManifestLatestBinarySidecars(dir, manifest); err != nil {
		return nil, err
	}
	if err := verifyManifestEventLogSidecars(dir, manifest); err != nil {
		return nil, err
	}
	if opts.CheckRetired {
		for _, ref := range manifest.Retired {
			if err := verifyManifestSegmentRef(dir, ref, opts); err != nil {
				return nil, err
			}
			report.RetiredSegments++
		}
	}
	return report, nil
}

func verifyManifestChainFreezerSidecars(dir string, manifest *Manifest) error {
	if manifest == nil {
		return nil
	}
	freezerByRange := make(map[chainBlockSegmentRange]SegmentRef)
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentChainFreezer || ref.normalizedDataset() != SegmentDatasetChainFreezer {
			continue
		}
		freezerByRange[chainBlockSegmentRange{from: ref.FromTxNum, to: ref.ToTxNum}] = ref
	}
	for _, ref := range manifest.Segments {
		if ref.normalizedDataset() != SegmentDatasetChainFreezer {
			continue
		}
		freezerRef, ok := freezerByRange[chainBlockSegmentRange{from: ref.FromTxNum, to: ref.ToTxNum}]
		if !ok {
			continue
		}
		switch ref.Kind {
		case SegmentChainIndex:
			if err := VerifyChainIndexSegmentAgainstChainFreezer(dir, ref, freezerRef); err != nil {
				return err
			}
		case SegmentChainFreezerAccessor:
			if err := VerifyChainFreezerAccessorSegmentAgainstChainFreezer(dir, ref, freezerRef); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyManifestStateDomainHistorySidecars(dir string, manifest *Manifest) error {
	if manifest == nil {
		return nil
	}
	registry := DefaultDomainRegistry()
	for _, ref := range manifest.Segments {
		cfg, ok := registry.ConfigForRef(ref)
		if !ok || ref.Kind != SegmentHistory || !cfg.IsHistoryBinarySegmentPath(ref.Path) {
			continue
		}
		idxRef, ok := cfg.HistoryIndexRef(manifest, ref)
		if !ok {
			return fmt.Errorf("snapshots: binary %s history %q missing required index %q", cfg.Dataset, ref.Path, cfg.HistoryIndexPathFor(ref.Path))
		}
		accessorRef, ok := cfg.HistoryAccessorRef(manifest, ref)
		if !ok {
			return fmt.Errorf("snapshots: binary %s history %q missing required accessor %q", cfg.Dataset, ref.Path, cfg.HistoryAccessorPathFor(ref.Path))
		}
		if err := verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir, ref, idxRef, accessorRef); err != nil {
			return err
		}
	}
	return nil
}

func verifyManifestLatestBinarySidecars(dir string, manifest *Manifest) error {
	if manifest == nil {
		return nil
	}
	registry := DefaultDomainRegistry()
	for _, ref := range manifest.Segments {
		cfg, ok := registry.ConfigForRef(ref)
		if !ok || ref.Kind != SegmentLatest || !cfg.HasLatest || !isLatestBinarySegmentPath(ref.Path) {
			continue
		}
		if err := verifyManifestLatestBinarySidecarSet(dir, manifest, cfg, ref); err != nil {
			return err
		}
	}
	return nil
}

func verifyManifestLatestBinarySidecarSet(dir string, manifest *Manifest, cfg DomainCfg, ref SegmentRef) error {
	if err := CheckLatestSegment(dir, ref); err != nil {
		return err
	}
	path := filepath.Join(dir, ref.Path)
	segFile, segHeader, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return err
	}
	defer segFile.Close()

	if cfg.HasLatestAccessor {
		accessorRef, ok := latestBinaryAccessorRef(manifest, ref)
		if ok {
			if err := CheckLatestAccessorSegment(dir, accessorRef); err != nil {
				return err
			}
			accessorFile, accessorHeader, err := openLatestBinaryAccessorReader(dir, accessorRef)
			if err != nil {
				return err
			}
			defer accessorFile.Close()
			if err := validateLatestBinaryAccessorMatchesSegment(path, ref, segHeader, accessorHeader); err != nil {
				return err
			}
			if err := verifyLatestBinaryAccessorOffsetsAgainstSegment(path, segFile, segHeader, accessorFile, accessorHeader); err != nil {
				return err
			}
		}
	}
	if cfg.HasLatestBTree {
		btreeRef, ok := latestBinaryBTreeRef(manifest, ref)
		if !ok {
			return fmt.Errorf("snapshots: binary latest %q missing required btree %q", ref.Path, latestBinaryBTreePath(ref.Path))
		}
		if err := CheckLatestBTreeSegment(dir, btreeRef); err != nil {
			return err
		}
		btreeFile, btreeHeader, err := openLatestBinaryBTreeReader(dir, btreeRef)
		if err != nil {
			return err
		}
		defer btreeFile.Close()
		if err := validateLatestBinaryCompanionMatchesSegment(path, ref, segHeader, btreeHeader.latestBinaryAccessorHeader, SegmentBTree); err != nil {
			return err
		}
		if err := verifyLatestBinaryBTreeAgainstSegment(path, segFile, segHeader, btreeFile, btreeHeader); err != nil {
			return err
		}
	}
	return nil
}

func verifyManifestEventLogSidecars(dir string, manifest *Manifest) error {
	if manifest == nil {
		return nil
	}
	refs := eventLogRefs(manifest)
	for _, indexRef := range eventLogIndexRefs(manifest) {
		if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, indexRef, refs); err != nil {
			return err
		}
	}
	return nil
}

func verifyManifestSegmentRef(dir string, ref SegmentRef, opts VerifyManifestOptions) error {
	if opts.RequireChecksums && strings.TrimSpace(ref.Checksum) == "" {
		return fmt.Errorf("snapshots: segment %q missing required checksum", ref.Path)
	}
	checked, err := CheckRegisteredSegment(dir, ref)
	if err != nil {
		return err
	}
	if checked {
		return nil
	}
	if opts.RequireRegistered {
		return fmt.Errorf("snapshots: segment %q has no registered checker for %s/%s", ref.Path, ref.NormalizedDataset(), ref.Kind)
	}
	return checkSegmentFileMetadata(dir, ref, opts.RequireChecksums)
}

func checkSegmentFileMetadata(dir string, ref SegmentRef, requireChecksum bool) error {
	path := filepath.Join(dir, ref.Path)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.IsDir() {
		return fmt.Errorf("snapshots: segment %q is a directory", ref.Path)
	}
	if ref.Size != 0 && uint64(stat.Size()) != ref.Size {
		return fmt.Errorf("snapshots: segment %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
	}
	if strings.TrimSpace(ref.Checksum) == "" {
		if requireChecksum {
			return fmt.Errorf("snapshots: segment %q missing required checksum", ref.Path)
		}
		return nil
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	got := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(got, ref.Checksum) {
		return fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
	}
	return nil
}
