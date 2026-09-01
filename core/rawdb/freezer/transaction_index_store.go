package freezer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
)

const (
	transactionIndexManifestVersion = uint32(1)
	transactionIndexDirectoryName   = "tx-index"
	transactionIndexRunsDirectory   = "runs"
	transactionIndexManifestName    = "manifest.json"
	// TransactionIndexCompactionLeafBlocks is the bounded online build unit.
	// Larger fused runs start at the equivalent geometric compaction level so
	// they do not merge prematurely with a single maintenance leaf.
	TransactionIndexCompactionLeafBlocks = uint64(8_192)
)

type transactionIndexManifest struct {
	Version uint32                        `json:"version"`
	Runs    []transactionIndexManifestRun `json:"runs"`
}

type transactionIndexManifestRun struct {
	File               string `json:"file"`
	StartBlock         uint64 `json:"start_block"`
	EndBlock           uint64 `json:"end_block"`
	Rows               uint64 `json:"rows"`
	CompactionLevel    uint32 `json:"compaction_level,omitempty"`
	CompactionLevelSet bool   `json:"compaction_level_set,omitempty"`
}

// TransactionIndexStore is the manifest-selected set of immutable runs. Runs
// currently cover disjoint contiguous block ranges. Lookup probes every run;
// later geometric merging keeps that fan-out logarithmically bounded.
type TransactionIndexStore struct {
	base     string
	runs     []*TransactionIndexRun
	coverage uint64
}

type transactionIndexOrphanCleanupResult struct {
	Files int
	Bytes uint64
}

// TransactionIndexRunPath returns the canonical destination for a run. The
// filename includes its half-open block range and is safe to publish in the
// transaction-index manifest after BuildTransactionIndexRun succeeds.
func TransactionIndexRunPath(ancientDir string, startBlock, endBlock uint64) string {
	name := fmt.Sprintf("%020d-%020d.gtxi", startBlock, endBlock)
	return filepath.Join(ancientDir, transactionIndexDirectoryName, transactionIndexRunsDirectory, name)
}

// OpenTransactionIndexStore opens only runs referenced by the atomic manifest.
// Missing manifests are a valid empty store; unreferenced temporary/run files
// are ignored after an interrupted build.
func OpenTransactionIndexStore(ancientDir string) (*TransactionIndexStore, error) {
	base := filepath.Join(ancientDir, transactionIndexDirectoryName)
	store := &TransactionIndexStore{base: base}
	manifest, err := readTransactionIndexManifest(base)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	if err := validateTransactionIndexManifest(manifest); err != nil {
		return nil, err
	}
	expectedStart := uint64(0)
	for _, declared := range manifest.Runs {
		if declared.StartBlock != expectedStart || declared.EndBlock <= declared.StartBlock {
			store.Close()
			return nil, fmt.Errorf("transaction index manifest: run range [%d,%d) does not continue at %d", declared.StartBlock, declared.EndBlock, expectedStart)
		}
		if filepath.Base(declared.File) != declared.File {
			store.Close()
			return nil, fmt.Errorf("transaction index manifest: invalid run filename %q", declared.File)
		}
		run, err := OpenTransactionIndexRun(filepath.Join(base, transactionIndexRunsDirectory, declared.File))
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("open transaction index run %q: %w", declared.File, err)
		}
		if run.StartBlock() != declared.StartBlock || run.EndBlock() != declared.EndBlock || run.Rows() != declared.Rows {
			run.Close()
			store.Close()
			return nil, fmt.Errorf("transaction index manifest: run %q metadata mismatch", declared.File)
		}
		store.runs = append(store.runs, run)
		expectedStart = declared.EndBlock
	}
	store.coverage = expectedStart
	return store, nil
}

// cleanupUnreferencedTransactionIndexRuns removes immutable runs made obsolete
// by an already-published manifest. The selected store must have opened and
// validated every manifest entry before this destructive step is reached.
//
// A run extending beyond the published coverage is an interrupted/future build
// and is deliberately retained for recovery. Unknown names and non-regular
// entries are also left untouched.
func cleanupUnreferencedTransactionIndexRuns(ancientDir string, selected *TransactionIndexStore) (transactionIndexOrphanCleanupResult, error) {
	return cleanupUnreferencedTransactionIndexRunsContext(context.Background(), ancientDir, selected)
}

func cleanupUnreferencedTransactionIndexRunsContext(ctx context.Context, ancientDir string, selected *TransactionIndexStore) (transactionIndexOrphanCleanupResult, error) {
	var result transactionIndexOrphanCleanupResult
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if selected == nil || selected.Coverage() == 0 {
		return result, nil
	}
	if err := validateSelectedTransactionIndexStoreContext(ctx, ancientDir, selected); err != nil {
		return result, err
	}
	runsDir := filepath.Join(ancientDir, transactionIndexDirectoryName, transactionIndexRunsDirectory)
	dir, err := os.Open(runsDir)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("transaction index orphan cleanup: open runs: %w", err)
	}
	defer dir.Close()
	active := make(map[string]struct{}, len(selected.runs))
	for _, run := range selected.runs {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		active[filepath.Base(run.Path())] = struct{}{}
	}
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		entries, readErr := dir.ReadDir(256)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			name := entry.Name()
			if _, ok := active[name]; ok {
				continue
			}
			_, end, ok := parseTransactionIndexRunFilename(name)
			if !ok || end > selected.Coverage() {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return result, fmt.Errorf("transaction index orphan cleanup: stat %q: %w", name, err)
			}
			if !info.Mode().IsRegular() {
				continue
			}
			if err := os.Remove(filepath.Join(runsDir, name)); err != nil {
				return result, fmt.Errorf("transaction index orphan cleanup: remove %q: %w", name, err)
			}
			result.Files++
			if info.Size() > 0 {
				result.Bytes += uint64(info.Size())
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return result, fmt.Errorf("transaction index orphan cleanup: read runs: %w", readErr)
		}
	}
	if result.Files > 0 {
		if err := syncDir(runsDir); err != nil {
			return result, fmt.Errorf("transaction index orphan cleanup: sync runs directory: %w", err)
		}
	}
	return result, nil
}

func validateSelectedTransactionIndexStore(ancientDir string, selected *TransactionIndexStore) error {
	return validateSelectedTransactionIndexStoreContext(context.Background(), ancientDir, selected)
}

func validateSelectedTransactionIndexStoreContext(ctx context.Context, ancientDir string, selected *TransactionIndexStore) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if selected == nil {
		return errors.New("transaction index orphan cleanup: selected store is nil")
	}
	wantBase := filepath.Clean(filepath.Join(ancientDir, transactionIndexDirectoryName))
	if filepath.Clean(selected.base) != wantBase {
		return errors.New("transaction index orphan cleanup: selected store belongs to another database")
	}
	manifest, err := readTransactionIndexManifest(filepath.Join(ancientDir, transactionIndexDirectoryName))
	if errors.Is(err, os.ErrNotExist) && len(selected.runs) == 0 && selected.Coverage() == 0 {
		return nil
	}
	if err != nil {
		return fmt.Errorf("transaction index orphan cleanup: read selected manifest: %w", err)
	}
	if err := validateTransactionIndexManifestContext(ctx, manifest); err != nil {
		return err
	}
	if len(manifest.Runs) != len(selected.runs) {
		return errors.New("transaction index orphan cleanup: selected store is stale")
	}
	for i, declared := range manifest.Runs {
		if err := ctx.Err(); err != nil {
			return err
		}
		run := selected.runs[i]
		if run == nil || filepath.Base(run.Path()) != declared.File || run.StartBlock() != declared.StartBlock ||
			run.EndBlock() != declared.EndBlock || run.Rows() != declared.Rows {
			return errors.New("transaction index orphan cleanup: selected store does not match manifest")
		}
	}
	if len(manifest.Runs) == 0 {
		if selected.Coverage() != 0 {
			return errors.New("transaction index orphan cleanup: empty manifest has non-zero selected coverage")
		}
		return nil
	}
	if selected.Coverage() != manifest.Runs[len(manifest.Runs)-1].EndBlock {
		return errors.New("transaction index orphan cleanup: selected coverage does not match manifest")
	}
	return nil
}

func parseTransactionIndexRunFilename(name string) (uint64, uint64, bool) {
	const (
		digits = 20
		suffix = ".gtxi"
	)
	if len(name) != digits+1+digits+len(suffix) || name[digits] != '-' || name[len(name)-len(suffix):] != suffix {
		return 0, 0, false
	}
	start, err := strconv.ParseUint(name[:digits], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	end, err := strconv.ParseUint(name[digits+1:digits+1+digits], 10, 64)
	if err != nil || end <= start {
		return 0, 0, false
	}
	if filepath.Base(TransactionIndexRunPath("", start, end)) != name {
		return 0, 0, false
	}
	return start, end, true
}

// PublishTransactionIndexRun appends one durable, already-built run to the
// manifest. Publication is atomic and requires contiguous block coverage.
func PublishTransactionIndexRun(ancientDir string, result TransactionIndexBuildResult) error {
	base := filepath.Join(ancientDir, transactionIndexDirectoryName)
	if err := verifyTransactionIndexBuildResult(ancientDir, result); err != nil {
		return err
	}
	manifest, err := readTransactionIndexManifest(base)
	if errors.Is(err, os.ErrNotExist) {
		manifest = transactionIndexManifest{Version: transactionIndexManifestVersion}
	} else if err != nil {
		return err
	}
	if err := validateTransactionIndexManifest(manifest); err != nil {
		return err
	}
	expectedStart := uint64(0)
	if len(manifest.Runs) > 0 {
		expectedStart = manifest.Runs[len(manifest.Runs)-1].EndBlock
	}
	if result.StartBlock != expectedStart || result.EndBlock <= result.StartBlock {
		return fmt.Errorf("transaction index publish: run range [%d,%d) does not continue at %d", result.StartBlock, result.EndBlock, expectedStart)
	}
	manifest.Runs = append(manifest.Runs, transactionIndexManifestRun{
		File:               filepath.Base(result.Path),
		StartBlock:         result.StartBlock,
		EndBlock:           result.EndBlock,
		Rows:               result.Rows,
		CompactionLevel:    transactionIndexBaseCompactionLevel(result.EndBlock - result.StartBlock),
		CompactionLevelSet: true,
	})
	return writeTransactionIndexManifest(base, manifest)
}

func verifyTransactionIndexBuildResult(ancientDir string, result TransactionIndexBuildResult) error {
	return verifyTransactionIndexBuildResultContext(context.Background(), ancientDir, result)
}

func verifyTransactionIndexBuildResultContext(ctx context.Context, ancientDir string, result TransactionIndexBuildResult) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runsDir := filepath.Join(ancientDir, transactionIndexDirectoryName, transactionIndexRunsDirectory)
	absPath, err := filepath.Abs(result.Path)
	if err != nil {
		return err
	}
	absRuns, err := filepath.Abs(runsDir)
	if err != nil {
		return err
	}
	if filepath.Dir(absPath) != absRuns {
		return fmt.Errorf("transaction index publish: run %q is outside %q", result.Path, runsDir)
	}
	run, err := OpenTransactionIndexRun(result.Path)
	if err != nil {
		return fmt.Errorf("transaction index publish: reopen run: %w", err)
	}
	defer run.Close()
	if err := run.VerifyContext(ctx); err != nil {
		return fmt.Errorf("transaction index publish: verify run: %w", err)
	}
	if run.StartBlock() != result.StartBlock || run.EndBlock() != result.EndBlock || run.Rows() != result.Rows || run.Size() != result.FileBytes {
		return errors.New("transaction index publish: build result does not match run")
	}
	return nil
}

func writeTransactionIndexManifest(base string, manifest transactionIndexManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(base, 0755); err != nil {
		return err
	}
	return reset(filepath.Join(base, transactionIndexManifestName), data)
}

func readTransactionIndexManifest(base string) (transactionIndexManifest, error) {
	var manifest transactionIndexManifest
	data, err := os.ReadFile(filepath.Join(base, transactionIndexManifestName))
	if err != nil {
		return manifest, err
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifest, fmt.Errorf("decode transaction index manifest: %w", err)
	}
	if manifest.Version != transactionIndexManifestVersion {
		return manifest, fmt.Errorf("transaction index manifest: unsupported version %d", manifest.Version)
	}
	return manifest, nil
}

func validateTransactionIndexManifest(manifest transactionIndexManifest) error {
	return validateTransactionIndexManifestContext(context.Background(), manifest)
}

func validateTransactionIndexManifestContext(ctx context.Context, manifest transactionIndexManifest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	expectedStart := uint64(0)
	for _, run := range manifest.Runs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if run.StartBlock != expectedStart || run.EndBlock <= run.StartBlock {
			return fmt.Errorf("transaction index manifest: run range [%d,%d) does not continue at %d", run.StartBlock, run.EndBlock, expectedStart)
		}
		if filepath.Base(run.File) != run.File {
			return fmt.Errorf("transaction index manifest: invalid run filename %q", run.File)
		}
		expectedStart = run.EndBlock
	}
	return nil
}

func transactionIndexBaseCompactionLevel(blocks uint64) uint32 {
	if blocks == 0 {
		return 0
	}
	leaves := uint64(1) + (blocks-1)/TransactionIndexCompactionLeafBlocks
	return uint32(bits.Len64(leaves - 1))
}

func (s *TransactionIndexStore) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	for _, run := range s.runs {
		if err := run.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	s.runs = nil
	return errors.Join(errs...)
}

func (s *TransactionIndexStore) Coverage() uint64 {
	if s == nil {
		return 0
	}
	return s.coverage
}

func (s *TransactionIndexStore) CoversBlock(number uint64) bool {
	return s != nil && number < s.coverage
}

func (s *TransactionIndexStore) Candidates(hash [32]byte) ([]uint64, error) {
	if s == nil {
		return nil, nil
	}
	var candidates []uint64
	for i := len(s.runs) - 1; i >= 0; i-- {
		found, err := s.runs[i].Candidates(hash)
		if err != nil {
			return nil, err
		}
		for _, location := range found {
			block := transactionIndexLocationBlock(location)
			if !s.runs[i].CoversBlock(block) {
				return nil, fmt.Errorf("transaction index run %q returned block %d outside [%d,%d)", s.runs[i].Path(), block, s.runs[i].StartBlock(), s.runs[i].EndBlock())
			}
			candidates = append(candidates, location)
		}
	}
	return candidates, nil
}
