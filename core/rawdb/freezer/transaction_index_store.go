package freezer

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	transactionIndexManifestVersion = uint32(1)
	transactionIndexDirectoryName   = "tx-index"
	transactionIndexRunsDirectory   = "runs"
	transactionIndexManifestName    = "manifest.json"
)

type transactionIndexManifest struct {
	Version uint32                        `json:"version"`
	Runs    []transactionIndexManifestRun `json:"runs"`
}

type transactionIndexManifestRun struct {
	File       string `json:"file"`
	StartBlock uint64 `json:"start_block"`
	EndBlock   uint64 `json:"end_block"`
	Rows       uint64 `json:"rows"`
}

// TransactionIndexStore is the manifest-selected set of immutable runs. Runs
// currently cover disjoint contiguous block ranges. Lookup probes every run;
// later geometric merging keeps that fan-out logarithmically bounded.
type TransactionIndexStore struct {
	base     string
	runs     []*TransactionIndexRun
	coverage uint64
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

// PublishTransactionIndexRun appends one durable, already-built run to the
// manifest. Publication is atomic and requires contiguous block coverage.
func PublishTransactionIndexRun(ancientDir string, result TransactionIndexBuildResult) error {
	base := filepath.Join(ancientDir, transactionIndexDirectoryName)
	runsDir := filepath.Join(base, transactionIndexRunsDirectory)
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
	if err := run.Verify(); err != nil {
		run.Close()
		return fmt.Errorf("transaction index publish: verify run: %w", err)
	}
	if run.StartBlock() != result.StartBlock || run.EndBlock() != result.EndBlock || run.Rows() != result.Rows || run.Size() != result.FileBytes {
		run.Close()
		return errors.New("transaction index publish: build result does not match run")
	}
	if err := run.Close(); err != nil {
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
		File:       filepath.Base(result.Path),
		StartBlock: result.StartBlock,
		EndBlock:   result.EndBlock,
		Rows:       result.Rows,
	})
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
	expectedStart := uint64(0)
	for _, run := range manifest.Runs {
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
