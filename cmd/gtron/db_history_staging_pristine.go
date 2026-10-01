package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// verifyStoragePristine only checks the two stores. The caller has already
// proved the canonical boundary. It never admits, deletes or ignores a plan.
func (c *historyStagingCLIContext) verifyStoragePristine(source ethdb.KeyValueStore) error {
	present, err := rawdb.HistoryStagingMetadataPresent(source)
	if err != nil {
		return err
	}
	if present {
		return errors.New("history staging source contains migration metadata")
	}
	entries, err := os.ReadDir(c.paths.Target)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, entry := range entries {
		if entry.Name() != ".history-staging-migration.lock" {
			return fmt.Errorf("history staging target is not pristine: %s", entry.Name())
		}
		info, err := os.Lstat(filepath.Join(c.paths.Target, entry.Name()))
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("history staging migration lock is not a regular file")
		}
	}
	return nil
}

// verifyPristine is the strict read-only repin preflight: storage must be
// pristine and no job plan, including an unpublished temporary plan, may
// remain. No cold manifest identity is inferred or written.
func (c *historyStagingCLIContext) verifyPristine(source ethdb.KeyValueStore, datadir string) error {
	if err := c.verifyStoragePristine(source); err != nil {
		return err
	}
	planDir := historyStagingPlanDirectory(datadir)
	info, err := os.Lstat(planDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("history staging plan directory is not a regular directory")
	}
	plans, err := os.ReadDir(planDir)
	if err != nil {
		return err
	}
	if len(plans) != 0 {
		return errors.New("history staging job plan directory is not pristine")
	}
	return nil
}
