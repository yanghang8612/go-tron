//go:build integration

package rawdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/types"
)

// TestRecoverSyncStagedBlockFromSSTables scans physical Pebble SSTables for
// obsolete versions of one sync-staged body. It deliberately does not open the
// live Pebble DB, so an operator can use it while the node still owns LOCK.
//
// Required environment:
//
//   - GTRON_SST_DIR: live Pebble directory containing *.sst
//   - GTRON_RECOVER_BLOCK: staged block number
//   - GTRON_RECOVER_OUT: directory for recovered raw block candidates
//
// The test validates each value as a TRON block and only writes candidates
// whose embedded block number matches the requested key.
func TestRecoverSyncStagedBlockFromSSTables(t *testing.T) {
	sstDir := os.Getenv("GTRON_SST_DIR")
	blockText := os.Getenv("GTRON_RECOVER_BLOCK")
	outDir := os.Getenv("GTRON_RECOVER_OUT")
	if sstDir == "" || blockText == "" || outDir == "" {
		t.Skip("GTRON_SST_DIR, GTRON_RECOVER_BLOCK, and GTRON_RECOVER_OUT are required")
	}
	blockNum, err := strconv.ParseUint(blockText, 10, 64)
	if err != nil {
		t.Fatalf("parse GTRON_RECOVER_BLOCK: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(sstDir, "*.sst"))
	if err != nil {
		t.Fatalf("glob SSTables: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no SSTables found in %s", sstDir)
	}
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		t.Fatalf("create recovery output: %v", err)
	}

	target := syncStagedBlockKey(blockNum)
	jobs := make(chan string)
	var found atomic.Int64
	var scanned atomic.Int64
	var outputMu sync.Mutex
	var outputs []string
	workerCount := 16
	if len(files) < workerCount {
		workerCount = len(files)
	}
	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				scanned.Add(1)
				file, err := vfs.Default.Open(path)
				if err != nil {
					continue
				}
				readable, err := sstable.NewSimpleReadable(file)
				if err != nil {
					_ = file.Close()
					continue
				}
				reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
				if err != nil {
					_ = readable.Close()
					continue
				}
				iter, err := reader.NewIter(target, nil)
				if err != nil {
					_ = reader.Close()
					continue
				}
				key, lazy := iter.SeekGE(target, 0)
				if key != nil && bytes.Equal(key.UserKey, target) {
					value, _, valueErr := lazy.Value(nil)
					if valueErr == nil && len(value) > 0 {
						block, decodeErr := types.UnmarshalBlock(value)
						if decodeErr == nil && block.Number() == blockNum {
							name := fmt.Sprintf("block-%d-%x-%s.bin", blockNum, block.Hash(), filepath.Base(path))
							output := filepath.Join(outDir, name)
							if writeErr := os.WriteFile(output, value, 0o600); writeErr == nil {
								found.Add(1)
								outputMu.Lock()
								outputs = append(outputs, output)
								outputMu.Unlock()
							}
						}
					}
				}
				_ = iter.Close()
				_ = reader.Close()
			}
		}()
	}
	for _, path := range files {
		jobs <- path
	}
	close(jobs)
	wg.Wait()
	for _, output := range outputs {
		t.Logf("recovered staged block candidate: %s", output)
	}
	if found.Load() == 0 {
		t.Fatalf("scanned %d SSTables but found no recoverable staged block %d", scanned.Load(), blockNum)
	}
	t.Logf("recovered %d staged block candidate(s) after scanning %d SSTables", found.Load(), scanned.Load())
}
