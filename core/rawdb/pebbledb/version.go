package pebbledb

import (
	"fmt"
	"runtime"

	pebblev1 "github.com/cockroachdb/pebble"
	v1bloom "github.com/cockroachdb/pebble/bloom"
	pebblev2 "github.com/cockroachdb/pebble/v2"
	v2vfs "github.com/cockroachdb/pebble/v2/vfs"
	v1vfs "github.com/cockroachdb/pebble/vfs"
	"github.com/ethereum/go-ethereum/log"
)

// pebbleV2BridgeFormat is the oldest on-disk format supported by Pebble v2.
// Pebble v1 also understands it, so switching the runtime does not make the
// database unreadable by the previous go-tron binary.
const pebbleV2BridgeFormat = pebblev2.FormatFlushableIngest

func peekPebbleFormat(file string) (exists bool, version uint64, err error) {
	desc, v2err := pebblev2.Peek(file, v2vfs.Default)
	if v2err == nil && desc.Exists {
		return true, uint64(desc.FormatMajorVersion), nil
	}
	// FormatMostCompatible uses the legacy CURRENT-file layout. Pebble v2's
	// Peek does not recognize it, so fall back to v1 before deciding that the
	// directory is empty.
	desc1, v1err := pebblev1.Peek(file, v1vfs.Default)
	if v1err != nil {
		if v2err != nil {
			return false, 0, v2err
		}
		return false, 0, v1err
	}
	if !desc1.Exists {
		return false, 0, nil
	}
	return true, uint64(desc1.FormatMajorVersion), nil
}

func needsPebbleV1(file string) (bool, error) {
	exists, version, err := peekPebbleFormat(file)
	if err != nil || !exists {
		return false, err
	}
	return pebblev2.FormatMajorVersion(version) < pebbleV2BridgeFormat, nil
}

// upgradePebbleV1 performs a manifest-format ratchet only. It does not rewrite
// SSTables. The target is deliberately shared by Pebble v1 and v2, and the
// final v2 open verifies the bridge before the production database is opened.
func upgradePebbleV1(file string, tune Options) error {
	exists, version, err := peekPebbleFormat(file)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("Pebble database not found at %s", file)
	}
	if pebblev2.FormatMajorVersion(version) >= pebbleV2BridgeFormat {
		return nil
	}

	logger := log.New("database", file)
	logger.Info("Upgrading Pebble database format for v2 runtime",
		"from", version, "to", pebbleV2BridgeFormat)
	opts := &pebblev1.Options{
		Comparer:                 exactPointComparerV1(),
		MaxConcurrentCompactions: func() int { return compactionConcurrency(runtime.GOMAXPROCS(0)) },
		MemTableSize:             tune.MemTableSizeBytes,
		LBaseMaxBytes:            tune.LBaseMaxBytes,
		L0CompactionThreshold:    tune.L0CompactionThreshold,
		L0StopWritesThreshold:    tune.L0StopWritesThreshold,
		Levels: []pebblev1.LevelOptions{
			{TargetFileSize: tune.TargetFileSizeBytes, Compression: pebblev1.NoCompression, FilterPolicy: v1bloom.FilterPolicy(10)},
			{TargetFileSize: tune.TargetFileSizeBytes << 1, Compression: pebblev1.NoCompression, FilterPolicy: v1bloom.FilterPolicy(10)},
			{TargetFileSize: tune.TargetFileSizeBytes << 2, Compression: pebblev1.NoCompression, FilterPolicy: v1bloom.FilterPolicy(10)},
			{TargetFileSize: tune.TargetFileSizeBytes << 3, Compression: pebblev1.NoCompression, FilterPolicy: v1bloom.FilterPolicy(10)},
			{TargetFileSize: tune.TargetFileSizeBytes << 4, Compression: pebblev1.NoCompression, FilterPolicy: v1bloom.FilterPolicy(10)},
			{TargetFileSize: tune.TargetFileSizeBytes << 5, Compression: pebblev1.NoCompression, FilterPolicy: v1bloom.FilterPolicy(10)},
			{TargetFileSize: tune.TargetFileSizeBytes << 6},
		},
		Logger: panicLoggerV1{},
	}
	db, err := pebblev1.Open(file, opts)
	if err != nil {
		return fmt.Errorf("open Pebble v1 database: %w", err)
	}
	if err := db.RatchetFormatMajorVersion(pebblev1.FormatFlushableIngest); err != nil {
		db.Close()
		return fmt.Errorf("ratchet Pebble format: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close Pebble v1 database after ratchet: %w", err)
	}

	verify, err := pebblev2.Open(file, &pebblev2.Options{
		Comparer:           exactPointComparer,
		FormatMajorVersion: pebbleV2BridgeFormat,
		Logger:             panicLogger{},
	})
	if err != nil {
		return fmt.Errorf("verify Pebble v2 bridge: %w", err)
	}
	if err := verify.Close(); err != nil {
		return fmt.Errorf("close Pebble v2 bridge verification: %w", err)
	}
	logger.Info("Pebble v2 bridge upgrade complete", "format", pebbleV2BridgeFormat)
	return nil
}

func exactPointComparerV1() *pebblev1.Comparer {
	comparer := *pebblev1.DefaultComparer
	comparer.Split = func(key []byte) int { return len(key) }
	return &comparer
}

type panicLoggerV1 struct{}

func (panicLoggerV1) Infof(string, ...interface{})  {}
func (panicLoggerV1) Errorf(string, ...interface{}) {}
func (panicLoggerV1) Fatalf(format string, args ...interface{}) {
	panic(fmt.Errorf("fatal: "+format, args...))
}
