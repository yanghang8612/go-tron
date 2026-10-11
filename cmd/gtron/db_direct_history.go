package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

func dbMoveHistoryCommand() *cli.Command {
	return &cli.Command{
		Name:  "move-history",
		Usage: "Offline direct history move; resume by rerunning, without semantic certification",
		Flags: []cli.Flag{dataDirFlag, snapshotDirFlag, historyStoreFlag, dbCacheFlag, dbHandlesFlag,
			&cli.StringFlag{Name: "legacy-staging-dir", Usage: "Old staging database (default: datadir/gtron/history-staging)"},
			&cli.IntFlag{Name: "page-mib", Value: 64, Usage: "Copy and immediately delete each page after destination sync"},
			&cli.BoolFlag{Name: "compact", Value: true, Usage: "Compact moved source ranges after conversion"},
		},
		Action: dbMoveHistory,
	}
}

func dbDirectHistoryCapabilityCommand() *cli.Command {
	return &cli.Command{Name: "direct-history-capability", Action: func(ctx *cli.Context) error {
		return json.NewEncoder(ctx.App.Writer).Encode(map[string]any{"direct_history": true, "format_version": 1})
	}}
}

func dbMoveHistory(ctx *cli.Context) error {
	datadir := ctx.String(dataDirFlag.Name)
	hotPath, historyPath := chainDataDir(datadir), directHistoryPath(ctx, datadir)
	legacyPath := ctx.String("legacy-staging-dir")
	if legacyPath == "" {
		legacyPath = filepath.Join(filepath.Dir(hotPath), "history-staging")
	}
	if err := rawdb.ValidateHistoryStagingPaths(hotPath, historyPath, snapshotDir(ctx, datadir)); err != nil {
		return err
	}
	if err := rawdb.ValidateHistoryStagingPaths(hotPath, legacyPath, historyPath); err != nil {
		return err
	}
	page := ctx.Int("page-mib")
	if page < 1 || page > 512 {
		return errors.New("page-mib must be in [1,512]")
	}
	hot, err := rawdb.NewPebbleDB(hotPath, max(32, ctx.Int(dbCacheFlag.Name)/2), max(32, ctx.Int(dbHandlesFlag.Name)))
	if err != nil {
		return err
	}
	defer hot.Close()
	var legacy ethdb.KeyValueStore
	if _, err := os.Stat(filepath.Join(legacyPath, "CURRENT")); err == nil {
		legacy, err = rawdb.NewHistoryStagingPebbleDB(legacyPath, 32, 64, false)
		if err != nil {
			return err
		}
		defer legacy.Close()
	} else if !os.IsNotExist(err) {
		return err
	}
	history, err := rawdb.NewDirectHistoryPebbleDB(historyPath, max(32, ctx.Int(dbCacheFlag.Name)/2), max(32, ctx.Int(dbHandlesFlag.Name)/2), false)
	if err != nil {
		return err
	}
	defer history.Close()
	// Only clean an old target whose identity was actually matched this run.
	// Completed reruns can have no old identity left in hot.
	cleanupLegacy := false
	identity, hasLegacy, err := rawdb.ReadHistoryStagingIdentity(hot)
	if err != nil {
		return err
	}
	if hasLegacy && legacy != nil {
		manager, err := rawdb.NewHistoryStagingManager(hot, legacy, identity)
		if err != nil {
			return err
		}
		if err := manager.VerifyIdentity(); err != nil {
			return err
		}
		cleanupLegacy = true
	}
	enc := json.NewEncoder(ctx.App.Writer)
	start := time.Now()
	if err := enc.Encode(map[string]any{"phase": "move", "source": hotPath, "history": historyPath, "legacy": legacyPath}); err != nil {
		return err
	}
	if err := rawdb.MoveDirectHistory(ctx.Context, hot, legacy, history, page<<20, func(p rawdb.DirectHistoryMoveProgress) error { return enc.Encode(p) }); err != nil {
		return err
	}
	old := rawdb.DirectHistoryLegacyStagingMetadataRange()
	if err := hot.DeleteRange(old.Start, old.End); err != nil {
		return err
	}
	if err := hot.SyncKeyValue(); err != nil {
		return err
	}
	if cleanupLegacy {
		if err := legacy.DeleteRange(old.Start, old.End); err != nil {
			return err
		}
		if err := legacy.SyncKeyValue(); err != nil {
			return err
		}
	}
	if ctx.Bool("compact") {
		for _, r := range append(rawdb.DirectHistoryRanges(), old) {
			if err := enc.Encode(map[string]any{"phase": "compact", "family": r.Name}); err != nil {
				return err
			}
			if err := hot.Compact(r.Start, r.End); err != nil {
				return fmt.Errorf("compact %s: %w", r.Name, err)
			}
		}
		if cleanupLegacy {
			if err := legacy.Compact(nil, nil); err != nil {
				return err
			}
		}
	}
	return enc.Encode(map[string]any{"phase": "complete", "elapsed_seconds": time.Since(start).Seconds(), "history": historyPath})
}
