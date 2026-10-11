package main

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

var historyStoreFlag = &cli.StringFlag{
	Name:  "history.store",
	Usage: "Independent history database directory (default: datadir/gtron/history)",
}

func directHistoryPath(ctx *cli.Context, datadir string) string {
	if path := ctx.String(historyStoreFlag.Name); path != "" {
		return path
	}
	return filepath.Join(filepath.Dir(chainDataDir(datadir)), "history")
}

// Startup reads two format bits, not a history inventory or semantic proof.
// Existing stores are converted once by db move-history before this path runs.
func openDirectHistoryRuntime(ctx *cli.Context, datadir, hotPath string, cache, handles int, tune rawdb.PebbleOptions) (*rawdb.DirectHistoryStore, string, error) {
	path := directHistoryPath(ctx, datadir)
	if err := rawdb.ValidateHistoryStagingPaths(hotPath, path, snapshotDir(ctx, datadir)); err != nil {
		return nil, "", err
	}
	hotCache, historyCache, err := historyStagingRuntimeCacheSplit(cache)
	if err != nil {
		return nil, "", err
	}
	hot, err := rawdb.NewPebbleDBWithOptions(hotPath, hotCache, handles, tune)
	if err != nil {
		return nil, "", err
	}
	marked, err := rawdb.DirectHistoryLayoutInitialized(hot)
	if err != nil {
		hot.Close()
		return nil, "", err
	}
	if !marked {
		_, hasHead, readErr := rawdb.ReadHeadBlockHashStrict(hot)
		if readErr != nil || hasHead {
			hot.Close()
			return nil, "", errors.Join(errors.New("existing database needs one offline db move-history conversion before startup"), readErr)
		}
	}
	history, err := rawdb.NewDirectHistoryPebbleDB(path, historyCache, max(32, handles/8), false)
	if err != nil {
		hot.Close()
		return nil, "", fmt.Errorf("open history database: %w", err)
	}
	historyMarked, err := rawdb.DirectHistoryLayoutInitialized(history)
	if err == nil {
		err = rawdb.EnsureDirectHistoryPair(hot, history, !marked)
	}
	if err == nil && marked && !historyMarked {
		err = errors.New("configured history database is missing its format marker")
	}
	if err == nil && !marked {
		err = rawdb.MarkDirectHistoryLayout(history)
		if err == nil {
			err = history.SyncKeyValue()
		}
		if err == nil {
			err = rawdb.MarkDirectHistoryLayout(hot)
		}
		if err == nil {
			err = hot.SyncKeyValue()
		}
	}
	if err != nil {
		history.Close()
		hot.Close()
		return nil, "", err
	}
	return rawdb.NewDirectHistoryStore(hot, history), path, nil
}
