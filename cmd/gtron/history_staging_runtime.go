package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

type historyStagingReaderMarker struct {
	Version       int    `json:"version"`
	FormatVersion int    `json:"format_version"`
	BinarySHA256  string `json:"binary_sha256"`
	SourceCommit  string `json:"source_commit"`
	StopIntent    bool   `json:"stop_intent"`
	HistoryWindow uint64 `json:"history_window"`
	PruneMode     string `json:"prune_mode"`
}

type historyStagingRuntimeRequest struct {
	Enabled          bool
	Fresh            bool
	Required         bool
	Persisted        bool
	Paths            historyStagingPaths
	ExecutableSHA256 string
	HistoryWindow    uint64
	PruneMode        string
}

func readHistoryStagingReaderMarker(path string) (historyStagingReaderMarker, bool, error) {
	var marker historyStagingReaderMarker
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return marker, false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 4096 {
		return marker, false, errors.New("history staging reader marker is not a bounded regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return marker, false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil {
		return marker, false, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return marker, false, errors.New("history staging reader marker contains trailing JSON")
	}
	if marker.Version != 1 || marker.FormatVersion != 1 ||
		!historyStagingSHAPattern.MatchString(marker.BinarySHA256) || marker.SourceCommit == "" ||
		marker.HistoryWindow == 0 || (marker.PruneMode != params.HistoryModeSnap && marker.PruneMode != params.HistoryModeArchive) {
		return marker, false, errors.New("history staging reader marker identity is invalid")
	}
	return marker, true, nil
}

func resolveHistoryStagingRuntimeRequest(ctx *cli.Context, datadir, source, cold string) (historyStagingRuntimeRequest, error) {
	var request historyStagingRuntimeRequest
	markerPath := filepath.Join(filepath.Dir(datadir), "HISTORY_STAGING_READER_REQUIRED.json")
	marker, required, err := readHistoryStagingReaderMarker(markerPath)
	if err != nil {
		return request, err
	}
	persisted := false
	persistedGenesisOnly := false
	legacyStoreExists := false
	if _, err := os.Stat(filepath.Join(source, "CURRENT")); err == nil {
		legacyStoreExists = true
		store, openErr := rawdb.NewPebbleDBReadOnly(source, 16, 16)
		if openErr != nil {
			return request, openErr
		}
		identity, hasIdentity, identityErr := rawdb.ReadHistoryStagingIdentity(store)
		persisted, openErr = hasIdentity, identityErr
		if openErr == nil && persisted {
			head, present, readErr := rawdb.ReadHeadBlockHashStrict(store)
			if readErr != nil {
				openErr = readErr
			} else {
				persistedGenesisOnly = present && head == identity.GenesisHash
			}
		}
		closeErr := store.Close()
		if openErr != nil {
			return request, openErr
		}
		if closeErr != nil {
			return request, closeErr
		}
	} else if !os.IsNotExist(err) {
		return request, err
	}
	if ctx.Bool("history.staging") && legacyStoreExists && !persisted {
		return request, errors.New("existing chaindata requires offline history staging migration before enabling the staged reader")
	}
	requested := ctx.Bool("history.staging") || persisted
	if !required && !requested {
		return request, nil
	}
	if required && marker.StopIntent {
		return request, errors.New("history staging reader marker preserves inactive service intent")
	}
	sha, err := runningHistoryStagingExecutableSHA256()
	if err != nil {
		return request, err
	}
	if required && sha != marker.BinarySHA256 {
		return request, fmt.Errorf("history staging running executable %s differs from required %s", sha, marker.BinarySHA256)
	}
	paths, err := resolveHistoryStagingPathsWithSource(datadir, "", cold, persisted || required, persisted || required)
	if err != nil {
		return request, err
	}
	request = historyStagingRuntimeRequest{Enabled: true, Fresh: !required && (!persisted || persistedGenesisOnly),
		Required: required, Persisted: persisted, Paths: paths, ExecutableSHA256: sha,
		HistoryWindow: marker.HistoryWindow, PruneMode: marker.PruneMode}
	return request, nil
}

func publishFreshHistoryStagingBarrier(manager *rawdb.HistoryStagingManager, sha string, head uint64, headHash, genesisHash common.Hash) error {
	if head != 0 || headHash != genesisHash {
		return errors.New("history staging fresh barrier requires the configured genesis head")
	}
	if existing, present, err := manager.ReadHistoryStagingRouteBarrier(); err != nil {
		return err
	} else if present {
		if existing.Epoch != 1 || existing.ThroughBucket != 0 || existing.EligibleThrough != 0 {
			return errors.New("history staging fresh barrier conflicts with stored route coverage")
		}
		return nil
	}
	candidate, err := historyStagingCandidateBytes(sha)
	if err != nil {
		return err
	}
	plan := sha256.Sum256(append([]byte("gtron-history-staging-fresh-genesis-v1\x00"), genesisHash[:]...))
	barrier := rawdb.HistoryStagingRouteBarrier{Version: rawdb.HistoryStagingFormatVersion,
		Epoch: 1, ThroughBucket: 0, EligibleThrough: 0, PlanDigest: plan, CandidateSHA: candidate}
	return manager.PublishHistoryStagingRouteBarrier(context.Background(), barrier, func() error {
		current, err := manager.CurrentEpoch()
		if err != nil || current != 1 {
			return errors.New("history staging fresh epoch changed before barrier")
		}
		route, present, err := manager.ReadRoute(0)
		if err != nil || !present || route.Epoch != 1 || route.Owner != rawdb.HistoryStagingOwnerSource {
			return errors.New("history staging fresh genesis route is incomplete")
		}
		return nil
	})
}

func historyStagingRuntimeCacheSplit(total int) (hot, stage int, err error) {
	if total < 32 {
		return 0, 0, errors.New("history staging requires --db.cache >= 32 MiB")
	}
	stage = total / 8
	if stage < 16 {
		stage = 16
	}
	if stage > 512 {
		stage = 512
	}
	return total - stage, stage, nil
}

func validateHistoryStagingRuntimeConfig(cfg *params.ChainConfig) error {
	if cfg == nil || !cfg.HistoryEnabled || cfg.EffectiveHistoryPruneWindow() == 0 {
		return errors.New("history staging requires enabled temporal history and a positive retention window")
	}
	switch cfg.EffectiveHistoryMode() {
	case params.HistoryModeSnap, params.HistoryModeArchive:
		return nil
	default:
		return fmt.Errorf("history staging requires snap or archive history mode, got %q", cfg.EffectiveHistoryMode())
	}
}

// Both Pebble engines must retain the same free-space reserve. If their
// directories share a filesystem, taking the minimum observes that one
// filesystem once; if they differ, neither engine can mask the other's limit.
func historyStagingMinimumFreeBytes(source, target string) (uint64, error) {
	hot, err := offlineSpaceAvailable(source, 0, 0)
	if err != nil {
		return 0, err
	}
	// A dry-run plan must not create its target store. Observe the nearest
	// existing ancestor then; after creation, observe the target itself so an
	// independently mounted staging filesystem is measured correctly.
	stagePath := target
	for {
		if _, err := os.Stat(stagePath); err == nil {
			break
		} else if !os.IsNotExist(err) {
			return 0, err
		}
		parent := filepath.Dir(stagePath)
		if parent == stagePath {
			return 0, errors.New("history staging target has no existing filesystem ancestor")
		}
		stagePath = parent
	}
	stage, err := offlineSpaceAvailable(stagePath, 0, 0)
	if err != nil {
		return 0, err
	}
	if hot < stage {
		return hot, nil
	}
	return stage, nil
}
