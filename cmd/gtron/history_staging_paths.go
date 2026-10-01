package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type historyStagingPaths struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Cold   string `json:"cold"`
}

func defaultHistoryStagingDir(dataDir string) string {
	return filepath.Join(dataDir, "gtron", "history-staging")
}

func canonicalHistoryStagingPath(path string, mustExist bool) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("history staging path is empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if mustExist {
		resolved, err := filepath.EvalSymlinks(abs)
		if err != nil {
			return "", err
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return "", err
		}
		if !info.IsDir() {
			return "", fmt.Errorf("history staging path is not a directory: %s", abs)
		}
		return resolved, nil
	}
	if _, err := os.Lstat(abs); err == nil {
		return canonicalHistoryStagingPath(abs, true)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	parent, err := canonicalHistoryStagingPath(filepath.Dir(abs), false)
	if err != nil {
		return "", fmt.Errorf("history staging target parent: %w", err)
	}
	return filepath.Join(parent, filepath.Base(abs)), nil
}

func historyStagingPathContains(outer, inner string) bool {
	rel, err := filepath.Rel(outer, inner)
	return err == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func resolveHistoryStagingPaths(dataDir, target, cold string, targetMustExist bool) (historyStagingPaths, error) {
	return resolveHistoryStagingPathsWithSource(dataDir, target, cold, true, targetMustExist)
}

func resolveHistoryStagingPathsWithSource(dataDir, target, cold string, sourceMustExist, targetMustExist bool) (historyStagingPaths, error) {
	if target == "" {
		target = defaultHistoryStagingDir(dataDir)
	}
	if cold == "" {
		cold = stateSnapshotsDir(dataDir)
	}
	source, err := canonicalHistoryStagingPath(chainDataDir(dataDir), sourceMustExist)
	if err != nil {
		return historyStagingPaths{}, fmt.Errorf("history staging source: %w", err)
	}
	target, err = canonicalHistoryStagingPath(target, targetMustExist)
	if err != nil {
		return historyStagingPaths{}, fmt.Errorf("history staging target: %w", err)
	}
	cold, err = canonicalHistoryStagingPath(cold, false)
	if err != nil {
		return historyStagingPaths{}, fmt.Errorf("history staging cold directory: %w", err)
	}
	paths := historyStagingPaths{Source: source, Target: target, Cold: cold}
	for _, pair := range [][2]string{{source, target}, {source, cold}, {target, cold}} {
		if historyStagingPathContains(pair[0], pair[1]) || historyStagingPathContains(pair[1], pair[0]) {
			return historyStagingPaths{}, fmt.Errorf("history staging source, target and cold directories must be disjoint: %s and %s", pair[0], pair[1])
		}
	}
	return paths, nil
}

func historyStagingStoreID(path string, genesis common.Hash, networkID uint64) [32]byte {
	h := sha256.New()
	h.Write([]byte("gtron-history-staging-store-id-v1\x00"))
	h.Write(genesis[:])
	var network [8]byte
	for i := 7; i >= 0; i-- {
		network[i] = byte(networkID)
		networkID >>= 8
	}
	h.Write(network[:])
	h.Write([]byte(path))
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func historyStagingIdentity(paths historyStagingPaths, genesis common.Hash, networkID uint64) rawdb.HistoryStagingIdentity {
	return rawdb.HistoryStagingIdentity{
		Version:     rawdb.HistoryStagingFormatVersion,
		GenesisHash: genesis,
		NetworkID:   networkID,
		SourceID:    historyStagingStoreID(paths.Source, genesis, networkID),
		TargetID:    historyStagingStoreID(paths.Target, genesis, networkID),
	}
}
