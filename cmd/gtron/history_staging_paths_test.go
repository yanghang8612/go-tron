package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestResolveHistoryStagingPathsDisjointAndSymlinkSafe(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	for _, path := range []string{chainDataDir(dataDir), stateSnapshotsDir(dataDir)} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	paths, err := resolveHistoryStagingPaths(dataDir, "", "", false)
	if err != nil {
		t.Fatal(err)
	}
	canonicalParent, err := filepath.EvalSymlinks(filepath.Join(dataDir, "gtron"))
	if err != nil {
		t.Fatal(err)
	}
	if paths.Target != filepath.Join(canonicalParent, "history-staging") {
		t.Fatalf("default staging target = %q", paths.Target)
	}
	if _, err := resolveHistoryStagingPaths(dataDir, chainDataDir(dataDir), "", true); err == nil {
		t.Fatal("source and target overlap was accepted")
	}
	if _, err := resolveHistoryStagingPaths(dataDir, filepath.Join(chainDataDir(dataDir), "stage"), "", false); err == nil {
		t.Fatal("target inside source was accepted")
	}
	if _, err := resolveHistoryStagingPaths(dataDir, filepath.Join(stateSnapshotsDir(dataDir), "stage"), "", false); err == nil {
		t.Fatal("target inside cold directory was accepted")
	}
	link := filepath.Join(root, "alias")
	if err := os.Symlink(chainDataDir(dataDir), link); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveHistoryStagingPaths(dataDir, link, "", true); err == nil {
		t.Fatal("symlink alias of source was accepted")
	}
	missingTarget := filepath.Join(root, "missing", "stage")
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := resolveHistoryStagingPaths(dataDir, missingTarget, "", false)
	if err != nil || fresh.Target != filepath.Join(canonicalRoot, "missing", "stage") {
		t.Fatalf("fresh target with missing parent = %+v, %v", fresh, err)
	}
	if _, err := os.Stat(filepath.Dir(missingTarget)); !os.IsNotExist(err) {
		t.Fatalf("fresh path resolution created target parent: %v", err)
	}
	if _, err := resolveHistoryStagingPaths(dataDir, missingTarget, "", true); err == nil {
		t.Fatal("existing staged reader accepted a missing target")
	}
	if _, err := resolveHistoryStagingPaths(dataDir, filepath.Join(link, "missing", "stage"), "", false); err == nil {
		t.Fatal("missing target below source symlink escaped overlap check")
	}
}

func TestResolveHistoryStagingPathsFreshSourceMayBeAbsent(t *testing.T) {
	root := t.TempDir()
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	datadir := filepath.Join(root, "new", "datadir")
	paths, err := resolveHistoryStagingPathsWithSource(datadir, "", "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	canonicalDatadir := filepath.Join(canonicalRoot, "new", "datadir")
	if paths.Source != chainDataDir(canonicalDatadir) || paths.Target != defaultHistoryStagingDir(canonicalDatadir) ||
		paths.Cold != stateSnapshotsDir(canonicalDatadir) {
		t.Fatalf("fresh paths = %+v", paths)
	}
	if _, err := os.Stat(filepath.Join(root, "new")); !os.IsNotExist(err) {
		t.Fatalf("fresh path resolution created source ancestor: %v", err)
	}
	if _, err := resolveHistoryStagingPathsWithSource(datadir, "", "", true, true); err == nil {
		t.Fatal("existing staged reader accepted absent source and target")
	}
}

func TestHistoryStagingStoreIDBindsPathAndChain(t *testing.T) {
	var genesis common.Hash
	genesis[0] = 1
	paths := historyStagingPaths{Source: "/data/source", Target: "/data/stage", Cold: "/data/cold"}
	identity := historyStagingIdentity(paths, genesis, 123)
	if identity.SourceID == identity.TargetID || identity.SourceID == ([32]byte{}) {
		t.Fatal("source and target IDs were not distinct")
	}
	changed := historyStagingIdentity(historyStagingPaths{Source: paths.Source, Target: paths.Target + "-other"}, genesis, 123)
	if identity.SourceID != changed.SourceID || identity.TargetID == changed.TargetID {
		t.Fatal("store IDs did not bind the canonical paths")
	}
	changed = historyStagingIdentity(paths, genesis, 124)
	if identity.SourceID == changed.SourceID {
		t.Fatal("store ID did not bind network ID")
	}
}
