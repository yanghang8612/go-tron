package main

import (
	"flag"
	"testing"

	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

func makeSnapshotCompressionFlagSet(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.BoolFlag{
			Name:    snapshotCompressHistoryFlag.Name,
			Usage:   snapshotCompressHistoryFlag.Usage,
			Value:   snapshotCompressHistoryFlag.Value,
			EnvVars: snapshotCompressHistoryFlag.EnvVars,
		},
	}
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	for _, f := range app.Flags {
		if err := f.Apply(set); err != nil {
			t.Fatalf("apply flag: %v", err)
		}
	}
	if err := set.Parse(argv); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	return cli.NewContext(app, set, nil)
}

func withSnapshotCompressionGlobal(t *testing.T) {
	t.Helper()
	prev := statesnapshots.CompressHistorySegments
	t.Cleanup(func() {
		statesnapshots.CompressHistorySegments = prev
	})
}

func TestApplySnapshotCompressionConfig_DefaultsEnabled(t *testing.T) {
	withSnapshotCompressionGlobal(t)
	statesnapshots.CompressHistorySegments = false

	enabled := applySnapshotCompressionConfig(makeSnapshotCompressionFlagSet(t, nil))
	if !enabled || !statesnapshots.CompressHistorySegments {
		t.Fatalf("snapshot history compression = %v/%v, want enabled", enabled, statesnapshots.CompressHistorySegments)
	}
}

func TestApplySnapshotCompressionConfig_CanDisable(t *testing.T) {
	withSnapshotCompressionGlobal(t)
	statesnapshots.CompressHistorySegments = true

	enabled := applySnapshotCompressionConfig(makeSnapshotCompressionFlagSet(t, []string{"--snapshot.compress-history=false"}))
	if enabled || statesnapshots.CompressHistorySegments {
		t.Fatalf("snapshot history compression = %v/%v, want disabled", enabled, statesnapshots.CompressHistorySegments)
	}
}

func TestApplySnapshotCompressionConfig_CanReenable(t *testing.T) {
	withSnapshotCompressionGlobal(t)
	statesnapshots.CompressHistorySegments = false

	enabled := applySnapshotCompressionConfig(makeSnapshotCompressionFlagSet(t, []string{"--snapshot.compress-history=true"}))
	if !enabled || !statesnapshots.CompressHistorySegments {
		t.Fatalf("snapshot history compression = %v/%v, want enabled", enabled, statesnapshots.CompressHistorySegments)
	}
}

func TestApplySnapshotCompressionConfig_EnvironmentCanDisable(t *testing.T) {
	withSnapshotCompressionGlobal(t)
	statesnapshots.CompressHistorySegments = true
	t.Setenv("GTRON_SNAPSHOT_COMPRESS_HISTORY", "false")

	enabled := applySnapshotCompressionConfig(makeSnapshotCompressionFlagSet(t, nil))
	if enabled || statesnapshots.CompressHistorySegments {
		t.Fatalf("snapshot history compression from env = %v/%v, want disabled", enabled, statesnapshots.CompressHistorySegments)
	}
}
