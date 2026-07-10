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
		&cli.BoolFlag{
			Name:    snapshotCompressLatestFlag.Name,
			Usage:   snapshotCompressLatestFlag.Usage,
			Value:   snapshotCompressLatestFlag.Value,
			EnvVars: snapshotCompressLatestFlag.EnvVars,
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
	prevHistory := statesnapshots.CompressHistorySegments
	prevLatest := statesnapshots.CompressLatestSegments
	t.Cleanup(func() {
		statesnapshots.CompressHistorySegments = prevHistory
		statesnapshots.CompressLatestSegments = prevLatest
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

func TestApplySnapshotLatestCompressionConfig_DefaultsEnabled(t *testing.T) {
	withSnapshotCompressionGlobal(t)
	statesnapshots.CompressLatestSegments = false

	enabled := applySnapshotLatestCompressionConfig(makeSnapshotCompressionFlagSet(t, nil))
	if !enabled || !statesnapshots.CompressLatestSegments {
		t.Fatalf("snapshot latest compression = %v/%v, want enabled", enabled, statesnapshots.CompressLatestSegments)
	}
}

func TestApplySnapshotLatestCompressionConfig_CanDisable(t *testing.T) {
	withSnapshotCompressionGlobal(t)
	statesnapshots.CompressLatestSegments = true

	enabled := applySnapshotLatestCompressionConfig(makeSnapshotCompressionFlagSet(t, []string{"--snapshot.compress-latest=false"}))
	if enabled || statesnapshots.CompressLatestSegments {
		t.Fatalf("snapshot latest compression = %v/%v, want disabled", enabled, statesnapshots.CompressLatestSegments)
	}
}

func TestApplySnapshotLatestCompressionConfig_EnvironmentCanDisable(t *testing.T) {
	withSnapshotCompressionGlobal(t)
	statesnapshots.CompressLatestSegments = true
	t.Setenv("GTRON_SNAPSHOT_COMPRESS_LATEST", "false")

	enabled := applySnapshotLatestCompressionConfig(makeSnapshotCompressionFlagSet(t, nil))
	if enabled || statesnapshots.CompressLatestSegments {
		t.Fatalf("snapshot latest compression from env = %v/%v, want disabled", enabled, statesnapshots.CompressLatestSegments)
	}
}

func TestApplySnapshotCompressionConfigs_AppliesBothEmissionGates(t *testing.T) {
	withSnapshotCompressionGlobal(t)
	ctx := makeSnapshotCompressionFlagSet(t, []string{
		"--snapshot.compress-history=false",
		"--snapshot.compress-latest=false",
	})

	history, latest := applySnapshotCompressionConfigs(ctx)
	if history || latest || statesnapshots.CompressHistorySegments || statesnapshots.CompressLatestSegments {
		t.Fatalf("snapshot compression gates = history %v latest %v globals %v/%v, want both disabled", history, latest, statesnapshots.CompressHistorySegments, statesnapshots.CompressLatestSegments)
	}
}

func TestSnapshotCommandAppliesCompressionConfigs(t *testing.T) {
	withSnapshotCompressionGlobal(t)
	command := snapshotCommand()
	if command.Before == nil {
		t.Fatal("snapshot command has no compression configuration hook")
	}
	ctx := makeSnapshotCompressionFlagSet(t, []string{
		"--snapshot.compress-history=false",
		"--snapshot.compress-latest=false",
	})
	if err := command.Before(ctx); err != nil {
		t.Fatalf("snapshot command configuration hook: %v", err)
	}
	if statesnapshots.CompressHistorySegments || statesnapshots.CompressLatestSegments {
		t.Fatalf("snapshot command globals = history %v latest %v, want both disabled", statesnapshots.CompressHistorySegments, statesnapshots.CompressLatestSegments)
	}
}
