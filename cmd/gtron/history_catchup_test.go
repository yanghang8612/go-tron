package main

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

// Retrieve the production flag rather than defining a look-alike test flag:
// otherwise these tests could pass while the executable forgot to register it.
func registeredHistoryCatchupFlag(t *testing.T) *cli.StringFlag {
	t.Helper()
	var found *cli.StringFlag
	for _, candidate := range app.Flags {
		for _, name := range candidate.Names() {
			if name != "history.catchup-mode" {
				continue
			}
			if found != nil {
				t.Fatal("history.catchup-mode registered more than once")
			}
			var ok bool
			found, ok = candidate.(*cli.StringFlag)
			if !ok {
				t.Fatalf("history.catchup-mode flag type = %T, want *cli.StringFlag", candidate)
			}
		}
	}
	if found == nil {
		t.Fatal("production app does not register history.catchup-mode")
	}
	if len(found.EnvVars) != 0 {
		t.Fatalf("catch-up mode unexpectedly acquired environment defaults: %v", found.EnvVars)
	}
	if found.Value != string(snapshots.HistoryCatchupBalanced) {
		t.Fatalf("production CLI default = %q, want balanced", found.Value)
	}
	// urfave flags retain IsSet state. Never share this mutable copy across runs.
	copy := *found
	return &copy
}

func TestRuntimeHistoryCatchupProductionCLI(t *testing.T) {
	// This variable must not silently opt an operator into throughput mode, nor
	// be overwritten by mode parsing. The new switch is an explicit CLI choice.
	t.Setenv("GTRON_HISTORY_CATCHUP_MODE", "ignored-sentinel")
	for _, tc := range []struct {
		name string
		args []string
		want snapshots.HistoryCatchupMode
		bad  bool
	}{
		{name: "default", want: snapshots.HistoryCatchupBalanced},
		{name: "balanced", args: []string{"--history.catchup-mode=balanced"}, want: snapshots.HistoryCatchupBalanced},
		{name: "throughput", args: []string{"--history.catchup-mode", "throughput"}, want: snapshots.HistoryCatchupThroughput},
		{name: "zero-value-compatible", args: []string{"--history.catchup-mode="}, want: snapshots.HistoryCatchupBalanced},
		{name: "unknown", args: []string{"--history.catchup-mode=unbounded"}, bad: true},
		{name: "wrong-case", args: []string{"--history.catchup-mode=Throughput"}, bad: true},
		{name: "trailing-space", args: []string{"--history.catchup-mode=throughput "}, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got snapshots.HistoryCatchupMode
			called := false
			cliApp := &cli.App{
				Name: app.Name, Writer: io.Discard, ErrWriter: io.Discard,
				Flags: []cli.Flag{registeredHistoryCatchupFlag(t)},
				Action: func(ctx *cli.Context) error {
					called = true
					var err error
					got, err = runtimeHistoryCatchupMode(ctx)
					return err
				},
			}
			err := cliApp.Run(append([]string{"gtron"}, tc.args...))
			if !called {
				t.Fatalf("registered CLI flag did not reach runtime parsing: %v", err)
			}
			if tc.bad {
				if err == nil || !strings.Contains(err.Error(), "balanced or throughput") {
					t.Fatalf("invalid option returned %v", err)
				}
			} else if err != nil || got != tc.want {
				t.Fatalf("runtime mode = %q, %v; want %q", got, err, tc.want)
			}
			if gotEnv := os.Getenv("GTRON_HISTORY_CATCHUP_MODE"); gotEnv != "ignored-sentinel" {
				t.Fatalf("parsing changed process environment to %q", gotEnv)
			}
		})
	}
}

func TestRuntimeHistoryCatchupInvalidRejectedBeforeGenesisOrData(t *testing.T) {
	// Exercise the actual node Action with the actual global flag set. A missing
	// genesis is a second stop before any DB/node work even if validation regresses;
	// the expected catch-up error must take precedence over that sentinel failure.
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "auto")
	registeredHistoryCatchupFlag(t)
	flags := make([]cli.Flag, len(app.Flags))
	for i, original := range app.Flags {
		value := reflect.ValueOf(original)
		if value.Kind() != reflect.Pointer {
			t.Fatalf("cannot isolate mutable CLI flag %T", original)
		}
		cloned := reflect.New(value.Elem().Type())
		cloned.Elem().Set(value.Elem())
		flags[i] = cloned.Interface().(cli.Flag)
	}
	parent := t.TempDir()
	datadir := filepath.Join(parent, "must-not-be-created")
	missingGenesis := filepath.Join(parent, "missing-genesis.json")
	cliApp := &cli.App{
		Name: app.Name, Flags: flags, Action: app.Action,
		Writer: io.Discard, ErrWriter: io.Discard,
	}
	err := cliApp.Run([]string{"gtron", "--history.catchup-mode=invalid",
		"--datadir", datadir, "--genesis", missingGenesis})
	if err == nil || !strings.Contains(err.Error(), "invalid history catchup mode") {
		t.Fatalf("node Action failed to reject invalid catch-up mode first: %v", err)
	}
	if _, err := os.Stat(datadir); !os.IsNotExist(err) {
		t.Fatalf("invalid catch-up mode touched data directory: %v", err)
	}
}
