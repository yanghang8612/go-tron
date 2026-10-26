package main

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/urfave/cli/v2"
)

func TestHistoryReadersOnlyConfiguration(t *testing.T) {
	for _, tc := range []struct {
		value     string
		want, bad bool
	}{
		{"", false, false}, {"0", false, false}, {"false", false, false},
		{"1", true, false}, {" true ", true, false}, {"yes", false, true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv(historyReadersOnlyEnv, tc.value)
			got, err := runtimeHistoryReadersOnly()
			if (err != nil) != tc.bad || got != tc.want {
				t.Fatal("rollout configuration", got, err)
			}
		})
	}
}

func TestHistoryReadersOnlyInvalidBeforeData(t *testing.T) {
	t.Setenv(historyReadersOnlyEnv, "invalid")
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "auto")
	flags := make([]cli.Flag, len(app.Flags))
	for i, f := range app.Flags {
		v := reflect.ValueOf(f)
		copy := reflect.New(v.Elem().Type())
		copy.Elem().Set(v.Elem())
		flags[i] = copy.Interface().(cli.Flag)
	}
	root := t.TempDir()
	datadir := filepath.Join(root, "must-not-exist")
	a := &cli.App{Name: app.Name, Flags: flags, Action: app.Action, Writer: io.Discard, ErrWriter: io.Discard}
	err := a.Run([]string{"gtron", "--datadir", datadir, "--genesis", filepath.Join(root, "missing-genesis")})
	if err == nil || !strings.Contains(err.Error(), historyReadersOnlyEnv) {
		t.Fatal("rollout validation did not precede source access", err)
	}
	if _, err := os.Stat(datadir); !os.IsNotExist(err) {
		t.Fatal("invalid rollout configuration touched source", err)
	}
}
