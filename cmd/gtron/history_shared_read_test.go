package main

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

func registeredHistorySharedReadFlags(t *testing.T) []cli.Flag {
	t.Helper()
	var flags []cli.Flag
	for _, name := range []string{"history.shared-read-workers", "history.shared-chunk-cache"} {
		found := 0
		for _, f := range app.Flags {
			for _, n := range f.Names() {
				if n == name {
					found++
					v := reflect.ValueOf(f)
					copy := reflect.New(v.Elem().Type())
					copy.Elem().Set(v.Elem())
					flags = append(flags, copy.Interface().(cli.Flag))
				}
			}
		}
		if found != 1 {
			t.Fatalf("flag %s registered %d times", name, found)
		}
	}
	return flags
}

func TestHistorySharedReadCLI(t *testing.T) {
	t.Setenv("GTRON_HISTORY_SHARED_READ_WORKERS", "8")
	t.Setenv("GTRON_HISTORY_SHARED_CHUNK_CACHE", "true")
	for _, tc := range []struct {
		name string
		args []string
		want snapshots.HistoryReadOptions
		bad  bool
	}{
		{name: "default"}, {name: "two", args: []string{"--history.shared-read-workers=2"}, want: snapshots.HistoryReadOptions{Workers: 2}},
		{name: "four-cache", args: []string{"--history.shared-read-workers=4", "--history.shared-chunk-cache"}, want: snapshots.HistoryReadOptions{Workers: 4, ChunkCache: true}},
		{name: "eight", args: []string{"--history.shared-read-workers=8"}, want: snapshots.HistoryReadOptions{Workers: 8}},
		{name: "cache-serial", args: []string{"--history.shared-chunk-cache=true"}, want: snapshots.HistoryReadOptions{ChunkCache: true}},
		{name: "one", args: []string{"--history.shared-read-workers=1"}, bad: true}, {name: "negative", args: []string{"--history.shared-read-workers=-2"}, bad: true},
		{name: "three", args: []string{"--history.shared-read-workers=3"}, bad: true}, {name: "large", args: []string{"--history.shared-read-workers=64"}, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got snapshots.HistoryReadOptions
			a := &cli.App{Name: "gtron", Writer: io.Discard, ErrWriter: io.Discard, Flags: registeredHistorySharedReadFlags(t), Action: func(ctx *cli.Context) error {
				var err error
				got, err = runtimeHistorySharedReadOptions(ctx)
				return err
			}}
			err := a.Run(append([]string{"gtron"}, tc.args...))
			if tc.bad {
				if err == nil || !strings.Contains(err.Error(), "0, 2, 4 or 8") {
					t.Fatal("invalid option accepted", err)
				}
			} else if err != nil || got != tc.want {
				t.Fatal("wrong options", got, err)
			}
		})
	}
}

func TestHistorySharedReadInvalidBeforeData(t *testing.T) {
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
	err := a.Run([]string{"gtron", "--history.shared-read-workers=3", "--datadir", datadir, "--genesis", filepath.Join(root, "missing-genesis")})
	if err == nil || !strings.Contains(err.Error(), "shared-read workers") {
		t.Fatal("validation did not precede source access", err)
	}
	if _, err := os.Stat(datadir); !os.IsNotExist(err) {
		t.Fatal("invalid config touched source", err)
	}
}

func TestHistorySharedReadResourcesReuseSameSampler(t *testing.T) {
	now := time.Unix(1000, 0)
	calls := 0
	gomax := 16
	quota := uint64(math.MaxUint64)
	p := &runtimeHistoryParallelProbe{now: func() time.Time { return now }, gomax: func() int { return gomax }, numCPU: func() int { return 16 }, read: func() (historyParallelObservation, error) {
		calls++
		o := historyParallelTestObservation(uint64(calls * 100))
		o.cpuLimitMilli = quota
		return o, nil
	}}
	resources := &runtimeHistoryResources{parallel: p}
	resources.running.Store(true)
	if resources.sharedReadResources().Available {
		t.Fatal("single observation invented capacity")
	}
	now = now.Add(5 * time.Second)
	out := resources.sharedReadResources()
	if !out.Available || out.IdleCoresMilli != 8000 || !out.SampledAt.Equal(now) || calls != 2 {
		t.Fatal("effective capacity differs", out, calls)
	}
	if !p.ready() || calls != 2 {
		t.Fatal("old ready callback created another baseline")
	}
	gomax = 8
	if out = resources.sharedReadResources(); out.IdleCoresMilli != 0 {
		t.Fatal("GOMAXPROCS change ignored", out)
	}
	gomax = 16
	quota = 16000
	now = now.Add(5 * time.Second)
	if resources.sharedReadResources().Available {
		t.Fatal("finite quota was treated as idle evidence")
	}
	resources.running.Store(false)
	before := calls
	if resources.sharedReadResources().Available || calls != before {
		t.Fatal("stopped resource sampler reactivated")
	}
}
