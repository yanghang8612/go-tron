package main

import (
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/internal/tronapi"
	tnet "github.com/tronprotocol/go-tron/net"
	"github.com/urfave/cli/v2"
)

func registeredHistoryBacklogFlags(t *testing.T) []cli.Flag {
	t.Helper()
	var flags []cli.Flag
	for _, original := range app.Flags {
		if !strings.HasPrefix(original.Names()[0], "history.backlog-") {
			continue
		}
		value := reflect.ValueOf(original)
		cloned := reflect.New(value.Elem().Type())
		cloned.Elem().Set(value.Elem())
		flags = append(flags, cloned.Interface().(cli.Flag))
	}
	if len(flags) != 3 {
		t.Fatalf("registered %d backlog flags, want 3", len(flags))
	}
	return flags
}

func TestRuntimeHistoryBacklogRegisteredCLI(t *testing.T) {
	t.Setenv("GTRON_HISTORY_BACKLOG_ADMISSION", "true")
	for _, tc := range []struct {
		name string
		args []string
		want historyBacklogOptions
		bad  bool
	}{
		{name: "default-off"},
		{name: "valid", args: []string{"--history.backlog-admission", "--history.backlog-high-blocks=1000000", "--history.backlog-low-blocks=990000"}, want: historyBacklogOptions{true, 1000000, 990000}},
		{name: "no-implicit-default", args: []string{"--history.backlog-admission"}, bad: true},
		{name: "no-silent-enable", args: []string{"--history.backlog-high-blocks=100"}, bad: true},
		{name: "inverted", args: []string{"--history.backlog-admission", "--history.backlog-high-blocks=100", "--history.backlog-low-blocks=101"}, bad: true},
		{name: "equal", args: []string{"--history.backlog-admission", "--history.backlog-high-blocks=100", "--history.backlog-low-blocks=100"}, bad: true},
		{name: "zero-low", args: []string{"--history.backlog-admission", "--history.backlog-high-blocks=100"}, bad: true},
		{name: "overflow", args: []string{"--history.backlog-admission", "--history.backlog-high-blocks=18446744073709551616", "--history.backlog-low-blocks=100"}, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got historyBacklogOptions
			a := &cli.App{Name: "gtron", Writer: io.Discard, ErrWriter: io.Discard, Flags: registeredHistoryBacklogFlags(t), Action: func(ctx *cli.Context) error { var err error; got, err = runtimeHistoryBacklogOptions(ctx); return err }}
			err := a.Run(append([]string{"gtron"}, tc.args...))
			if tc.bad {
				if err == nil {
					t.Fatal("invalid config accepted")
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got=%+v err=%v want=%+v", got, err, tc.want)
			}
		})
	}
}

func TestRuntimeHistoryBacklogDisabledDoesNotUseService(t *testing.T) {
	if err := configureRuntimeHistoryBacklog(nil, nil, 0, historyBacklogOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryBacklogLowWatermarkRetainsForcedColdLiveness(t *testing.T) {
	const window = uint64(65536)
	minimum := maxBusyDeferredColdHistoryBlocks(maxDeferredColdHistoryBlocks(window))
	for _, low := range []uint64{1, window, minimum - 1} {
		if err := validateHistoryBacklogBuilderBounds(historyBacklogOptions{true, minimum + 10000, low}, window); err == nil {
			t.Fatalf("low=%d can trap held importer below forced cold boundary", low)
		}
	}
	if err := validateHistoryBacklogBuilderBounds(historyBacklogOptions{true, minimum + 10000, minimum}, window); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryBacklogWalletStatusSeparatesIntentionalHold(t *testing.T) {
	info := tronapi.SyncInfo{Active: true, HistoryBacklog: historyBacklogSyncInfo(tnet.HistoryBacklogAdmissionStatus{})}
	encoded, err := json.Marshal(info)
	if err != nil || strings.Contains(string(encoded), "historyBacklog") {
		t.Fatalf("disabled legacy shape: %s %v", encoded, err)
	}
	at := time.Date(2026, 9, 15, 10, 0, 0, 1, time.FixedZone("CST", 8*3600))
	info.HistoryBacklog = historyBacklogSyncInfo(tnet.HistoryBacklogAdmissionStatus{Enabled: true, Holding: true, Reason: "backlog", HighBlocks: 1000000, LowBlocks: 990000, LagBlocks: 1002000, HeadGapBlocks: 1067536, ObservedAt: at, Checks: 12})
	encoded, err = json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Active, Paused bool
		HistoryBacklog *tronapi.HistoryBacklogInfo
	}
	if err = json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if !wire.Active || wire.Paused || wire.HistoryBacklog == nil || !wire.HistoryBacklog.Holding || wire.HistoryBacklog.LagBlocks != 1002000 || wire.HistoryBacklog.HeadGapBlocks != 1067536 || wire.HistoryBacklog.ObservedAt != "2026-09-15T02:00:00.000000001Z" {
		t.Fatalf("misleading wallet status: %s", encoded)
	}
}
