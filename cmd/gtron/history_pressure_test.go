package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type historyEstimateFunc func([]byte, []byte) (uint64, error)

func (f historyEstimateFunc) EstimateDiskUsage(a, b []byte) (uint64, error) { return f(a, b) }

func TestHistoryPressureLimits(t *testing.T) {
	for _, total := range []uint64{100 << 30, 7 << 40} {
		got, err := resolveHistoryPressureLimits(total, 0, 0, 0)
		if err != nil || got.hot != total/20 || got.free != total/10 || got.minimum != total/50 {
			t.Fatalf("total %d: %+v %v", total, got, err)
		}
	}
	got, err := resolveHistoryPressureLimits(100<<30, 500, 1000, 200)
	if err != nil || got != (historyPressureLimits{500 << 20, 1000 << 20, 200 << 20}) {
		t.Fatalf("explicit limits: %+v %v", got, err)
	}
	for _, args := range [][4]uint64{{0, 0, 0, 0}, {100 << 30, ^uint64(0), 0, 0}, {100 << 30, 0, 100, 101}, {100 << 30, 0, 100 << 10, 0}} {
		if _, err := resolveHistoryPressureLimits(args[0], args[1], args[2], args[3]); err == nil {
			t.Fatalf("accepted invalid limits %v", args)
		}
	}
}

func TestRuntimeHistoryPressureCacheAndFreshFreeSpace(t *testing.T) {
	now := time.Unix(1000, 0)
	calls, diskCalls := 0, 0
	free := uint64(300)
	p := &runtimeHistoryPressureProbe{
		paths: []string{"database", "snapshots", "scratch"}, now: func() time.Time { return now },
		disk: func(path string) (vfs.DiskUsage, error) {
			diskCalls++
			if path == "scratch" {
				return vfs.DiskUsage{AvailBytes: free}, nil
			}
			return vfs.DiskUsage{AvailBytes: 999}, nil
		},
		estimate: historyEstimateFunc(func(a, b []byte) (uint64, error) {
			calls++
			start, end := rawdb.StateHistoryKeyspaceBounds()
			if !bytes.Equal(a, start) || !bytes.Equal(b, end) {
				t.Fatal("estimate used wrong key range")
			}
			return uint64(calls - 1), nil // A measured zero must be cached.
		}),
	}
	got, err := p.read(context.Background())
	if err != nil || !got.FreeBytesAvailable || !got.HotHistoryBytesAvailable || got.FreeBytes != 300 || got.HotHistoryBytes != 0 {
		t.Fatalf("first probe: %+v %v", got, err)
	}
	free, now = 0, now.Add(59*time.Second)
	got, err = p.read(context.Background())
	if err != nil || got.FreeBytes != 0 || !got.FreeBytesAvailable || calls != 1 || diskCalls != 6 {
		t.Fatalf("cached probe: %+v %v calls %d/%d", got, err, calls, diskCalls)
	}
	now = now.Add(time.Second)
	got, err = p.read(context.Background())
	if err != nil || got.HotHistoryBytes != 1 || calls != 2 {
		t.Fatalf("refresh: %+v %v", got, err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.read(cancelled); !errors.Is(err, context.Canceled) || diskCalls != 9 {
		t.Fatalf("cancelled: %v calls %d", err, diskCalls)
	}
}

func TestRuntimeHistoryPressureErrorsAndUnavailableEstimate(t *testing.T) {
	want := errors.New("probe failure")
	p := &runtimeHistoryPressureProbe{paths: []string{"a"}, now: time.Now, disk: func(string) (vfs.DiskUsage, error) { return vfs.DiskUsage{AvailBytes: 42}, nil }}
	got, err := p.read(context.Background())
	if err != nil || got.HotHistoryBytesAvailable || got.FreeBytes != 42 {
		t.Fatalf("no estimator: %+v %v", got, err)
	}
	p.estimate = historyEstimateFunc(func([]byte, []byte) (uint64, error) { return 0, want })
	got, err = p.read(context.Background())
	if !errors.Is(err, want) || got.HotHistoryBytesAvailable || !got.FreeBytesAvailable {
		t.Fatalf("estimate error: %+v %v", got, err)
	}
	p.disk = func(string) (vfs.DiskUsage, error) { return vfs.DiskUsage{}, want }
	if _, err := p.read(context.Background()); !errors.Is(err, want) {
		t.Fatal(err)
	}
}

func TestHistoryPressureDiskUsageMissingScratch(t *testing.T) {
	dir := t.TempDir()
	parent, err := historyOutputDiskUsage(dir)
	if err != nil {
		t.Fatal(err)
	}
	child, err := historyOutputDiskUsage(filepath.Join(dir, "not-created", "scratch"))
	if err != nil || child.TotalBytes != parent.TotalBytes {
		t.Fatalf("child: %+v %v", child, err)
	}
}
