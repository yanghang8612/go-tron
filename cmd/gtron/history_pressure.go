package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

var (
	historyPressureHotFlag  = &cli.Uint64Flag{Name: "history.pressure-hot-mib", Usage: "Accelerate bounded cold history work above this estimated hot-history SST size (0 = 5% of smallest output volume)"}
	historyPressureFreeFlag = &cli.Uint64Flag{Name: "history.pressure-free-mib", Usage: "Accelerate bounded cold history work below this free space (0 = 10% of smallest output volume)"}
	historyBuildMinFreeFlag = &cli.Uint64Flag{Name: "history.build-min-free-mib", Usage: "Defer new cold builds below this free space; covered hot pruning continues (0 = 2% of smallest output volume; admission only, not peak-space guarantee)"}
)

type historyDiskEstimator interface {
	EstimateDiskUsage(start, end []byte) (uint64, error)
}

type historyPressureLimits struct{ hot, free, minimum uint64 }

func resolveHistoryPressureLimits(total, hotMiB, freeMiB, minimumMiB uint64) (historyPressureLimits, error) {
	if total == 0 {
		return historyPressureLimits{}, fmt.Errorf("history pressure: zero output volume capacity")
	}
	values := [3]uint64{max(uint64(1), total/20), max(uint64(1), total/10), max(uint64(1), total/50)}
	for i, mib := range []uint64{hotMiB, freeMiB, minimumMiB} {
		if mib > ^uint64(0)>>20 {
			return historyPressureLimits{}, fmt.Errorf("history pressure: MiB value overflows bytes")
		}
		if mib != 0 {
			values[i] = mib << 20
		}
	}
	if values[2] > values[1] || values[1] >= total {
		return historyPressureLimits{}, fmt.Errorf("history pressure requires minimum free <= pressure free < volume capacity")
	}
	return historyPressureLimits{values[0], values[1], values[2]}, nil
}

// Missing scratch directories inherit the filesystem of the closest existing
// parent. No directory is created by a read-only scheduling probe.
func historyOutputDiskUsage(path string) (vfs.DiskUsage, error) {
	for {
		usage, err := vfs.Default.GetDiskUsage(path)
		if err == nil {
			return usage, nil
		}
		if !os.IsNotExist(err) {
			return usage, err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return usage, err
		}
		path = parent
	}
}

type runtimeHistoryPressureProbe struct {
	mu       sync.Mutex
	paths    []string
	estimate historyDiskEstimator
	disk     func(string) (vfs.DiskUsage, error)
	now      func() time.Time
	measured time.Time
	hotBytes uint64
	hotKnown bool
}

func (p *runtimeHistoryPressureProbe) read(ctx context.Context) (statesnapshots.HistoryPressure, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out statesnapshots.HistoryPressure
	if err := ctx.Err(); err != nil {
		return out, err
	}
	// Free space is read every pass, independently of the cached SST estimate.
	for _, path := range p.paths {
		usage, err := p.disk(path)
		if err != nil {
			return out, fmt.Errorf("history pressure: disk usage at %s: %w", path, err)
		}
		if !out.FreeBytesAvailable || usage.AvailBytes < out.FreeBytes {
			out.FreeBytes = usage.AvailBytes
		}
		out.FreeBytesAvailable = true
	}
	if p.estimate != nil && (!p.hotKnown || p.now().Sub(p.measured) >= time.Minute) {
		start, end := rawdb.StateHistoryKeyspaceBounds()
		value, err := p.estimate.EstimateDiskUsage(start, end)
		if err != nil {
			return out, fmt.Errorf("history pressure: hot SST estimate: %w", err)
		}
		p.hotBytes, p.hotKnown, p.measured = value, true, p.now()
	}
	out.HotHistoryBytes, out.HotHistoryBytesAvailable = p.hotBytes, p.hotKnown
	return out, ctx.Err()
}

func makeRuntimeHistoryPressure(ctx *cli.Context, db any, paths ...string) (*runtimeHistoryPressureProbe, historyPressureLimits, error) {
	p := &runtimeHistoryPressureProbe{disk: historyOutputDiskUsage, now: time.Now}
	p.estimate, _ = db.(historyDiskEstimator)
	seen := make(map[string]bool)
	var total uint64
	for _, path := range paths {
		if path == "" {
			continue
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, historyPressureLimits{}, err
		}
		if seen[absolute] {
			continue
		}
		seen[absolute] = true
		p.paths = append(p.paths, absolute)
		usage, err := p.disk(absolute)
		if err != nil {
			return nil, historyPressureLimits{}, fmt.Errorf("history pressure: inspect output volume %s: %w", absolute, err)
		}
		if total == 0 || usage.TotalBytes < total {
			total = usage.TotalBytes
		}
	}
	limits, err := resolveHistoryPressureLimits(total, ctx.Uint64(historyPressureHotFlag.Name), ctx.Uint64(historyPressureFreeFlag.Name), ctx.Uint64(historyBuildMinFreeFlag.Name))
	return p, limits, err
}
