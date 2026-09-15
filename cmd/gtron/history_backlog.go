package main

import (
	"context"
	"fmt"
	"time"

	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	tnet "github.com/tronprotocol/go-tron/net"
	"github.com/urfave/cli/v2"
)

var (
	historyBacklogAdmissionFlag = &cli.BoolFlag{Name: "history.backlog-admission", Usage: "Limit sync import sessions using eligible cold-history backlog high/low watermarks (reduces catch-up speed; disabled by default)"}
	historyBacklogHighFlag      = &cli.Uint64Flag{Name: "history.backlog-high-blocks", Usage: "Hold new sync import sessions at this eligible history backlog; requires explicit admission enablement"}
	historyBacklogLowFlag       = &cli.Uint64Flag{Name: "history.backlog-low-blocks", Usage: "Resume sync import sessions once eligible history backlog falls to this value"}
)

type historyBacklogOptions struct {
	enabled   bool
	high, low uint64
}

func runtimeHistoryBacklogOptions(ctx *cli.Context) (historyBacklogOptions, error) {
	o := historyBacklogOptions{ctx.Bool(historyBacklogAdmissionFlag.Name), ctx.Uint64(historyBacklogHighFlag.Name), ctx.Uint64(historyBacklogLowFlag.Name)}
	if !o.enabled {
		if o.high != 0 || o.low != 0 {
			return o, fmt.Errorf("history backlog watermarks require --history.backlog-admission")
		}
		return o, nil
	}
	if o.low == 0 || o.high <= o.low {
		return o, fmt.Errorf("history backlog admission requires explicit 0 < low blocks < high blocks")
	}
	return o, nil
}

func configureRuntimeHistoryBacklog(service *tnet.SyncService, chain *core.BlockChain, window uint64, options historyBacklogOptions) error {
	if !options.enabled {
		return nil
	}
	if err := validateHistoryBacklogBuilderBounds(options, window); err != nil {
		return err
	}
	return service.ConfigureHistoryBacklogAdmission(tnet.HistoryBacklogAdmissionConfig{
		Enabled: true, HighBlocks: options.high, LowBlocks: options.low,
		Probe: func(ctx context.Context) (tnet.HistoryBacklogSample, error) {
			sample, err := chain.SampleHistoryBacklog(ctx, window)
			if err != nil {
				return tnet.HistoryBacklogSample{}, err
			}
			return tnet.HistoryBacklogSample{ObservedAt: sample.SampledAt, EligibleBlock: sample.Eligible, PublishedBlock: sample.Covered, HeadBlock: sample.Head}, nil
		},
	})
}

func validateHistoryBacklogBuilderBounds(options historyBacklogOptions, window uint64) error {
	if !options.enabled {
		return nil
	}
	// Below the busy deferral boundary, deep-sync cold work may legitimately
	// wait for importer-idle/resource admission. A held downloader still has
	// buffered blocks, so require release before entering that deferral region.
	minimum := maxBusyDeferredColdHistoryBlocks(maxDeferredColdHistoryBlocks(window))
	if options.low < minimum {
		return fmt.Errorf("history backlog low blocks must be at least %d to retain forced cold-build progress", minimum)
	}
	return nil
}

func historyBacklogSyncInfo(status tnet.HistoryBacklogAdmissionStatus) *tronapi.HistoryBacklogInfo {
	if !status.Enabled {
		return nil
	}
	stamp := func(t time.Time) string {
		if t.IsZero() {
			return ""
		}
		return t.UTC().Format(time.RFC3339Nano)
	}
	return &tronapi.HistoryBacklogInfo{
		Enabled: status.Enabled, Holding: status.Holding, Reason: status.Reason,
		HighBlocks: status.HighBlocks, LowBlocks: status.LowBlocks, HeadBlock: status.HeadBlock,
		EligibleBlock: status.EligibleBlock, PublishedBlock: status.PublishedBlock,
		LagBlocks: status.LagBlocks, HeadGapBlocks: status.HeadGapBlocks,
		ObservedAt: stamp(status.ObservedAt), CheckedAt: stamp(status.CheckedAt), Since: stamp(status.Since),
		LastError: status.LastError, Checks: status.Checks, ProbeErrors: status.ProbeErrors, HoldTransitions: status.HoldTransitions,
	}
}
