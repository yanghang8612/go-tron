package main

import (
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

var historySharedReadWorkersFlag = &cli.IntFlag{
	Name: "history.shared-read-workers", Value: 0,
	Usage: "Maximum history shared-pack read workers (0 serial, 2, 4 or 8); fresh resource admission may select fewer",
}
var historySharedChunkCacheFlag = &cli.BoolFlag{
	Name: "history.shared-chunk-cache", Value: false,
	Usage: "Allow a bounded per-build authenticated shared-chunk cache when fresh resource admission permits",
}

var historyReferenceContainerFlag = &cli.BoolFlag{
	Name: "history.reference-container", Value: false,
	Usage: "Write self-contained reference history; every future restart must use a reference-capable reader",
}

func runtimeHistorySharedReadOptions(ctx *cli.Context) (snapshots.HistoryReadOptions, error) {
	opts := snapshots.HistoryReadOptions{Workers: ctx.Int(historySharedReadWorkersFlag.Name), ChunkCache: ctx.Bool(historySharedChunkCacheFlag.Name), ReferenceContainer: ctx.Bool(historyReferenceContainerFlag.Name)}
	return opts, opts.Validate()
}
