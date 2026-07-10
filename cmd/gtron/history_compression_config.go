package main

import (
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

func applySnapshotCompressionConfig(ctx *cli.Context) bool {
	enabled := snapshotCompressHistoryFlag.Value
	if ctx != nil {
		enabled = ctx.Bool(snapshotCompressHistoryFlag.Name)
	}
	statesnapshots.CompressHistorySegments = enabled
	return enabled
}

func applySnapshotLatestCompressionConfig(ctx *cli.Context) bool {
	enabled := snapshotCompressLatestFlag.Value
	if ctx != nil {
		enabled = ctx.Bool(snapshotCompressLatestFlag.Name)
	}
	statesnapshots.CompressLatestSegments = enabled
	return enabled
}

func applySnapshotCompressionConfigs(ctx *cli.Context) (history, latest bool) {
	return applySnapshotCompressionConfig(ctx), applySnapshotLatestCompressionConfig(ctx)
}
