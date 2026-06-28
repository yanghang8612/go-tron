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
