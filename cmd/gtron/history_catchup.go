package main

import (
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

var historyCatchupModeFlag = &cli.StringFlag{
	Name: "history.catchup-mode", Value: string(snapshots.HistoryCatchupBalanced),
	Usage: "History maintenance scheduling: balanced or throughput (full bounded busy batches and measured recovery; requires spare CPU and I/O)",
}

func runtimeHistoryCatchupMode(ctx *cli.Context) (snapshots.HistoryCatchupMode, error) {
	return snapshots.ParseHistoryCatchupMode(ctx.String(historyCatchupModeFlag.Name))
}
