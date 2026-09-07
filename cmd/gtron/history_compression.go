package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

var historyBlockDedupFlag = &cli.BoolFlag{Name: "history.block-dedup", Usage: "Deduplicate repeated large values within each hot history block pack (adds foreground CPU; benchmark representative replay before enabling)"}

var historyCompressionFormatFlag = &cli.StringFlag{
	Name:    "history.compression-format",
	Value:   "auto",
	EnvVars: []string{"GTRON_HISTORY_COMPRESSION_FORMAT"},
	Usage:   "Cold history container: auto selects CDC for repeated large values, 1=legacy, 2=streaming footer, 3=CDC for every history segment",
}

// Configure once before opening stores/starting any node lifecycle. Individual
// builders select an explicit per-build format; they never temporarily mutate
// the environment around a concurrent build or merge.
func applyRuntimeHistoryCompression(ctx *cli.Context) (string, error) {
	format := ctx.String(historyCompressionFormatFlag.Name)
	if format == "" {
		format = "auto"
	}
	switch format {
	case "auto", "1", "2", "3":
	default:
		return "", fmt.Errorf("invalid history.compression-format %q (want auto, 1, 2 or 3)", format)
	}
	if err := os.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", format); err != nil {
		return "", err
	}
	return format, nil
}
