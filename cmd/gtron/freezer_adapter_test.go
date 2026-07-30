package main

import (
	"flag"
	"testing"

	"github.com/urfave/cli/v2"
)

func makeFreezerFlagContext(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		freezerDisableFlag,
		freezerIntervalFlag,
		freezerMarginFlag,
		freezerBatchFlag,
		freezerV2DisableFlag,
		freezerV2FrameBlocksFlag,
		freezerV2SegmentBlocksFlag,
		freezerTxIndexDisableFlag,
		freezerTxIndexPrefixBitsFlag,
	}
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	for _, item := range app.Flags {
		if err := item.Apply(set); err != nil {
			t.Fatal(err)
		}
	}
	if err := set.Parse(argv); err != nil {
		t.Fatal(err)
	}
	return cli.NewContext(app, set, nil)
}

func TestMakeFreezerConfigV2DefaultsAndOverrides(t *testing.T) {
	cfg, err := makeFreezerConfig(makeFreezerFlagContext(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.V2Enabled || cfg.V2FrameBlocks != 64 || cfg.V2SegmentBlocks != 65_536 || !cfg.TransactionIndexEnabled || cfg.TransactionIndexPrefixBits != 20 {
		t.Fatalf("V2 defaults = %+v", cfg)
	}
	cfg, err = makeFreezerConfig(makeFreezerFlagContext(t, []string{
		"--freezer.v2.disable",
		"--freezer.v2.frame-blocks", "32",
		"--freezer.v2.segment-blocks", "1024",
		"--freezer.tx-index.disable",
		"--freezer.tx-index.prefix-bits", "16",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.V2Enabled || cfg.V2FrameBlocks != 32 || cfg.V2SegmentBlocks != 1024 || cfg.TransactionIndexEnabled || cfg.TransactionIndexPrefixBits != 16 {
		t.Fatalf("V2 overrides = %+v", cfg)
	}
}

func TestMakeFreezerConfigRejectsInvalidV2Dimensions(t *testing.T) {
	for _, argv := range [][]string{
		{"--freezer.v2.frame-blocks", "0"},
		{"--freezer.v2.segment-blocks", "0"},
		{"--freezer.v2.frame-blocks", "63", "--freezer.v2.segment-blocks", "65536"},
		{"--freezer.tx-index.prefix-bits", "7"},
		{"--freezer.tx-index.prefix-bits", "25"},
	} {
		if _, err := makeFreezerConfig(makeFreezerFlagContext(t, argv)); err == nil {
			t.Fatalf("flags %v accepted", argv)
		}
	}
}
