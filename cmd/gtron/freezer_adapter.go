package main

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

const freezerTableSize = 2 * 1024 * 1024 * 1024

type freezerChainSource struct {
	chain *core.BlockChain
}

func newFreezerChainSource(chain *core.BlockChain) chainfreezer.ChainSource {
	return &freezerChainSource{chain: chain}
}

func (a *freezerChainSource) LatestSolidifiedBlockNum() int64 {
	return a.chain.DynProps().LatestSolidifiedBlockNum()
}

func (a *freezerChainSource) DB() ethdb.KeyValueStore {
	return a.chain.DB()
}

func (a *freezerChainSource) ReadBlockRaw(number uint64) []byte {
	return rawdb.ReadBlockRaw(a.chain.DB(), number)
}

func (a *freezerChainSource) ReadTransactionInfosRaw(number uint64) []byte {
	return rawdb.ReadTransactionInfosRaw(a.chain.DB(), number)
}

func (a *freezerChainSource) ViewBlockRaw(number uint64, fn func([]byte) error) (bool, error) {
	return rawdb.ViewBlockRaw(a.chain.DB(), number, fn)
}

func (a *freezerChainSource) ViewTransactionInfosRaw(number uint64, fn func([]byte) error) (bool, error) {
	return rawdb.ViewTransactionInfosRaw(a.chain.DB(), number, fn)
}

func (a *freezerChainSource) ReadBlockHash(_ uint64, blockRaw []byte) tcommon.Hash {
	return rawdb.ReadBlockHashRaw(blockRaw)
}

func (a *freezerChainSource) ReadBlockStateRootRaw(hash tcommon.Hash) []byte {
	return rawdb.ReadBlockStateRootRaw(a.chain.DB(), hash)
}

type freezerStore struct {
	rawdb.AncientReader
	f *rawdbfreezer.Freezer
}

func newFreezerStore(f *rawdbfreezer.Freezer) chainfreezer.FreezerStore {
	if f == nil {
		return nil
	}
	return &freezerStore{AncientReader: rawdb.NewFreezerReader(f), f: f}
}

func (s *freezerStore) ModifyAncients(fn func(rawdb.AncientWriteOp) error) (int64, error) {
	return s.f.ModifyAncients(fn)
}

func (s *freezerStore) TruncateHead(items uint64) (uint64, error) {
	return s.f.TruncateHead(items)
}

func (s *freezerStore) Sync() error {
	return s.f.Sync()
}

func (s *freezerStore) V2Coverage() uint64 {
	return s.f.V2Coverage()
}

func (s *freezerStore) MigrateV2(options rawdbfreezer.V2MigrationOptions) (rawdbfreezer.V2MigrationResult, error) {
	return s.f.MigrateV2(options)
}

func makeFreezerConfig(ctx *cli.Context) (chainfreezer.Config, error) {
	cfg := chainfreezer.Default()
	cfg.Enabled = !ctx.Bool("freezer.disable")
	if ctx.IsSet("freezer.interval") {
		cfg.Interval = ctx.Duration("freezer.interval")
	}
	if ctx.IsSet("freezer.margin") {
		cfg.MarginBlocks = ctx.Uint64("freezer.margin")
	}
	if ctx.IsSet("freezer.batch") {
		cfg.BatchBlocks = ctx.Uint64("freezer.batch")
	}
	cfg.V2Enabled = !ctx.Bool("freezer.v2.disable")
	frameBlocks := ctx.Uint64("freezer.v2.frame-blocks")
	if frameBlocks == 0 || frameBlocks > math.MaxUint32 {
		return chainfreezer.Config{}, fmt.Errorf("--freezer.v2.frame-blocks must be between 1 and %d", uint64(math.MaxUint32))
	}
	cfg.V2FrameBlocks = uint32(frameBlocks)
	cfg.V2SegmentBlocks = ctx.Uint64("freezer.v2.segment-blocks")
	if cfg.V2SegmentBlocks == 0 || cfg.V2SegmentBlocks%frameBlocks != 0 {
		return chainfreezer.Config{}, fmt.Errorf("--freezer.v2.segment-blocks must be positive and divisible by --freezer.v2.frame-blocks")
	}
	return cfg, nil
}

func shouldOpenFreezer(path string, cfg chainfreezer.Config) bool {
	if cfg.Enabled {
		return true
	}
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func defaultFreezerInterval() time.Duration {
	return chainfreezer.Default().Interval
}

func defaultFreezerMargin() uint64 {
	return chainfreezer.Default().MarginBlocks
}

func defaultFreezerBatch() uint64 {
	return chainfreezer.Default().BatchBlocks
}
