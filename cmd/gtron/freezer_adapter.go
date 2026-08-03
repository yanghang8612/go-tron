package main

import (
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

func (a *freezerChainSource) ReadBlockRawStrict(number uint64) ([]byte, bool, error) {
	return rawdb.ReadBlockRawStrict(a.chain.DB(), number)
}

func (a *freezerChainSource) ReadTransactionInfosRawStrict(number uint64) ([]byte, bool, error) {
	return rawdb.ReadTransactionInfosRawStrict(a.chain.DB(), number)
}

func (a *freezerChainSource) ReadBlockHashByNumberStrict(number uint64) (tcommon.Hash, bool, error) {
	return rawdb.ReadBlockHashByNumberStrict(a.chain.ChainDB(), number)
}

func (a *freezerChainSource) ReadBlockStateRootRaw(hash tcommon.Hash) ([]byte, error) {
	data, _, err := rawdb.ReadBlockStateRootRawStrict(a.chain.ChainDB(), hash)
	if err != nil {
		return nil, err
	}
	return data, nil
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

func (s *freezerStore) AncientDatadir() (string, error) {
	return s.f.AncientDatadir()
}

func (s *freezerStore) TransactionIndexCoverage() uint64 {
	return s.f.TransactionIndexCoverage()
}

func (s *freezerStore) PublishTransactionIndexRun(result rawdbfreezer.TransactionIndexBuildResult) error {
	return s.f.PublishTransactionIndexRun(result)
}

func (s *freezerStore) CompactTransactionIndexTail() (bool, error) {
	return s.f.CompactTransactionIndexTail()
}

func makeFreezerConfig(ctx *cli.Context) chainfreezer.Config {
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
	cfg.TransactionIndexEnabled = !ctx.Bool("freezer.tx-index.disable")
	return cfg
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
