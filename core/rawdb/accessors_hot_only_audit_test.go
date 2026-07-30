package rawdb

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestNoProductionHotBlockKVReadReferences(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBReferences(t, root, map[string]struct{}{
		"ReadBlockKV": {},
	}, nil)
	if len(offenders) > 0 {
		t.Fatalf("production code must use freezer-aware chain accessors instead of hot-only rawdb references:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestHotBlockKVAuditRejectsReaderFunctionValue(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var readBlock = rawdb.ReadBlockKV

func query(db any) {
	_ = readBlock(db, 7)
}
`)

	offenders := auditForbiddenRawDBReferences(t, root, map[string]struct{}{
		"ReadBlockKV": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadBlockKV") {
		t.Fatalf("offenders = %+v, want hot-only block reader function value rejected", offenders)
	}
}

func TestNoUnexpectedProductionRawFreezerReadReferences(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBReferences(t, root, map[string]struct{}{
		"ReadBlockRaw":                  {},
		"ReadBlockRawStrict":            {},
		"ReadTransactionInfosRaw":       {},
		"ReadTransactionInfosRawStrict": {},
		"ReadBlockStateRootRaw":         {},
		"ReadBlockStateRootRawStrict":   {},
	}, map[string]map[string]struct{}{
		"cmd/gtron/freezer_adapter.go": {
			"ReadBlockRaw":                  {},
			"ReadBlockRawStrict":            {},
			"ReadTransactionInfosRaw":       {},
			"ReadTransactionInfosRawStrict": {},
			"ReadBlockStateRootRaw":         {},
			"ReadBlockStateRootRawStrict":   {},
		},
		"core/blockbuffer/buffer.go": {
			"ReadBlockRawStrict": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production code must route raw freezer reads through the freezer adapter boundary:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestRawFreezerReadAuditRejectsReaderFunctionValue(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var readRawBlock = rawdb.ReadBlockRaw

func query(db any) {
	_ = readRawBlock(db, 7)
}
`)

	offenders := auditForbiddenRawDBReferences(t, root, map[string]struct{}{
		"ReadBlockRaw": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadBlockRaw") {
		t.Fatalf("offenders = %+v, want raw freezer reader function value rejected", offenders)
	}
}

func TestRawFreezerReadAuditRejectsStrictReaderReferences(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var readRawInfosStrict = rawdb.ReadTransactionInfosRawStrict

func query(db any) {
	_, _, _ = rawdb.ReadBlockRawStrict(db, 7)
	_, _, _ = readRawInfosStrict(db, 7)
}
`)

	offenders := auditForbiddenRawDBReferences(t, root, map[string]struct{}{
		"ReadBlockRawStrict":            {},
		"ReadTransactionInfosRawStrict": {},
	}, nil)
	if len(offenders) != 2 {
		t.Fatalf("offenders = %+v, want strict raw freezer reader references rejected", offenders)
	}
	joined := strings.Join(offenders, "\n")
	for _, want := range []string{"rawdb.ReadBlockRawStrict", "rawdb.ReadTransactionInfosRawStrict"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("offenders = %+v, want %s rejected", offenders, want)
		}
	}
}

func TestRawFreezerReadAuditRejectsDotImportedReaderFunctionValue(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import . "github.com/tronprotocol/go-tron/core/rawdb"

var readRawInfos = ReadTransactionInfosRaw

func query(db any) {
	_ = readRawInfos(db, 7)
}
`)

	offenders := auditForbiddenRawDBReferences(t, root, map[string]struct{}{
		"ReadTransactionInfosRaw": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "ReadTransactionInfosRaw") {
		t.Fatalf("offenders = %+v, want dot-imported raw freezer reader function value rejected", offenders)
	}
}

func TestNoActuatorDirectHotBlockHashReads(t *testing.T) {
	repoRoot := findRepoRoot(t)
	actuatorRoot := filepath.Join(repoRoot, "actuator")
	offenders := auditForbiddenRawDBCallsOutsideAllowedFuncs(t, actuatorRoot, map[string]struct{}{
		"ReadBlockHashByNumber": {},
	}, map[string]map[string]struct{}{
		"actuator.go": {
			"EffectiveGenesisHash": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("actuator historical compatibility checks must use Context.EffectiveGenesisHash instead of direct hot block hash reads:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionBlockHashByNumberReadsStayOnAuditedBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBCallsOutsideAllowedFuncs(t, root, map[string]struct{}{
		"ReadBlockHashByNumber": {},
	}, map[string]map[string]struct{}{
		"actuator/actuator.go": {
			"EffectiveGenesisHash": {},
		},
		"core/blockbuffer/buffer.go": {
			"BlockHashByNumber": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production block-hash-by-number reads must stay behind audited freezer/cold-index boundaries:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestBlockHashByNumberAuditRejectsSameFileNonBoundaryCall(t *testing.T) {
	root := writeAuditFixture(t, "actuator/actuator.go", `package actuator

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func EffectiveGenesisHash(db any) {
	_ = rawdb.ReadBlockHashByNumber(db, 0)
}

func Validate(db any) {
	_ = rawdb.ReadBlockHashByNumber(db, 1)
}
`)

	offenders := auditForbiddenRawDBCallsOutsideAllowedFuncs(t, root, map[string]struct{}{
		"ReadBlockHashByNumber": {},
	}, map[string]map[string]struct{}{
		"actuator/actuator.go": {
			"EffectiveGenesisHash": {},
		},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadBlockHashByNumber") {
		t.Fatalf("offenders = %+v, want same-file non-boundary block-hash read rejected", offenders)
	}
}

func TestVMBlockHashReadsUseStrictBoundary(t *testing.T) {
	repoRoot := findRepoRoot(t)
	vmRoot := filepath.Join(repoRoot, "vm")
	offenders := auditForbiddenRawDBCalls(t, vmRoot, map[string]struct{}{
		"ReadBlockHashByNumber": {},
	}, nil)
	if len(offenders) > 0 {
		t.Fatalf("VM BLOCKHASH/CHAINID paths must use strict block-hash reads so corrupt hot/freezer rows abort execution:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionHotOnlyChainDBConstructorsStayOnAuditedBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditHotOnlyChainDBConstructors(t, root, map[string]map[string]struct{}{
		"cmd/gtron/db_cmd.go": {
			"dbSeedBalanceTraceReplayFromSnapshot": {},
		},
		"core/balance_trace_backfill.go": {
			"BackfillBalanceTracesByReplay":   {},
			"collectReplayedBalanceTraceRows": {},
		},
		"core/blockchain.go": {
			"NewBlockChainWithAncient": {},
		},
		"core/genesis.go": {
			"SetupGenesisBlockWithAncient": {},
		},
		"internal/dbcompare/compare.go": {
			"gtronHead":    {},
			"compareBlock": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production code must not construct hot-only ChainDB wrappers outside audited constructor/replay/diagnostic boundaries:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestHotOnlyChainDBAuditRecognizesConvertedNoopAncient(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func build() {
	_ = rawdb.NewChainDB(nil, rawdb.AncientReader(rawdb.NoopAncient{}))
}
`)

	offenders := auditHotOnlyChainDBConstructors(t, root, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.NewChainDB(..., nil/NoopAncient)") {
		t.Fatalf("offenders = %+v, want converted NoopAncient constructor", offenders)
	}
}

func TestHotOnlyChainDBAuditRecognizesNilAncient(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func build() {
	_ = rawdb.NewChainDB(nil, nil)
}
`)

	offenders := auditHotOnlyChainDBConstructors(t, root, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.NewChainDB(..., nil/NoopAncient)") {
		t.Fatalf("offenders = %+v, want nil AncientReader constructor", offenders)
	}
}

func TestHotOnlyChainDBAuditRecognizesNoopAncientAlias(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func build() {
	var ancient rawdb.AncientReader = rawdb.NoopAncient{}
	_ = rawdb.NewChainDB(nil, ancient)
}
`)

	offenders := auditHotOnlyChainDBConstructors(t, root, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.NewChainDB(..., nil/NoopAncient)") {
		t.Fatalf("offenders = %+v, want aliased NoopAncient constructor", offenders)
	}
}

func TestHotOnlyChainDBAuditRecognizesZeroValueAncientAlias(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func build() {
	var ancient rawdb.AncientReader
	_ = rawdb.NewChainDB(nil, ancient)
}
`)

	offenders := auditHotOnlyChainDBConstructors(t, root, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.NewChainDB(..., nil/NoopAncient)") {
		t.Fatalf("offenders = %+v, want zero-value AncientReader constructor", offenders)
	}
}

func TestHotOnlyChainDBAuditRecognizesHotOnlyFallbackAncient(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func build() {
	ancient := rawdb.NewFallbackAncientReader(rawdb.NoopAncient{}, nil)
	_ = rawdb.NewChainDB(nil, ancient)
}
`)

	offenders := auditHotOnlyChainDBConstructors(t, root, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.NewChainDB(..., nil/NoopAncient)") {
		t.Fatalf("offenders = %+v, want all-hot fallback AncientReader constructor", offenders)
	}
}

func TestHotOnlyChainDBAuditClearsHotOnlyAliasAfterColdFallback(t *testing.T) {
	root := writeAuditFixture(t, "app/rewrapped.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func build(external rawdb.AncientReader) {
	var ancient rawdb.AncientReader = rawdb.NoopAncient{}
	ancient = rawdb.NewFallbackAncientReader(ancient, external)
	_ = rawdb.NewChainDB(nil, ancient)
}
`)

	if offenders := auditHotOnlyChainDBConstructors(t, root, nil); len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want fallback with external ancient reader accepted", offenders)
	}
}

func TestHotOnlyChainDBAuditRejectsSameFileNonBoundaryConstructor(t *testing.T) {
	root := writeAuditFixture(t, "core/blockchain.go", `package core

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func NewBlockChainWithAncient() {
	_ = rawdb.NewChainDB(nil, rawdb.NoopAncient{})
}

func debugHotOnlyReader() {
	_ = rawdb.NewChainDB(nil, rawdb.NoopAncient{})
}
`)

	offenders := auditHotOnlyChainDBConstructors(t, root, map[string]map[string]struct{}{
		"core/blockchain.go": {
			"NewBlockChainWithAncient": {},
		},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.NewChainDB(..., nil/NoopAncient)") {
		t.Fatalf("offenders = %+v, want same-file non-boundary hot-only constructor rejected", offenders)
	}
}

func TestProductionColdArchiveReadersUseChainDBBoundary(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadAccountTrace":                  {},
		"ReadAccountTraceAtOrBefore":        {},
		"ReadAccountTraceStrict":            {},
		"ReadBlock":                         {},
		"ReadBlockBalanceTrace":             {},
		"ReadBlockBalanceTraceStrict":       {},
		"ReadBlockHashByNumber":             {},
		"ReadBlockHashByNumberStrict":       {},
		"ReadBlockNumber":                   {},
		"ReadBlockNumberStrict":             {},
		"ReadBlockStateRoot":                {},
		"ReadBlockStateRootRawStrict":       {},
		"ReadBlockStateRootStrict":          {},
		"ReadSectionBloom":                  {},
		"ReadSectionBloomBitSet":            {},
		"ReadSectionBloomBitSetStrict":      {},
		"ReadSectionBloomStrict":            {},
		"ReadTransactionIndex":              {},
		"ReadTransactionIndexStrict":        {},
		"ReadTransactionInfo":               {},
		"ReadTransactionInfoStrict":         {},
		"ReadTransactionInfosByBlock":       {},
		"ReadTransactionInfosByBlockStrict": {},
	}, map[string]map[string]struct{}{
		"actuator/actuator.go": {
			"ReadBlockHashByNumber": {},
		},
		"cmd/gtron/db_cmd.go": {
			"ReadBlockHashByNumber":       {},
			"ReadBlockHashByNumberStrict": {},
		},
		"core/balance_trace_backfill.go": {
			"ReadAccountTrace":            {},
			"ReadAccountTraceStrict":      {},
			"ReadBlockBalanceTrace":       {},
			"ReadBlockBalanceTraceStrict": {},
		},
		"core/blockbuffer/buffer.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/state/pruning/pruner.go": {
			"ReadBlockHashByNumber":       {},
			"ReadBlockHashByNumberStrict": {},
		},
		"core/state/snapshots/cold_builder.go": {
			"ReadBlockHashByNumber":       {},
			"ReadBlockHashByNumberStrict": {},
		},
		"core/state/snapshots/stage_hash.go": {
			"ReadBlockHashByNumberStrict": {},
		},
		"vm/instructions.go": {
			"ReadBlockHashByNumber":       {},
			"ReadBlockHashByNumberStrict": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production archive readers must use the freezer/cold-sidecar-aware ChainDB boundary:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionDerivedHotRowIteratorsStayOnSnapshotBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBReferencesOutsideAllowedFuncs(t, root, derivedHotRowIteratorReferences(), map[string]map[string]struct{}{
		"core/balance_trace_backfill.go": {
			"collectReplayedBalanceTraceRows": {},
		},
		"core/state/snapshots/balance_trace_prune.go": {
			"verifyHotBalanceTraceRowsCovered": {},
		},
		"core/state/snapshots/balance_trace_segment.go": {
			"countBalanceTraceBlockRows":          {},
			"collectBalanceTraceAccountRowsToETL": {},
			"writeBalanceTraceSegmentFromDB":      {},
		},
		"core/state/snapshots/section_bloom_prune.go": {
			"verifyHotSectionBloomRowsCovered": {},
		},
		"core/state/snapshots/section_bloom_segment.go": {
			"collectSectionBloomRowsToETL": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production derived hot-row iterators must stay behind snapshot build/prune/backfill boundaries:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionDerivedIndexWritesStayOnAuditedBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBReferencesOutsideAllowedFuncs(t, root, derivedIndexWriteRawDBReferences(), map[string]map[string]struct{}{
		"core/blockchain.go": {
			"writeBlockMetadataBatch": {},
		},
		"core/state/snapshots/chain_freezer_segment.go": {
			"restoreChainFreezerIndexesForRow": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production derived-index writes must stay in the hot execution batch or collector-backed restore boundary:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionStateHistoryAsOfReadsStayBehindHistoryBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBReferencesOutsideAllowedFuncs(t, root, stateHistoryAsOfRawDBReferences(), map[string]map[string]struct{}{
		"core/state/snapshots/domain_registry.go": {
			"buildDefaultDomainRegistry": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production state history as-of reads must go through state.HistoryReader or the snapshot registry boundary:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionStateLatestReadsStayBehindStateBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBReferencesSkipping(t, root, stateLatestRawDBReferences(), nil, map[string]struct{}{
		"core/state": {},
	})
	if len(offenders) > 0 {
		t.Fatalf("production archive/API code must use state.Database or state.HistoryReader instead of raw state latest readers:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestDerivedHotRowIteratorAuditRejectsAPIBoundaryBypass(t *testing.T) {
	root := writeAuditFixture(t, "core/tron_backend.go", `package core

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var iterAccountTrace = rawdb.IterateAccountTraceRows

func query(db any) {
	_ = rawdb.IterateBlockBalanceTraceRows(db, 1, 2, nil)
	_ = rawdb.IterateSectionBloomRows(db, nil)
	_ = iterAccountTrace(db, 1, 2, nil)
}
`)

	offenders := auditForbiddenRawDBReferences(t, root, derivedHotRowIteratorReferences(), nil)
	if len(offenders) != 3 {
		t.Fatalf("offenders = %+v, want hot derived iterator function value and direct calls rejected", offenders)
	}
	joined := strings.Join(offenders, "\n")
	for _, want := range []string{"rawdb.IterateAccountTraceRows", "rawdb.IterateBlockBalanceTraceRows", "rawdb.IterateSectionBloomRows"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("offenders = %+v, want %s rejected", offenders, want)
		}
	}
}

func TestDerivedHotRowIteratorAuditRejectsSameFileNonBoundaryReference(t *testing.T) {
	root := writeAuditFixture(t, "core/state/snapshots/balance_trace_segment.go", `package snapshots

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func countBalanceTraceBlockRows(db any) {
	_ = rawdb.IterateBlockBalanceTraceRows(db, 1, 2, nil)
}

func debugIterator(db any) {
	_ = rawdb.IterateBlockBalanceTraceRows(db, 1, 2, nil)
}
`)

	offenders := auditForbiddenRawDBReferencesOutsideAllowedFuncs(t, root, derivedHotRowIteratorReferences(), map[string]map[string]struct{}{
		"core/state/snapshots/balance_trace_segment.go": {
			"countBalanceTraceBlockRows": {},
		},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.IterateBlockBalanceTraceRows") {
		t.Fatalf("offenders = %+v, want same-file non-boundary derived iterator rejected", offenders)
	}
}

func TestDerivedIndexWriteAuditRejectsRebuildBypass(t *testing.T) {
	root := writeAuditFixture(t, "cmd/gtron/db_cmd.go", `package main

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var writeTxInfo = rawdb.WriteTransactionInfo

func rebuild(db any, tx []byte) {
	_ = rawdb.WriteTransactionIndex(db, tx, 7)
	_ = writeTxInfo(db, tx, nil)
}
`)

	offenders := auditForbiddenRawDBReferencesOutsideAllowedFuncs(t, root, derivedIndexWriteRawDBReferences(), nil)
	if len(offenders) != 2 {
		t.Fatalf("offenders = %+v, want direct and function-value derived-index writes rejected", offenders)
	}
	joined := strings.Join(offenders, "\n")
	for _, want := range []string{"rawdb.WriteTransactionInfo", "rawdb.WriteTransactionIndex"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("offenders = %+v, want %s rejected", offenders, want)
		}
	}
}

func TestDerivedIndexWriteAuditScopesAllowedBoundariesToFunctions(t *testing.T) {
	root := writeAuditFixture(t, "core/blockchain.go", `package core

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func writeBlockMetadataBatch(db any, tx []byte) {
	_ = rawdb.WriteTransactionIndex(db, tx, 7)
}

func repair(db any, tx []byte) {
	_ = rawdb.WriteTransactionInfo(db, tx, nil)
}
`)

	offenders := auditForbiddenRawDBReferencesOutsideAllowedFuncs(t, root, derivedIndexWriteRawDBReferences(), map[string]map[string]struct{}{
		"core/blockchain.go": {
			"writeBlockMetadataBatch": {},
		},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.WriteTransactionInfo") {
		t.Fatalf("offenders = %+v, want same-file non-boundary derived-index write rejected", offenders)
	}
}

func TestStateHistoryAsOfAuditRejectsDirectRawDBReference(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var readStateAt = rawdb.ReadStateKVAsOfTxNum

func query(db any) {
	_, _, _ = rawdb.ReadStateKVGenerationAsOfTxNum(db, nil, 1, 2)
}
`)

	offenders := auditForbiddenRawDBReferences(t, root, stateHistoryAsOfRawDBReferences(), nil)
	if len(offenders) != 2 {
		t.Fatalf("offenders = %+v, want rawdb state-as-of function value and direct call rejected", offenders)
	}
	joined := strings.Join(offenders, "\n")
	if !strings.Contains(joined, "rawdb.ReadStateKVAsOfTxNum") || !strings.Contains(joined, "rawdb.ReadStateKVGenerationAsOfTxNum") {
		t.Fatalf("offenders = %+v, want both state-as-of references rejected", offenders)
	}
}

func TestStateHistoryAsOfAuditRejectsSameFileNonBoundaryReference(t *testing.T) {
	root := writeAuditFixture(t, "core/state/snapshots/domain_registry.go", `package snapshots

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func buildDefaultDomainRegistry() {
	_ = rawdb.ReadStateKVAsOfTxNum
}

func debugAsOfReader(db any) {
	_, _, _ = rawdb.ReadStateKVGenerationAsOfTxNum(db, nil, 1, 2)
}
`)

	offenders := auditForbiddenRawDBReferencesOutsideAllowedFuncs(t, root, stateHistoryAsOfRawDBReferences(), map[string]map[string]struct{}{
		"core/state/snapshots/domain_registry.go": {
			"buildDefaultDomainRegistry": {},
		},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadStateKVGenerationAsOfTxNum") {
		t.Fatalf("offenders = %+v, want same-file non-boundary state history as-of read rejected", offenders)
	}
}

func TestStateLatestAuditRejectsArchiveAPIRawDBReference(t *testing.T) {
	root := writeAuditFixture(t, "core/tron_backend.go", `package core

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var readLatest = rawdb.ReadStateKVLatest

func query(db any) {
	_, _, _ = rawdb.ReadStateKVGeneration(db, [20]byte{})
}
`)

	offenders := auditForbiddenRawDBReferencesSkipping(t, root, stateLatestRawDBReferences(), nil, map[string]struct{}{
		"core/state": {},
	})
	if len(offenders) != 2 {
		t.Fatalf("offenders = %+v, want raw state latest function value and generation read rejected", offenders)
	}
	joined := strings.Join(offenders, "\n")
	if !strings.Contains(joined, "rawdb.ReadStateKVLatest") || !strings.Contains(joined, "rawdb.ReadStateKVGeneration") {
		t.Fatalf("offenders = %+v, want both state latest references rejected", offenders)
	}
}

func TestStateLatestAuditRejectsArchiveAPIRawDBIteratorReference(t *testing.T) {
	root := writeAuditFixture(t, "core/tron_backend.go", `package core

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var iterCode = rawdb.IterateStateCode

func query(db any) {
	_ = rawdb.IterateStateKVLatestRows(db, nil)
	_ = rawdb.IterateStateKVGeneration(db, nil, nil)
	_ = iterCode(db, nil)
}
`)

	offenders := auditForbiddenRawDBReferencesSkipping(t, root, stateLatestRawDBReferences(), nil, map[string]struct{}{
		"core/state": {},
	})
	if len(offenders) != 3 {
		t.Fatalf("offenders = %+v, want raw state latest iterator references rejected", offenders)
	}
	joined := strings.Join(offenders, "\n")
	for _, want := range []string{"rawdb.IterateStateCode", "rawdb.IterateStateKVGeneration", "rawdb.IterateStateKVLatestRows"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("offenders = %+v, want %s rejected", offenders, want)
		}
	}
}

func TestStateLatestAuditAllowsStatePackageBoundary(t *testing.T) {
	root := writeAuditFixture(t, "core/state/store.go", `package state

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(db any) {
	_, _, _ = rawdb.ReadStateAccountLatest(db, [20]byte{})
}
`)

	offenders := auditForbiddenRawDBReferencesSkipping(t, root, stateLatestRawDBReferences(), nil, map[string]struct{}{
		"core/state": {},
	})
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want state package boundary accepted", offenders)
	}
}

func TestColdArchiveAuditRejectsStrictBlockHashReadOnHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(db any) {
	_, _, _ = rawdb.ReadBlockHashByNumberStrict(db, 7)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadBlockHashByNumberStrict": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadBlockHashByNumberStrict") {
		t.Fatalf("offenders = %+v, want strict block-hash read on hot store rejected", offenders)
	}
}

func TestColdArchiveAuditRejectsStrictTransactionInfoReadOnHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(db any, tx []byte) {
	_, _, _ = rawdb.ReadTransactionInfoStrict(db, tx)
	_, _, _ = rawdb.ReadTransactionInfosByBlockStrict(db, 7)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfoStrict":         {},
		"ReadTransactionInfosByBlockStrict": {},
	}, nil)
	if len(offenders) != 2 {
		t.Fatalf("offenders = %+v, want hot-store strict transaction info reads rejected", offenders)
	}
	joined := strings.Join(offenders, "\n")
	for _, want := range []string{"rawdb.ReadTransactionInfoStrict", "rawdb.ReadTransactionInfosByBlockStrict"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("offenders = %+v, want %s rejected", offenders, want)
		}
	}
}

func TestColdArchiveAuditRejectsStrictStateRootReadsOnHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import (
	"github.com/tronprotocol/go-tron/common"
	rawdb "github.com/tronprotocol/go-tron/core/rawdb"
)

func query(db any, hash common.Hash) {
	_, _, _ = rawdb.ReadBlockStateRootRawStrict(db, hash)
	_, _, _ = rawdb.ReadBlockStateRootStrict(db, hash)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadBlockStateRootRawStrict": {},
		"ReadBlockStateRootStrict":    {},
	}, nil)
	if len(offenders) != 2 {
		t.Fatalf("offenders = %+v, want strict state-root reads on hot store rejected", offenders)
	}
	joined := strings.Join(offenders, "\n")
	for _, want := range []string{"rawdb.ReadBlockStateRootRawStrict", "rawdb.ReadBlockStateRootStrict"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("offenders = %+v, want %s rejected", offenders, want)
		}
	}
}

func TestColdArchiveAuditAllowsStrictStateRootReadsOnChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import (
	"github.com/tronprotocol/go-tron/common"
	rawdb "github.com/tronprotocol/go-tron/core/rawdb"
)

func query(db *rawdb.ChainDB, hash common.Hash) {
	chainDB := db
	_, _, _ = rawdb.ReadBlockStateRootRawStrict(chainDB, hash)
	_, _, _ = rawdb.ReadBlockStateRootStrict(chainDB, hash)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadBlockStateRootRawStrict": {},
		"ReadBlockStateRootStrict":    {},
	}, nil)
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want ChainDB strict state-root boundary accepted", offenders)
	}
}

func TestColdArchiveAuditRejectsStrictTraceReadsOnHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(db any, owner []byte) {
	_, _, _ = rawdb.ReadBlockBalanceTraceStrict(db, 7)
	_, _, _ = rawdb.ReadAccountTraceStrict(db, owner, 7)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadAccountTraceStrict":      {},
		"ReadBlockBalanceTraceStrict": {},
	}, nil)
	if len(offenders) != 2 {
		t.Fatalf("offenders = %+v, want strict trace reads on hot store rejected", offenders)
	}
	joined := strings.Join(offenders, "\n")
	for _, want := range []string{"rawdb.ReadAccountTraceStrict", "rawdb.ReadBlockBalanceTraceStrict"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("offenders = %+v, want %s rejected", offenders, want)
		}
	}
}

func TestColdArchiveAuditAllowsStrictTraceReadsOnChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(db *rawdb.ChainDB, owner []byte) {
	chainDB := db
	_, _, _ = rawdb.ReadBlockBalanceTraceStrict(chainDB, 7)
	_, _, _ = rawdb.ReadAccountTraceStrict(chainDB, owner, 7)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadAccountTraceStrict":      {},
		"ReadBlockBalanceTraceStrict": {},
	}, nil)
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want ChainDB strict trace boundary accepted", offenders)
	}
}

func TestColdArchiveAuditAllowsStrictTransactionInfoReadOnChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(db *rawdb.ChainDB, tx []byte) {
	chainDB := db
	_, _, _ = rawdb.ReadTransactionInfoStrict(chainDB, tx)
	_, _, _ = rawdb.ReadTransactionInfosByBlockStrict(chainDB, 7)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfoStrict":         {},
		"ReadTransactionInfosByBlockStrict": {},
	}, nil)
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want ChainDB strict transaction info boundary accepted", offenders)
	}
}

func TestColdArchiveAuditAllowsTypedChainDBParameter(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(source *rawdb.ChainDB, tx []byte) {
	_ = rawdb.ReadTransactionInfo(source, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want typed ChainDB parameter accepted", offenders)
	}
}

func TestColdArchiveAuditRejectsBlockChainDBMethodHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

type chain struct{}

func (chain) DB() any { return nil }

func query(chain chain, tx []byte) {
	_ = rawdb.ReadTransactionInfo(chain.DB(), tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadTransactionInfo") {
		t.Fatalf("offenders = %+v, want chain.DB() hot store rejected", offenders)
	}
}

func TestColdArchiveAuditRejectsChainDBMethodHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

type chain struct{}

func (chain) ChainDB() any { return nil }

func query(chain chain, tx []byte) {
	_ = rawdb.ReadTransactionInfo(chain.ChainDB(), tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadTransactionInfo") {
		t.Fatalf("offenders = %+v, want ChainDB() method with hot-store return rejected", offenders)
	}
}

func TestColdArchiveAuditAllowsBlockChainChainDBMethodBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

type chain struct{}

func (chain) ChainDB() *rawdb.ChainDB { return nil }

func query(chain chain, tx []byte) {
	_ = rawdb.ReadTransactionInfo(chain.ChainDB(), tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want chain.ChainDB() boundary accepted", offenders)
	}
}

func TestColdArchiveAuditRejectsTrustedNameOnHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(source any, tx []byte) {
	_ = rawdb.ReadTransactionInfo(source, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadTransactionInfo") {
		t.Fatalf("offenders = %+v, want trusted-name hot store rejected", offenders)
	}
}

func TestColdArchiveAuditRejectsSelectorNamedChainDBOnHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

type holder struct {
	chaindb any
}

func query(h holder, tx []byte) {
	_ = rawdb.ReadTransactionInfo(h.chaindb, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadTransactionInfo") {
		t.Fatalf("offenders = %+v, want selector-named hot store rejected", offenders)
	}
}

func TestColdArchiveAuditAllowsNewChainDBAliasBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(tx []byte) {
	archive := rawdb.NewChainDB(nil, nil)
	_ = rawdb.ReadTransactionInfo(archive, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want NewChainDB alias accepted", offenders)
	}
}

func TestColdArchiveAuditAllowsTypedChainDBFieldSelector(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

type holder struct {
	chaindb *rawdb.ChainDB
}

func query(h holder, tx []byte) {
	_ = rawdb.ReadTransactionInfo(h.chaindb, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want typed ChainDB field selector accepted", offenders)
	}
}

func TestColdArchiveAuditRejectsReaderFunctionValueOnHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var readTxInfo = rawdb.ReadTransactionInfo

func query(source any, tx []byte) {
	_ = readTxInfo(source, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadTransactionInfo") {
		t.Fatalf("offenders = %+v, want rawdb reader function value on hot store rejected", offenders)
	}
}

func TestColdArchiveAuditRejectsDotImportedReaderOnHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import . "github.com/tronprotocol/go-tron/core/rawdb"

var readTxInfo = ReadTransactionInfo

func direct(source any, tx []byte) {
	_ = ReadTransactionInfo(source, tx)
}

func indirect(source any, tx []byte) {
	_ = readTxInfo(source, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 2 {
		t.Fatalf("offenders = %+v, want direct and function-value dot-imported readers rejected", offenders)
	}
	for _, offender := range offenders {
		if !strings.Contains(offender, "rawdb.ReadTransactionInfo") {
			t.Fatalf("offenders = %+v, want dot-imported transaction info readers reported as rawdb.ReadTransactionInfo", offenders)
		}
	}
}

func TestColdArchiveAuditAllowsDotImportedReaderOnChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import . "github.com/tronprotocol/go-tron/core/rawdb"

var readTxInfo = ReadTransactionInfo

func direct(chainDB *ChainDB, tx []byte) {
	_ = ReadTransactionInfo(chainDB, tx)
}

func indirect(chainDB *ChainDB, tx []byte) {
	_ = readTxInfo(chainDB, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want dot-imported readers on ChainDB boundary accepted", offenders)
	}
}

func TestColdArchiveAuditRejectsColdIndexReaderFunctionValuesOnHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var readTxIndex = rawdb.ReadTransactionIndex
var readBlockNumber = rawdb.ReadBlockNumber

func query(source any, tx []byte, hash [32]byte) {
	_ = readTxIndex(source, tx)
	_ = readBlockNumber(source, hash)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadBlockNumber":      {},
		"ReadTransactionIndex": {},
	}, nil)
	if len(offenders) != 2 {
		t.Fatalf("offenders = %+v, want both cold index reader function values rejected", offenders)
	}
	joined := strings.Join(offenders, "\n")
	if !strings.Contains(joined, "rawdb.ReadTransactionIndex") || !strings.Contains(joined, "rawdb.ReadBlockNumber") {
		t.Fatalf("offenders = %+v, want both cold index reader aliases reported", offenders)
	}
}

func TestColdArchiveAuditAllowsReaderFunctionValueOnChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var readTxInfo = rawdb.ReadTransactionInfo

func query(chainDB *rawdb.ChainDB, tx []byte) {
	_ = readTxInfo(chainDB, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want rawdb reader function value on ChainDB boundary accepted", offenders)
	}
}

func TestColdArchiveAuditRejectsSelectorReaderFunctionValueOnHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

type readers struct {
	readTxInfo func(any, []byte) any
}

func query(source any, tx []byte) {
	var r readers
	r.readTxInfo = rawdb.ReadTransactionInfo
	_ = r.readTxInfo(source, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadTransactionInfo") {
		t.Fatalf("offenders = %+v, want rawdb selector reader function value on hot store rejected", offenders)
	}
}

func TestColdArchiveAuditAllowsSelectorReaderFunctionValueOnChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

type readers struct {
	readTxInfo func(any, []byte) any
}

func query(chainDB *rawdb.ChainDB, tx []byte) {
	var r readers
	r.readTxInfo = rawdb.ReadTransactionInfo
	_ = r.readTxInfo(chainDB, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want selector reader function value on ChainDB boundary accepted", offenders)
	}
}

func TestColdArchiveAuditReaderFunctionValueDoesNotLeakAcrossFunctions(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func first(source any, tx []byte) {
	readTxInfo := rawdb.ReadTransactionInfo
	_ = readTxInfo(source, tx)
}

func second(source any, tx []byte, readTxInfo func(any, []byte) any) {
	_ = readTxInfo(source, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadTransactionInfo") {
		t.Fatalf("offenders = %+v, want only the local rawdb reader alias rejected", offenders)
	}
}

func TestColdArchiveAuditChainDBAliasDoesNotLeakAcrossFunctions(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func first(db *rawdb.ChainDB) {
	archive := db
	_ = archive
}

func second(archive any, tx []byte) {
	_ = rawdb.ReadTransactionInfo(archive, tx)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfo": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadTransactionInfo") {
		t.Fatalf("offenders = %+v, want ChainDB alias from first function not to bless second function hot-store read", offenders)
	}
}

func TestProductionEventLogQueriesUseChainDBBoundary(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"EventLogRangeCovered":          {},
		"EventLogRangeCoveredForFilter": {},
		"IterateEventLogs":              {},
		"IterateCoveredEventLogs":       {},
	})
	if len(offenders) > 0 {
		t.Fatalf("production event-log queries must go through the cold-sidecar-aware ChainDB boundary:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionEventLogCoverageChecksStayOnAuditedBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditEventLogCoverageCheckCalls(t, root, eventLogCoverageCheckMethods(), map[string]map[string]struct{}{
		"cmd/gtron/snapshot_cmd.go": {
			"snapshotBuildDerivedIndexesFromCold": {},
			"snapshotBuildEventLogsCmd":           {},
		},
		"core/state/snapshots/event_log_segment.go": {
			"BuildEventLogSegmentFromReaderWithOptions": {},
			"EventLogIndexedRangeCovered":               {},
			"EventLogRangeCoveredForFilter":             {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production event-log coverage checks must stay behind audited snapshot boundaries; API queries should use IterateCoveredEventLogs:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionEventLogIndexedCoverageChecksStayOnAuditedBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditEventLogIndexedCoverageCalls(t, root, map[string]map[string]struct{}{
		"cmd/gtron/db_cmd.go": {
			"dbStageStatusSnapshotCoverageIssues": {},
		},
		"core/state/snapshots/chain_tail_prune.go": {
			"verifyColdEventLogTailCoverage": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production indexed event-log coverage checks must stay on snapshot prune or db diagnostics boundaries:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestEventLogCoverageAuditRejectsAPIBoundaryBypass(t *testing.T) {
	root := writeAuditFixture(t, "core/tron_backend.go", `package core

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(db *rawdb.ChainDB) {
	_, _ = db.EventLogRangeCoveredForFilter(1, 2, rawdb.EventLogFilter{})
}
`)

	offenders := auditEventLogCoverageCheckCalls(t, root, eventLogCoverageCheckMethods(), nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "EventLogRangeCoveredForFilter") {
		t.Fatalf("offenders = %+v, want API event-log coverage check rejected", offenders)
	}
}

func TestEventLogCoverageAuditScopesAllowedBoundariesToFunctions(t *testing.T) {
	root := writeAuditFixture(t, "app/coverage.go", `package app

type coldManager struct{}

func (coldManager) EventLogRangeCovered(uint64, uint64) (bool, error) {
	return true, nil
}

func allowed(m coldManager) {
	_, _ = m.EventLogRangeCovered(1, 2)
}

func query(m coldManager) {
	_, _ = m.EventLogRangeCovered(1, 2)
}
`)

	offenders := auditEventLogCoverageCheckCalls(t, root, eventLogCoverageCheckMethods(), map[string]map[string]struct{}{
		"app/coverage.go": {
			"allowed": {},
		},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "EventLogRangeCovered") {
		t.Fatalf("offenders = %+v, want same-file event-log coverage check outside allowed function rejected", offenders)
	}
}

func TestEventLogIndexedCoverageAuditRejectsAPIBoundaryBypass(t *testing.T) {
	root := writeAuditFixture(t, "core/tron_backend.go", `package core

type coldManager struct{}

func (coldManager) EventLogIndexedRangeCovered(uint64, uint64) (bool, error) {
	return true, nil
}

func query(m coldManager) {
	_, _ = m.EventLogIndexedRangeCovered(1, 2)
}
`)

	offenders := auditEventLogIndexedCoverageCalls(t, root, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "EventLogIndexedRangeCovered") {
		t.Fatalf("offenders = %+v, want indexed event-log coverage call rejected outside audited boundary", offenders)
	}
}

func TestEventLogIndexedCoverageAuditScopesAllowedBoundariesToFunctions(t *testing.T) {
	root := writeAuditFixture(t, "app/coverage.go", `package app

type coldManager struct{}

func (coldManager) EventLogIndexedRangeCovered(uint64, uint64) (bool, error) {
	return true, nil
}

func allowed(m coldManager) {
	_, _ = m.EventLogIndexedRangeCovered(1, 2)
}

func query(m coldManager) {
	_, _ = m.EventLogIndexedRangeCovered(1, 2)
}
`)

	offenders := auditEventLogIndexedCoverageCalls(t, root, map[string]map[string]struct{}{
		"app/coverage.go": {
			"allowed": {},
		},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "EventLogIndexedRangeCovered") {
		t.Fatalf("offenders = %+v, want same-file indexed event-log coverage call outside allowed function rejected", offenders)
	}
}

func TestEventLogAuditRejectsNonChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import "github.com/tronprotocol/go-tron/core/rawdb"

type coldManager struct{}

func (coldManager) IterateEventLogs(uint64, uint64, rawdb.EventLogFilter, func(rawdb.EventLog) (bool, error)) error {
	return nil
}

func query(m coldManager) {
	_ = m.IterateEventLogs(1, 2, rawdb.EventLogFilter{}, nil)
}
`)

	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"IterateEventLogs": {},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "IterateEventLogs") {
		t.Fatalf("offenders = %+v, want non-ChainDB IterateEventLogs call", offenders)
	}
}

func TestEventLogAuditRejectsCoveredIteratorOnNonChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import "github.com/tronprotocol/go-tron/core/rawdb"

type coldManager struct{}

func (coldManager) IterateCoveredEventLogs(uint64, uint64, rawdb.EventLogFilter, func(rawdb.EventLog) (bool, error)) (bool, error) {
	return true, nil
}

func query(m coldManager) {
	_, _ = m.IterateCoveredEventLogs(1, 2, rawdb.EventLogFilter{}, nil)
}
`)

	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"IterateCoveredEventLogs": {},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "IterateCoveredEventLogs") {
		t.Fatalf("offenders = %+v, want non-ChainDB IterateCoveredEventLogs call", offenders)
	}
}

func TestEventLogAuditRejectsMethodValueOnNonChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import "github.com/tronprotocol/go-tron/core/rawdb"

type coldManager struct{}

func (coldManager) IterateEventLogs(uint64, uint64, rawdb.EventLogFilter, func(rawdb.EventLog) (bool, error)) error {
	return nil
}

func query(m coldManager) {
	iter := m.IterateEventLogs
	_ = iter(1, 2, rawdb.EventLogFilter{}, nil)
}
`)

	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"IterateEventLogs": {},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "IterateEventLogs") {
		t.Fatalf("offenders = %+v, want non-ChainDB IterateEventLogs method value rejected", offenders)
	}
}

func TestEventLogAuditAllowsMethodValueOnChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import "github.com/tronprotocol/go-tron/core/rawdb"

func query(db *rawdb.ChainDB) {
	iter := db.IterateEventLogs
	_ = iter(1, 2, rawdb.EventLogFilter{}, nil)
}
`)

	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"IterateEventLogs": {},
	})
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want ChainDB IterateEventLogs method value accepted", offenders)
	}
}

func TestEventLogAuditMethodValueDoesNotLeakAcrossFunctions(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import "github.com/tronprotocol/go-tron/core/rawdb"

type coldManager struct{}

func (coldManager) IterateEventLogs(uint64, uint64, rawdb.EventLogFilter, func(rawdb.EventLog) (bool, error)) error {
	return nil
}

func first(m coldManager) {
	iter := m.IterateEventLogs
	_ = iter(1, 2, rawdb.EventLogFilter{}, nil)
}

func second(iter func(uint64, uint64, rawdb.EventLogFilter, func(rawdb.EventLog) (bool, error)) error) {
	_ = iter(1, 2, rawdb.EventLogFilter{}, nil)
}
`)

	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"IterateEventLogs": {},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "IterateEventLogs") {
		t.Fatalf("offenders = %+v, want only the local non-ChainDB IterateEventLogs alias rejected", offenders)
	}
}

func TestEventLogAuditAllowsChainDBAliasBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import "github.com/tronprotocol/go-tron/core/rawdb"

func query(db *rawdb.ChainDB) {
	chainDB := db
	_, _ = chainDB.EventLogRangeCoveredForFilter(1, 2, rawdb.EventLogFilter{})
}
`)

	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"EventLogRangeCoveredForFilter": {},
	})
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want ChainDB alias event-log boundary accepted", offenders)
	}
}

func TestEventLogAuditRejectsChainDBMethodHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import "github.com/tronprotocol/go-tron/core/rawdb"

type chain struct{}

func (chain) ChainDB() any { return nil }

func query(chain chain) {
	_, _ = chain.ChainDB().EventLogRangeCovered(1, 2)
}
`)

	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"EventLogRangeCovered": {},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "EventLogRangeCovered") {
		t.Fatalf("offenders = %+v, want ChainDB() method with hot-store return rejected", offenders)
	}
}

func TestEventLogAuditAllowsTypedChainDBMethodBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import "github.com/tronprotocol/go-tron/core/rawdb"

type chain struct{}

func (chain) ChainDB() *rawdb.ChainDB { return nil }

func query(chain chain) {
	_, _ = chain.ChainDB().EventLogRangeCovered(1, 2)
}
`)

	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"EventLogRangeCovered": {},
	})
	if len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want typed ChainDB() method accepted", offenders)
	}
}

func TestEventLogAuditRejectsTrustedNameOnNonChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import "github.com/tronprotocol/go-tron/core/rawdb"

type coldManager struct{}

func (coldManager) EventLogRangeCovered(uint64, uint64) (bool, error) {
	return true, nil
}

func query(chainDB coldManager) {
	_, _ = chainDB.EventLogRangeCovered(1, 2)
}
`)

	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"EventLogRangeCovered": {},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "EventLogRangeCovered") {
		t.Fatalf("offenders = %+v, want trusted-name non-ChainDB event-log call rejected", offenders)
	}
}

func TestEventLogAuditRejectsSelectorNamedChainDBOnNonChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import "github.com/tronprotocol/go-tron/core/rawdb"

type coldManager struct{}

func (coldManager) EventLogRangeCovered(uint64, uint64) (bool, error) {
	return true, nil
}

type holder struct {
	chaindb coldManager
}

func query(h holder) {
	_, _ = h.chaindb.EventLogRangeCovered(1, 2)
}
`)

	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"EventLogRangeCovered": {},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "EventLogRangeCovered") {
		t.Fatalf("offenders = %+v, want selector-named non-ChainDB event-log boundary rejected", offenders)
	}
}

func TestEventLogAuditChainDBAliasDoesNotLeakAcrossFunctions(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import "github.com/tronprotocol/go-tron/core/rawdb"

type coldManager struct{}

func (coldManager) EventLogRangeCovered(uint64, uint64) (bool, error) {
	return true, nil
}

func first(db *rawdb.ChainDB) {
	chainDB := db
	_ = chainDB
}

func second(chainDB coldManager) {
	_, _ = chainDB.EventLogRangeCovered(1, 2)
}
`)

	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"EventLogRangeCovered": {},
	})
	if len(offenders) != 1 || !strings.Contains(offenders[0], "EventLogRangeCovered") {
		t.Fatalf("offenders = %+v, want ChainDB alias from first function not to bless second function event-log call", offenders)
	}
}

func TestSnapshotPublishersUseStrictTransactionInfoReads(t *testing.T) {
	repoRoot := findRepoRoot(t)
	snapshotRoot := filepath.Join(repoRoot, "core", "state", "snapshots")
	offenders := auditForbiddenRawDBReferences(t, snapshotRoot, map[string]struct{}{
		"ReadTransactionInfosByBlock": {},
	}, nil)
	if len(offenders) > 0 {
		t.Fatalf("snapshot builders must use ReadTransactionInfosByBlockStrict so corrupt TransactionRet rows fail cold coverage publishing:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestSnapshotPublisherAuditRejectsTransactionInfoReaderFunctionValue(t *testing.T) {
	root := writeAuditFixture(t, "core/state/snapshots/offender.go", `package snapshots

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

var readTransactionInfos = rawdb.ReadTransactionInfosByBlock

func publish(db any) {
	_ = readTransactionInfos(db, 7)
}
`)

	offenders := auditForbiddenRawDBReferences(t, filepath.Join(root, "core", "state", "snapshots"), map[string]struct{}{
		"ReadTransactionInfosByBlock": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadTransactionInfosByBlock") {
		t.Fatalf("offenders = %+v, want non-strict transaction info reader function value rejected", offenders)
	}
}

func TestAuditHotOnlyReadsScriptCoversSourceAuditFixtures(t *testing.T) {
	root := findRepoRoot(t)
	scriptPath := filepath.Join(root, "scripts", "dev", "audit_hot_only_reads.sh")
	pattern := auditHotOnlyReadsScriptPattern(t, scriptPath)
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("compile %s pattern %q: %v", scriptPath, pattern, err)
	}
	var missing []string
	for _, name := range rawdbPackageTestNames(t, filepath.Join(root, "core", "rawdb")) {
		if auditHotOnlyReadsScriptShouldCover(name) && !re.MatchString(name) {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%s does not cover rawdb source-audit tests:\n%s", scriptPath, strings.Join(missing, "\n"))
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository root with go.mod not found")
		}
		dir = parent
	}
}

func writeAuditFixture(t *testing.T, rel, body string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module auditfixture\n"), 0o644); err != nil {
		t.Fatalf("write fixture go.mod: %v", err)
	}
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir fixture: %v", err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write fixture source: %v", err)
	}
	return root
}

func auditHotOnlyReadsScriptPattern(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "pattern='") && strings.HasSuffix(line, "'") {
			return strings.TrimSuffix(strings.TrimPrefix(line, "pattern='"), "'")
		}
	}
	t.Fatalf("%s: pattern='...' assignment not found", path)
	return ""
}

func rawdbPackageTestNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var names []string
	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && strings.HasPrefix(fn.Name.Name, "Test") {
				names = append(names, fn.Name.Name)
			}
		}
	}
	sort.Strings(names)
	return names
}

func auditHotOnlyReadsScriptShouldCover(name string) bool {
	exact := map[string]struct{}{
		"TestAuditHotOnlyReadsScriptCoversSourceAuditFixtures":               {},
		"TestNoProductionHotBlockKVReadReferences":                           {},
		"TestNoUnexpectedProductionRawFreezerReadReferences":                 {},
		"TestNoActuatorDirectHotBlockHashReads":                              {},
		"TestProductionBlockHashByNumberReadsStayOnAuditedBoundaries":        {},
		"TestVMBlockHashReadsUseStrictBoundary":                              {},
		"TestProductionHotOnlyChainDBConstructorsStayOnAuditedBoundaries":    {},
		"TestProductionColdArchiveReadersUseChainDBBoundary":                 {},
		"TestProductionDerivedHotRowIteratorsStayOnSnapshotBoundaries":       {},
		"TestProductionDerivedIndexWritesStayOnAuditedBoundaries":            {},
		"TestProductionStateHistoryAsOfReadsStayBehindHistoryBoundaries":     {},
		"TestProductionStateLatestReadsStayBehindStateBoundaries":            {},
		"TestProductionEventLogQueriesUseChainDBBoundary":                    {},
		"TestProductionEventLogCoverageChecksStayOnAuditedBoundaries":        {},
		"TestProductionEventLogIndexedCoverageChecksStayOnAuditedBoundaries": {},
		"TestSnapshotPublishersUseStrictTransactionInfoReads":                {},
	}
	if _, ok := exact[name]; ok {
		return true
	}
	for _, prefix := range []string{
		"TestHotBlockKVAudit",
		"TestRawFreezerReadAudit",
		"TestBlockHashByNumberAudit",
		"TestHotOnlyChainDBAudit",
		"TestColdArchiveAudit",
		"TestDerivedHotRowIteratorAudit",
		"TestDerivedIndexWriteAudit",
		"TestStateHistoryAsOfAudit",
		"TestStateLatestAudit",
		"TestEventLogAudit",
		"TestEventLogCoverageAudit",
		"TestEventLogIndexedCoverageAudit",
		"TestSnapshotPublisherAudit",
	} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func stateHistoryAsOfRawDBReferences() map[string]struct{} {
	return map[string]struct{}{
		"ReadStateKVAsOf":                      {},
		"ReadStateAccountLatestAsOf":           {},
		"ReadStateAccountLatestAsOfTxNum":      {},
		"ReadStateKVAsOfTxNum":                 {},
		"ReadStateKVGenerationAsOf":            {},
		"ReadStateKVGenerationAsOfTxNum":       {},
		"ReadStateAccountKVAsOf":               {},
		"ReadStateAccountKVAsOfTxNum":          {},
		"IterateStateKVAsOfPrefix":             {},
		"IterateStateKVAsOfPrefixTxNum":        {},
		"IterateStateAccountKVAsOfPrefix":      {},
		"IterateStateAccountKVAsOfPrefixTxNum": {},
	}
}

func stateLatestRawDBReferences() map[string]struct{} {
	return map[string]struct{}{
		"IterateStateAccountLatest":      {},
		"IterateStateCode":               {},
		"IterateStateKVGeneration":       {},
		"IterateStateKVLatest":           {},
		"IterateStateKVLatestDomainRows": {},
		"IterateStateKVLatestRows":       {},
		"ReadStateAccountLatest":         {},
		"ReadStateCode":                  {},
		"ReadStateKVGeneration":          {},
		"ReadStateKVLatest":              {},
	}
}

func derivedHotRowIteratorReferences() map[string]struct{} {
	return map[string]struct{}{
		"IterateAccountTraceRows":      {},
		"IterateBlockBalanceTraceRows": {},
		"IterateSectionBloomRows":      {},
	}
}

func derivedIndexWriteRawDBReferences() map[string]struct{} {
	return map[string]struct{}{
		"WriteAccountTrace":            {},
		"WriteBlockBalanceTrace":       {},
		"WriteSectionBloom":            {},
		"WriteTransactionIndex":        {},
		"WriteTransactionInfo":         {},
		"WriteTransactionInfosByBlock": {},
	}
}

func auditForbiddenRawDBCalls(t *testing.T, root string, forbidden map[string]struct{}, allowed map[string]map[string]struct{}) []string {
	t.Helper()
	var offenders []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", ".codex", "build", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.HasPrefix(path, filepath.Join(root, "core", "rawdb")+string(os.PathSeparator)) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rawdbNames := rawdbImportNames(file)
		if len(rawdbNames) == 0 {
			return nil
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				ident, ok := fun.X.(*ast.Ident)
				if !ok {
					return true
				}
				if _, imported := rawdbNames[ident.Name]; !imported {
					return true
				}
				if _, banned := forbidden[fun.Sel.Name]; banned && !isAllowedRawDBCall(root, path, fun.Sel.Name, allowed) {
					offenders = append(offenders, formatAuditOffender(fset, root, path, call.Pos(), ident.Name+"."+fun.Sel.Name))
				}
			case *ast.Ident:
				if _, dotImported := rawdbNames["."]; !dotImported {
					return true
				}
				if _, banned := forbidden[fun.Name]; banned {
					if isAllowedRawDBCall(root, path, fun.Name, allowed) {
						return true
					}
					offenders = append(offenders, formatAuditOffender(fset, root, path, call.Pos(), fun.Name))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("audit forbidden rawdb calls: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func auditForbiddenRawDBCallsOutsideAllowedFuncs(t *testing.T, root string, forbidden map[string]struct{}, allowed map[string]map[string]struct{}) []string {
	t.Helper()
	var offenders []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", ".codex", "build", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.HasPrefix(path, filepath.Join(root, "core", "rawdb")+string(os.PathSeparator)) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rawdbNames := rawdbImportNames(file)
		if len(rawdbNames) == 0 {
			return nil
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && isAllowedAuditFunc(root, path, fn.Name.Name, allowed) {
				continue
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					ident, ok := fun.X.(*ast.Ident)
					if !ok {
						return true
					}
					if _, imported := rawdbNames[ident.Name]; !imported {
						return true
					}
					if _, banned := forbidden[fun.Sel.Name]; banned {
						offenders = append(offenders, formatAuditOffender(fset, root, path, call.Pos(), ident.Name+"."+fun.Sel.Name))
					}
				case *ast.Ident:
					if _, dotImported := rawdbNames["."]; !dotImported {
						return true
					}
					if _, banned := forbidden[fun.Name]; banned {
						offenders = append(offenders, formatAuditOffender(fset, root, path, call.Pos(), fun.Name))
					}
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("audit forbidden rawdb calls outside allowed funcs: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func auditForbiddenRawDBReferences(t *testing.T, root string, forbidden map[string]struct{}, allowed map[string]map[string]struct{}) []string {
	t.Helper()
	return auditForbiddenRawDBReferencesSkipping(t, root, forbidden, allowed, nil)
}

func auditForbiddenRawDBReferencesSkipping(t *testing.T, root string, forbidden map[string]struct{}, allowed map[string]map[string]struct{}, skipRelDirs map[string]struct{}) []string {
	t.Helper()
	var offenders []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			rel := auditRelPath(root, path)
			if _, skip := skipRelDirs[rel]; skip {
				return filepath.SkipDir
			}
			switch entry.Name() {
			case ".git", ".claude", ".codex", "build", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.HasPrefix(path, filepath.Join(root, "core", "rawdb")+string(os.PathSeparator)) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rawdbNames := rawdbImportNames(file)
		if len(rawdbNames) == 0 {
			return nil
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.SelectorExpr:
				ident, ok := n.X.(*ast.Ident)
				if !ok {
					return true
				}
				if _, imported := rawdbNames[ident.Name]; !imported {
					return true
				}
				if _, banned := forbidden[n.Sel.Name]; banned && !isAllowedRawDBCall(root, path, n.Sel.Name, allowed) {
					offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), ident.Name+"."+n.Sel.Name))
				}
			case *ast.Ident:
				if _, dotImported := rawdbNames["."]; !dotImported {
					return true
				}
				if _, banned := forbidden[n.Name]; banned && !isAllowedRawDBCall(root, path, n.Name, allowed) {
					offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), n.Name))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("audit forbidden rawdb references: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func auditForbiddenRawDBReferencesOutsideAllowedFuncs(t *testing.T, root string, forbidden map[string]struct{}, allowed map[string]map[string]struct{}) []string {
	t.Helper()
	var offenders []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", ".codex", "build", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.HasPrefix(path, filepath.Join(root, "core", "rawdb")+string(os.PathSeparator)) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rawdbNames := rawdbImportNames(file)
		if len(rawdbNames) == 0 {
			return nil
		}
		for _, decl := range file.Decls {
			fnName := ""
			if fn, ok := decl.(*ast.FuncDecl); ok {
				fnName = fn.Name.Name
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.SelectorExpr:
					ident, ok := n.X.(*ast.Ident)
					if !ok {
						return true
					}
					if _, imported := rawdbNames[ident.Name]; !imported {
						return true
					}
					if _, banned := forbidden[n.Sel.Name]; banned && !isAllowedAuditFunc(root, path, fnName, allowed) {
						offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), ident.Name+"."+n.Sel.Name))
					}
				case *ast.Ident:
					if _, dotImported := rawdbNames["."]; !dotImported {
						return true
					}
					if _, banned := forbidden[n.Name]; banned && !isAllowedAuditFunc(root, path, fnName, allowed) {
						offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), n.Name))
					}
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("audit forbidden rawdb references outside allowed funcs: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func auditHotOnlyChainDBConstructors(t *testing.T, root string, allowed map[string]map[string]struct{}) []string {
	t.Helper()
	var offenders []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", ".codex", "build", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.HasPrefix(path, filepath.Join(root, "core", "rawdb")+string(os.PathSeparator)) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rawdbNames := rawdbImportNames(file)
		if len(rawdbNames) == 0 {
			return nil
		}
		ast.Inspect(file, func(node ast.Node) bool {
			fn, ok := node.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				return true
			}
			hotOnlyAncientAliases := make(map[string]struct{})
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.AssignStmt:
					recordHotOnlyAncientAliases(n.Lhs, n.Rhs, rawdbNames, hotOnlyAncientAliases)
					return true
				case *ast.ValueSpec:
					recordHotOnlyAncientValueSpec(n, rawdbNames, hotOnlyAncientAliases)
					return true
				case *ast.CallExpr:
					if len(n.Args) < 2 {
						return true
					}
					if !isRawDBCall(n.Fun, rawdbNames, "NewChainDB") {
						return true
					}
					if !isHotOnlyAncientExpr(n.Args[1], rawdbNames, hotOnlyAncientAliases) {
						return true
					}
					if isAllowedAuditFunction(root, path, fn.Name.Name, allowed) {
						return true
					}
					offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), "rawdb.NewChainDB(..., nil/NoopAncient)"))
				}
				return true
			})
			return false
		})
		return nil
	})
	if err != nil {
		t.Fatalf("audit hot-only ChainDB constructors: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func auditColdArchiveReaderCalls(t *testing.T, root string, watched map[string]struct{}, allowed map[string]map[string]struct{}) []string {
	t.Helper()
	var offenders []string
	fset := token.NewFileSet()
	typeIndex := buildAuditTypeIndex(t, root)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", ".codex", "build", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.HasPrefix(path, filepath.Join(root, "core", "rawdb")+string(os.PathSeparator)) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rawdbNames := rawdbImportNames(file)
		if len(rawdbNames) == 0 {
			return nil
		}
		rel := auditRelPath(root, path)
		packageAliases, packageVarTypes := packageChainDBBoundaries(file, rawdbNames, typeIndex)
		aliases := cloneStringSet(packageAliases)
		packageReaderAliases := packageColdArchiveReaderAliases(file, rawdbNames, watched)
		readerAliases := cloneStringMap(packageReaderAliases)
		varTypes := cloneAuditExprTypeMap(packageVarTypes)
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncDecl:
				aliases = cloneStringSet(packageAliases)
				readerAliases = cloneStringMap(packageReaderAliases)
				varTypes = cloneAuditExprTypeMap(packageVarTypes)
				recordAuditFieldTypes(n.Recv, rawdbNames, varTypes)
				recordAuditFieldTypes(n.Type.Params, rawdbNames, varTypes)
				recordChainDBFieldAliases(n.Type.Params, rawdbNames, aliases)
			case *ast.AssignStmt:
				recordChainDBAliases(n.Lhs, n.Rhs, rawdbNames, aliases, varTypes, typeIndex)
				recordColdArchiveReaderAliases(n.Lhs, n.Rhs, rawdbNames, watched, readerAliases)
			case *ast.ValueSpec:
				recordAuditTypedVars(n.Names, n.Type, rawdbNames, varTypes)
				recordChainDBTypedAliases(n.Names, n.Type, rawdbNames, aliases)
				recordChainDBAliases(exprsFromIdents(n.Names), n.Values, rawdbNames, aliases, varTypes, typeIndex)
				recordColdArchiveReaderAliases(exprsFromIdents(n.Names), n.Values, rawdbNames, watched, readerAliases)
			case *ast.CallExpr:
				if len(n.Args) == 0 {
					return true
				}
				name, ok := coldArchiveReaderCallName(n.Fun, rawdbNames, readerAliases)
				if !ok {
					return true
				}
				if _, watch := watched[name]; !watch {
					return true
				}
				if isAllowedRawDBCall(root, path, name, allowed) {
					return true
				}
				if isColdAwareArchiveReaderArg(rel, name, n.Args[0], rawdbNames, aliases, varTypes, typeIndex) {
					return true
				}
				offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), "rawdb."+name))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("audit cold archive reader calls: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func packageColdArchiveReaderAliases(file *ast.File, rawdbNames map[string]struct{}, watched map[string]struct{}) map[string]string {
	aliases := make(map[string]string)
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			recordColdArchiveReaderAliases(exprsFromIdents(value.Names), value.Values, rawdbNames, watched, aliases)
		}
	}
	return aliases
}

func packageChainDBBoundaries(file *ast.File, rawdbNames map[string]struct{}, typeIndex auditTypeIndex) (map[string]struct{}, map[string]auditExprType) {
	aliases := make(map[string]struct{})
	varTypes := make(map[string]auditExprType)
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			recordAuditTypedVars(value.Names, value.Type, rawdbNames, varTypes)
			recordChainDBTypedAliases(value.Names, value.Type, rawdbNames, aliases)
			recordChainDBAliases(exprsFromIdents(value.Names), value.Values, rawdbNames, aliases, varTypes, typeIndex)
		}
	}
	return aliases, varTypes
}

func auditEventLogMethodCalls(t *testing.T, root string, watched map[string]struct{}) []string {
	t.Helper()
	var offenders []string
	fset := token.NewFileSet()
	typeIndex := buildAuditTypeIndex(t, root)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", ".codex", "build", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.HasPrefix(path, filepath.Join(root, "core", "rawdb")+string(os.PathSeparator)) {
			return nil
		}
		if strings.HasPrefix(path, filepath.Join(root, "core", "state", "snapshots")+string(os.PathSeparator)) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rawdbNames := rawdbImportNames(file)
		packageAliases, packageVarTypes := packageChainDBBoundaries(file, rawdbNames, typeIndex)
		packageMethodAliases := packageEventLogMethodAliases(file, watched, rawdbNames, packageAliases, packageVarTypes, typeIndex)
		aliases := cloneStringSet(packageAliases)
		methodAliases := cloneEventLogMethodAliasMap(packageMethodAliases)
		varTypes := cloneAuditExprTypeMap(packageVarTypes)
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncDecl:
				aliases = cloneStringSet(packageAliases)
				methodAliases = cloneEventLogMethodAliasMap(packageMethodAliases)
				varTypes = cloneAuditExprTypeMap(packageVarTypes)
				recordAuditFieldTypes(n.Recv, rawdbNames, varTypes)
				recordAuditFieldTypes(n.Type.Params, rawdbNames, varTypes)
				recordChainDBFieldAliases(n.Type.Params, rawdbNames, aliases)
			case *ast.AssignStmt:
				recordChainDBAliases(n.Lhs, n.Rhs, rawdbNames, aliases, varTypes, typeIndex)
				recordEventLogMethodAliases(n.Lhs, n.Rhs, watched, rawdbNames, aliases, varTypes, typeIndex, methodAliases)
			case *ast.ValueSpec:
				recordAuditTypedVars(n.Names, n.Type, rawdbNames, varTypes)
				recordChainDBTypedAliases(n.Names, n.Type, rawdbNames, aliases)
				recordChainDBAliases(exprsFromIdents(n.Names), n.Values, rawdbNames, aliases, varTypes, typeIndex)
				recordEventLogMethodAliases(exprsFromIdents(n.Names), n.Values, watched, rawdbNames, aliases, varTypes, typeIndex, methodAliases)
			case *ast.CallExpr:
				switch fun := n.Fun.(type) {
				case *ast.SelectorExpr:
					if _, watch := watched[fun.Sel.Name]; !watch {
						return true
					}
					if isChainDBBoundaryExpr(fun.X, rawdbNames, aliases, varTypes, typeIndex) {
						return true
					}
					offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), fun.Sel.Name))
				case *ast.Ident:
					alias, ok := methodAliases[fun.Name]
					if !ok {
						return true
					}
					if alias.ChainDB {
						return true
					}
					offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), alias.Method))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("audit event-log method calls: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func eventLogCoverageCheckMethods() map[string]struct{} {
	return map[string]struct{}{
		"EventLogRangeCovered":          {},
		"EventLogRangeCoveredForFilter": {},
	}
}

func auditEventLogCoverageCheckCalls(t *testing.T, root string, watched map[string]struct{}, allowed map[string]map[string]struct{}) []string {
	t.Helper()
	var offenders []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", ".codex", "build", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.HasPrefix(path, filepath.Join(root, "core", "rawdb")+string(os.PathSeparator)) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fnName := ""
			if fn, ok := decl.(*ast.FuncDecl); ok {
				fnName = fn.Name.Name
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if _, watch := watched[selector.Sel.Name]; !watch {
					return true
				}
				if isAllowedAuditFunc(root, path, fnName, allowed) {
					return true
				}
				offenders = append(offenders, formatAuditOffender(fset, root, path, selector.Pos(), selector.Sel.Name))
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("audit event-log coverage checks: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func auditEventLogIndexedCoverageCalls(t *testing.T, root string, allowed map[string]map[string]struct{}) []string {
	t.Helper()
	var offenders []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", ".codex", "build", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.HasPrefix(path, filepath.Join(root, "core", "rawdb")+string(os.PathSeparator)) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && isAllowedAuditFunc(root, path, fn.Name.Name, allowed) {
				continue
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "EventLogIndexedRangeCovered" {
					return true
				}
				offenders = append(offenders, formatAuditOffender(fset, root, path, selector.Pos(), selector.Sel.Name))
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("audit indexed event-log coverage calls: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func isAllowedAuditFunc(root, path, name string, allowed map[string]map[string]struct{}) bool {
	if len(allowed) == 0 {
		return false
	}
	funcs, ok := allowed[auditRelPath(root, path)]
	if !ok {
		return false
	}
	_, ok = funcs[name]
	return ok
}

type auditEventLogMethodAlias struct {
	Method  string
	ChainDB bool
}

func packageEventLogMethodAliases(file *ast.File, watched map[string]struct{}, rawdbNames map[string]struct{}, chainAliases map[string]struct{}, varTypes map[string]auditExprType, typeIndex auditTypeIndex) map[string]auditEventLogMethodAlias {
	methodAliases := make(map[string]auditEventLogMethodAlias)
	aliases := cloneStringSet(chainAliases)
	types := cloneAuditExprTypeMap(varTypes)
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			recordAuditTypedVars(value.Names, value.Type, rawdbNames, types)
			recordChainDBTypedAliases(value.Names, value.Type, rawdbNames, aliases)
			recordChainDBAliases(exprsFromIdents(value.Names), value.Values, rawdbNames, aliases, types, typeIndex)
			recordEventLogMethodAliases(exprsFromIdents(value.Names), value.Values, watched, rawdbNames, aliases, types, typeIndex, methodAliases)
		}
	}
	return methodAliases
}

func recordChainDBAliases(lhs, rhs []ast.Expr, rawdbNames map[string]struct{}, aliases map[string]struct{}, varTypes map[string]auditExprType, typeIndex auditTypeIndex) {
	for i, left := range lhs {
		if i >= len(rhs) {
			return
		}
		ident, ok := left.(*ast.Ident)
		if !ok || ident.Name == "_" {
			continue
		}
		if isChainDBBoundaryExpr(rhs[i], rawdbNames, aliases, varTypes, typeIndex) {
			aliases[ident.Name] = struct{}{}
			continue
		}
		delete(aliases, ident.Name)
	}
}

func recordChainDBFieldAliases(fields *ast.FieldList, rawdbNames map[string]struct{}, aliases map[string]struct{}) {
	if fields == nil {
		return
	}
	for _, field := range fields.List {
		if !isRawDBChainDBType(field.Type, rawdbNames) {
			continue
		}
		for _, name := range field.Names {
			if name.Name != "_" {
				aliases[name.Name] = struct{}{}
			}
		}
	}
}

func recordChainDBTypedAliases(names []*ast.Ident, typ ast.Expr, rawdbNames map[string]struct{}, aliases map[string]struct{}) {
	if !isRawDBChainDBType(typ, rawdbNames) {
		return
	}
	for _, name := range names {
		if name.Name != "_" {
			aliases[name.Name] = struct{}{}
		}
	}
}

func recordHotOnlyAncientAliases(lhs, rhs []ast.Expr, rawdbNames map[string]struct{}, aliases map[string]struct{}) {
	for i, left := range lhs {
		if i >= len(rhs) {
			return
		}
		ident, ok := left.(*ast.Ident)
		if !ok || ident.Name == "_" {
			continue
		}
		if isHotOnlyAncientExpr(rhs[i], rawdbNames, aliases) {
			aliases[ident.Name] = struct{}{}
			continue
		}
		delete(aliases, ident.Name)
	}
}

func recordHotOnlyAncientValueSpec(spec *ast.ValueSpec, rawdbNames map[string]struct{}, aliases map[string]struct{}) {
	if len(spec.Values) > 0 {
		recordHotOnlyAncientAliases(exprsFromIdents(spec.Names), spec.Values, rawdbNames, aliases)
		return
	}
	if !isRawDBType(spec.Type, rawdbNames, "AncientReader") && !isRawDBType(spec.Type, rawdbNames, "NoopAncient") {
		return
	}
	for _, name := range spec.Names {
		if name.Name != "_" {
			aliases[name.Name] = struct{}{}
		}
	}
}

func recordColdArchiveReaderAliases(lhs, rhs []ast.Expr, rawdbNames map[string]struct{}, watched map[string]struct{}, aliases map[string]string) {
	for i, left := range lhs {
		if i >= len(rhs) {
			return
		}
		key, ok := coldArchiveReaderAliasKey(left)
		if !ok {
			continue
		}
		if name, ok := coldArchiveReaderAliasName(rhs[i], rawdbNames, aliases); ok {
			if _, watch := watched[name]; watch {
				aliases[key] = name
				continue
			}
		}
		delete(aliases, key)
	}
}

func recordEventLogMethodAliases(lhs, rhs []ast.Expr, watched map[string]struct{}, rawdbNames map[string]struct{}, chainAliases map[string]struct{}, varTypes map[string]auditExprType, typeIndex auditTypeIndex, methodAliases map[string]auditEventLogMethodAlias) {
	for i, left := range lhs {
		if i >= len(rhs) {
			return
		}
		ident, ok := left.(*ast.Ident)
		if !ok || ident.Name == "_" {
			continue
		}
		if alias, ok := eventLogMethodAlias(rhs[i], watched, rawdbNames, chainAliases, varTypes, typeIndex, methodAliases); ok {
			methodAliases[ident.Name] = alias
			continue
		}
		delete(methodAliases, ident.Name)
	}
}

func eventLogMethodAlias(expr ast.Expr, watched map[string]struct{}, rawdbNames map[string]struct{}, chainAliases map[string]struct{}, varTypes map[string]auditExprType, typeIndex auditTypeIndex, methodAliases map[string]auditEventLogMethodAlias) (auditEventLogMethodAlias, bool) {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	switch v := expr.(type) {
	case *ast.SelectorExpr:
		if _, watch := watched[v.Sel.Name]; !watch {
			return auditEventLogMethodAlias{}, false
		}
		return auditEventLogMethodAlias{
			Method:  v.Sel.Name,
			ChainDB: isChainDBBoundaryExpr(v.X, rawdbNames, chainAliases, varTypes, typeIndex),
		}, true
	case *ast.Ident:
		alias, ok := methodAliases[v.Name]
		return alias, ok
	default:
		return auditEventLogMethodAlias{}, false
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneStringSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func cloneAuditExprTypeMap(in map[string]auditExprType) map[string]auditExprType {
	out := make(map[string]auditExprType, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneEventLogMethodAliasMap(in map[string]auditEventLogMethodAlias) map[string]auditEventLogMethodAlias {
	out := make(map[string]auditEventLogMethodAlias, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func exprsFromIdents(idents []*ast.Ident) []ast.Expr {
	exprs := make([]ast.Expr, 0, len(idents))
	for _, ident := range idents {
		exprs = append(exprs, ident)
	}
	return exprs
}

type auditExprType struct {
	ChainDB bool
	Named   string
}

type auditTypeIndex map[string]map[string]auditExprType

func buildAuditTypeIndex(t *testing.T, root string) auditTypeIndex {
	t.Helper()
	index := make(auditTypeIndex)
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", ".codex", "build", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rawdbNames := rawdbImportNames(file)
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				st, ok := typeSpec.Type.(*ast.StructType)
				if !ok || st.Fields == nil {
					continue
				}
				fields := make(map[string]auditExprType)
				for _, field := range st.Fields.List {
					typ := auditExprTypeFromExpr(field.Type, rawdbNames)
					if typ == (auditExprType{}) {
						continue
					}
					for _, name := range field.Names {
						fields[name.Name] = typ
					}
				}
				if len(fields) > 0 {
					index[typeSpec.Name.Name] = fields
				}
			}
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil {
				continue
			}
			recvName := auditReceiverTypeName(fn.Recv)
			resultType, ok := auditSingleResultType(fn.Type.Results)
			if recvName == "" || !ok {
				continue
			}
			typ := auditExprTypeFromExpr(resultType, rawdbNames)
			if typ == (auditExprType{}) {
				continue
			}
			methods := index[recvName]
			if methods == nil {
				methods = make(map[string]auditExprType)
				index[recvName] = methods
			}
			methods[auditMethodTypeKey(fn.Name.Name)] = typ
		}
		return nil
	})
	if err != nil {
		t.Fatalf("build audit type index: %v", err)
	}
	return index
}

func auditMethodTypeKey(name string) string {
	return name + "()"
}

func auditReceiverTypeName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) != 1 {
		return ""
	}
	return auditNamedTypeName(recv.List[0].Type)
}

func auditNamedTypeName(expr ast.Expr) string {
	for {
		switch typed := expr.(type) {
		case *ast.ParenExpr:
			expr = typed.X
		case *ast.StarExpr:
			expr = typed.X
		default:
			goto done
		}
	}
done:
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return typed.Sel.Name
	default:
		return ""
	}
}

func auditSingleResultType(results *ast.FieldList) (ast.Expr, bool) {
	if results == nil || len(results.List) != 1 {
		return nil, false
	}
	result := results.List[0]
	if len(result.Names) > 1 {
		return nil, false
	}
	return result.Type, true
}

func auditExprTypeFromExpr(expr ast.Expr, rawdbNames map[string]struct{}) auditExprType {
	for {
		switch typed := expr.(type) {
		case *ast.ParenExpr:
			expr = typed.X
		case *ast.StarExpr:
			expr = typed.X
		default:
			goto done
		}
	}
done:
	if isRawDBType(expr, rawdbNames, "ChainDB") {
		return auditExprType{ChainDB: true}
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return auditExprType{Named: ident.Name}
	}
	if sel, ok := expr.(*ast.SelectorExpr); ok {
		return auditExprType{Named: sel.Sel.Name}
	}
	return auditExprType{}
}

func recordAuditFieldTypes(fields *ast.FieldList, rawdbNames map[string]struct{}, varTypes map[string]auditExprType) {
	if fields == nil {
		return
	}
	for _, field := range fields.List {
		typ := auditExprTypeFromExpr(field.Type, rawdbNames)
		if typ == (auditExprType{}) {
			continue
		}
		for _, name := range field.Names {
			if name.Name != "_" {
				varTypes[name.Name] = typ
			}
		}
	}
}

func recordAuditTypedVars(names []*ast.Ident, typ ast.Expr, rawdbNames map[string]struct{}, varTypes map[string]auditExprType) {
	resolved := auditExprTypeFromExpr(typ, rawdbNames)
	if resolved == (auditExprType{}) {
		return
	}
	for _, name := range names {
		if name.Name != "_" {
			varTypes[name.Name] = resolved
		}
	}
}

func isAllowedRawDBCall(root, path, function string, allowed map[string]map[string]struct{}) bool {
	if len(allowed) == 0 {
		return false
	}
	rel := auditRelPath(root, path)
	functions := allowed[rel]
	if len(functions) == 0 {
		return false
	}
	_, ok := functions[function]
	return ok
}

func isAllowedAuditPath(root, path string, allowed map[string]struct{}) bool {
	if len(allowed) == 0 {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = path
	}
	rel = filepath.ToSlash(rel)
	_, ok := allowed[rel]
	return ok
}

func isAllowedAuditFunction(root, path, function string, allowed map[string]map[string]struct{}) bool {
	if len(allowed) == 0 {
		return false
	}
	rel := auditRelPath(root, path)
	functions := allowed[rel]
	if len(functions) == 0 {
		return false
	}
	_, ok := functions[function]
	return ok
}

func rawDBCallName(expr ast.Expr, rawdbNames map[string]struct{}) (string, bool) {
	switch fun := expr.(type) {
	case *ast.SelectorExpr:
		ident, ok := fun.X.(*ast.Ident)
		if !ok {
			return "", false
		}
		if _, imported := rawdbNames[ident.Name]; !imported {
			return "", false
		}
		return fun.Sel.Name, true
	case *ast.Ident:
		if _, dotImported := rawdbNames["."]; !dotImported {
			return "", false
		}
		return fun.Name, true
	default:
		return "", false
	}
}

func isRawDBCall(expr ast.Expr, rawdbNames map[string]struct{}, name string) bool {
	if call, ok := expr.(*ast.CallExpr); ok {
		expr = call.Fun
	}
	got, ok := rawDBCallName(expr, rawdbNames)
	return ok && got == name
}

func coldArchiveReaderCallName(expr ast.Expr, rawdbNames map[string]struct{}, aliases map[string]string) (string, bool) {
	return coldArchiveReaderAliasName(expr, rawdbNames, aliases)
}

func coldArchiveReaderAliasName(expr ast.Expr, rawdbNames map[string]struct{}, aliases map[string]string) (string, bool) {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	if key, ok := coldArchiveReaderAliasKey(expr); ok {
		if name, ok := aliases[key]; ok {
			return name, true
		}
	}
	if name, ok := rawDBCallName(expr, rawdbNames); ok {
		return name, true
	}
	return "", false
}

func coldArchiveReaderAliasKey(expr ast.Expr) (string, bool) {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	switch v := expr.(type) {
	case *ast.Ident:
		if v.Name == "_" {
			return "", false
		}
		return v.Name, true
	case *ast.SelectorExpr:
		path := selectorPath(v)
		if len(path) == 0 {
			return "", false
		}
		return strings.Join(path, "."), true
	default:
		return "", false
	}
}

func isRawDBChainDBType(expr ast.Expr, rawdbNames map[string]struct{}) bool {
	for {
		switch typed := expr.(type) {
		case *ast.ParenExpr:
			expr = typed.X
		case *ast.StarExpr:
			expr = typed.X
		default:
			goto done
		}
	}
done:
	if sel, ok := expr.(*ast.SelectorExpr); ok && sel.Sel.Name == "ChainDB" {
		if ident, ok := sel.X.(*ast.Ident); ok {
			_, imported := rawdbNames[ident.Name]
			return imported
		}
	}
	if ident, ok := expr.(*ast.Ident); ok && ident.Name == "ChainDB" {
		_, dotImported := rawdbNames["."]
		return dotImported
	}
	return false
}

func isChainDBBoundaryExpr(expr ast.Expr, rawdbNames map[string]struct{}, aliases map[string]struct{}, varTypes map[string]auditExprType, typeIndex auditTypeIndex) bool {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	if ident, ok := expr.(*ast.Ident); ok {
		if _, exists := aliases[ident.Name]; exists {
			return true
		}
		if typ, ok := varTypes[ident.Name]; ok && typ.ChainDB {
			return true
		}
		return false
	}
	if isRawDBCall(expr, rawdbNames, "NewChainDB") {
		return true
	}
	if typ, ok := resolveAuditCallReturnType(expr, varTypes, typeIndex); ok && typ.ChainDB {
		return true
	}
	if typ, ok := resolveAuditSelectorType(expr, varTypes, typeIndex); ok && typ.ChainDB {
		return true
	}
	return false
}

func isColdAwareArchiveReaderArg(rel, function string, expr ast.Expr, rawdbNames map[string]struct{}, aliases map[string]struct{}, varTypes map[string]auditExprType, typeIndex auditTypeIndex) bool {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	if isChainDBBoundaryExpr(expr, rawdbNames, aliases, varTypes, typeIndex) {
		return true
	}
	path := selectorPath(expr)
	if len(path) == 0 {
		return false
	}
	if rel == "core/tron_backend.go" && function == "ReadSectionBloomBitSetStrict" && strings.Join(path, ".") == "m.db" {
		return true
	}
	return false
}

func resolveAuditSelectorType(expr ast.Expr, varTypes map[string]auditExprType, typeIndex auditTypeIndex) (auditExprType, bool) {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	switch v := expr.(type) {
	case *ast.Ident:
		typ, ok := varTypes[v.Name]
		return typ, ok
	case *ast.SelectorExpr:
		base, ok := resolveAuditSelectorType(v.X, varTypes, typeIndex)
		if !ok || base.Named == "" {
			return auditExprType{}, false
		}
		fields := typeIndex[base.Named]
		if len(fields) == 0 {
			return auditExprType{}, false
		}
		typ, ok := fields[v.Sel.Name]
		return typ, ok
	default:
		return auditExprType{}, false
	}
}

func resolveAuditCallReturnType(expr ast.Expr, varTypes map[string]auditExprType, typeIndex auditTypeIndex) (auditExprType, bool) {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return auditExprType{}, false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return auditExprType{}, false
	}
	base, ok := resolveAuditSelectorType(sel.X, varTypes, typeIndex)
	if !ok || base.Named == "" {
		return auditExprType{}, false
	}
	methods := typeIndex[base.Named]
	if len(methods) == 0 {
		return auditExprType{}, false
	}
	typ, ok := methods[auditMethodTypeKey(sel.Sel.Name)]
	return typ, ok
}

func selectorPath(expr ast.Expr) []string {
	switch v := expr.(type) {
	case *ast.Ident:
		return []string{v.Name}
	case *ast.SelectorExpr:
		base := selectorPath(v.X)
		if len(base) == 0 {
			return nil
		}
		return append(base, v.Sel.Name)
	default:
		return nil
	}
}

func isHotOnlyAncientAlias(expr ast.Expr, aliases map[string]struct{}) bool {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	_, exists := aliases[ident.Name]
	return exists
}

func isHotOnlyAncientExpr(expr ast.Expr, rawdbNames map[string]struct{}, aliases map[string]struct{}) bool {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	if ident, ok := expr.(*ast.Ident); ok && ident.Name == "nil" {
		return true
	}
	if isHotOnlyAncientAlias(expr, aliases) {
		return true
	}
	if call, ok := expr.(*ast.CallExpr); ok {
		if len(call.Args) == 1 && isRawDBType(call.Fun, rawdbNames, "AncientReader") {
			return isHotOnlyAncientExpr(call.Args[0], rawdbNames, aliases)
		}
		if isRawDBCall(call.Fun, rawdbNames, "NewFallbackAncientReader") {
			if len(call.Args) == 0 {
				return true
			}
			for _, arg := range call.Args {
				if !isHotOnlyAncientExpr(arg, rawdbNames, aliases) {
					return false
				}
			}
			return true
		}
	}
	return isNoopAncientExpr(expr, rawdbNames)
}

func isNoopAncientExpr(expr ast.Expr, rawdbNames map[string]struct{}) bool {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	if call, ok := expr.(*ast.CallExpr); ok && len(call.Args) == 1 && isRawDBType(call.Fun, rawdbNames, "AncientReader") {
		return isNoopAncientExpr(call.Args[0], rawdbNames)
	}
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return false
	}
	return isRawDBType(lit.Type, rawdbNames, "NoopAncient")
}

func isRawDBType(expr ast.Expr, rawdbNames map[string]struct{}, name string) bool {
	switch typ := expr.(type) {
	case *ast.SelectorExpr:
		ident, ok := typ.X.(*ast.Ident)
		if !ok || typ.Sel.Name != name {
			return false
		}
		_, imported := rawdbNames[ident.Name]
		return imported
	case *ast.Ident:
		if typ.Name != name {
			return false
		}
		_, dotImported := rawdbNames["."]
		return dotImported
	default:
		return false
	}
}

func rawdbImportNames(file *ast.File) map[string]struct{} {
	names := make(map[string]struct{})
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil || path != "github.com/tronprotocol/go-tron/core/rawdb" {
			continue
		}
		name := "rawdb"
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if name == "_" {
			continue
		}
		names[name] = struct{}{}
	}
	return names
}

func formatAuditOffender(fset *token.FileSet, root, path string, pos token.Pos, call string) string {
	position := fset.Position(pos)
	rel := auditRelPath(root, path)
	return rel + ":" + strconv.Itoa(position.Line) + ": " + call
}

func auditRelPath(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = path
	}
	return filepath.ToSlash(rel)
}
