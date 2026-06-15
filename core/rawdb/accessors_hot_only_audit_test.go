package rawdb

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
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
		"ReadBlockRaw":            {},
		"ReadTransactionInfosRaw": {},
		"ReadBlockStateRootRaw":   {},
	}, map[string]map[string]struct{}{
		"cmd/gtron/freezer_adapter.go": {
			"ReadBlockRaw":            {},
			"ReadTransactionInfosRaw": {},
			"ReadBlockStateRootRaw":   {},
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
	offenders := auditForbiddenRawDBCalls(t, actuatorRoot, map[string]struct{}{
		"ReadBlockHashByNumber": {},
	}, map[string]map[string]struct{}{
		"actuator.go": {
			"ReadBlockHashByNumber": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("actuator historical compatibility checks must use Context.EffectiveGenesisHash instead of direct hot block hash reads:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionBlockHashByNumberReadsStayOnAuditedBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBCalls(t, root, map[string]struct{}{
		"ReadBlockHashByNumber": {},
	}, map[string]map[string]struct{}{
		"actuator/actuator.go": {
			"ReadBlockHashByNumber": {},
		},
		"cmd/gtron/db_cmd.go": {
			"ReadBlockHashByNumber": {},
		},
		"cmd/gtron/freezer_adapter.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/blockbuffer/buffer.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/state/pruning/pruner.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/state/snapshots/cold_builder.go": {
			"ReadBlockHashByNumber": {},
		},
		"vm/instructions.go": {
			"ReadBlockHashByNumber": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production block-hash-by-number reads must stay behind audited freezer/cold-index boundaries:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionHotOnlyChainDBConstructorsStayOnAuditedBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditHotOnlyChainDBConstructors(t, root, map[string]struct{}{
		"cmd/gtron/db_cmd.go":            {},
		"core/balance_trace_backfill.go": {},
		"core/blockchain.go":             {},
		"core/genesis.go":                {},
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
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.NewChainDB(..., rawdb.NoopAncient{})") {
		t.Fatalf("offenders = %+v, want converted NoopAncient constructor", offenders)
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
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.NewChainDB(..., rawdb.NoopAncient{})") {
		t.Fatalf("offenders = %+v, want aliased NoopAncient constructor", offenders)
	}
}

func TestHotOnlyChainDBAuditClearsNoopAncientAliasAfterRewrap(t *testing.T) {
	root := writeAuditFixture(t, "app/rewrapped.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func build() {
	var ancient rawdb.AncientReader = rawdb.NoopAncient{}
	ancient = rawdb.NewFallbackAncientReader(ancient, nil)
	_ = rawdb.NewChainDB(nil, ancient)
}
`)

	if offenders := auditHotOnlyChainDBConstructors(t, root, nil); len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want rewrapped ancient reader accepted", offenders)
	}
}

func TestProductionColdArchiveReadersUseChainDBBoundary(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadAccountTrace":                  {},
		"ReadAccountTraceAtOrBefore":        {},
		"ReadBlock":                         {},
		"ReadBlockBalanceTrace":             {},
		"ReadBlockHashByNumber":             {},
		"ReadBlockNumber":                   {},
		"ReadBlockStateRoot":                {},
		"ReadSectionBloom":                  {},
		"ReadSectionBloomBitSet":            {},
		"ReadTransactionIndex":              {},
		"ReadTransactionInfo":               {},
		"ReadTransactionInfosByBlock":       {},
		"ReadTransactionInfosByBlockStrict": {},
	}, map[string]map[string]struct{}{
		"actuator/actuator.go": {
			"ReadBlockHashByNumber": {},
		},
		"cmd/balance-trace/main.go": {
			"ReadAccountTrace":      {},
			"ReadBlockBalanceTrace": {},
		},
		"cmd/gtron/db_cmd.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/balance_trace_backfill.go": {
			"ReadAccountTrace":      {},
			"ReadBlockBalanceTrace": {},
		},
		"core/blockbuffer/buffer.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/state/pruning/pruner.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/state/snapshots/cold_builder.go": {
			"ReadBlockHashByNumber": {},
		},
		"vm/instructions.go": {
			"ReadBlockHashByNumber": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production archive readers must use the freezer/cold-sidecar-aware ChainDB boundary:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionStateHistoryAsOfReadsStayBehindHistoryBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBReferences(t, root, stateHistoryAsOfRawDBReferences(), map[string]map[string]struct{}{
		"core/state/snapshots/domain_registry.go": {
			"ReadStateAccountLatestAsOfTxNum":      {},
			"ReadStateKVAsOfTxNum":                 {},
			"ReadStateKVGenerationAsOfTxNum":       {},
			"ReadStateAccountKVAsOfTxNum":          {},
			"IterateStateAccountKVAsOfPrefixTxNum": {},
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

func TestColdArchiveAuditRejectsStrictTransactionInfoReadOnHotStore(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(db any) {
	_, _, _ = rawdb.ReadTransactionInfosByBlockStrict(db, 7)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadTransactionInfosByBlockStrict": {},
	}, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.ReadTransactionInfosByBlockStrict") {
		t.Fatalf("offenders = %+v, want hot-store strict transaction info read rejected", offenders)
	}
}

func TestColdArchiveAuditAllowsStrictTransactionInfoReadOnChainDBBoundary(t *testing.T) {
	root := writeAuditFixture(t, "app/chain.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func query(db *rawdb.ChainDB) {
	chainDB := db
	_, _, _ = rawdb.ReadTransactionInfosByBlockStrict(chainDB, 7)
}
`)

	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
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
	})
	if len(offenders) > 0 {
		t.Fatalf("production event-log queries must go through the cold-sidecar-aware ChainDB boundary:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionEventLogIndexedCoverageChecksStayOnAuditedBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditEventLogIndexedCoverageCalls(t, root, map[string]struct{}{
		"cmd/gtron/db_cmd.go":                      {},
		"core/state/snapshots/chain_tail_prune.go": {},
	})
	if len(offenders) > 0 {
		t.Fatalf("production indexed event-log coverage checks must stay on snapshot prune or db diagnostics boundaries:\n%s", strings.Join(offenders, "\n"))
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
	offenders := auditForbiddenRawDBCalls(t, snapshotRoot, map[string]struct{}{
		"ReadTransactionInfosByBlock": {},
	}, nil)
	if len(offenders) > 0 {
		t.Fatalf("snapshot builders must use ReadTransactionInfosByBlockStrict so corrupt TransactionRet rows fail cold coverage publishing:\n%s", strings.Join(offenders, "\n"))
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
		"ReadStateAccountLatest": {},
		"ReadStateCode":          {},
		"ReadStateKVGeneration":  {},
		"ReadStateKVLatest":      {},
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

func auditHotOnlyChainDBConstructors(t *testing.T, root string, allowed map[string]struct{}) []string {
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
			noopAncientAliases := make(map[string]struct{})
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.AssignStmt:
					recordNoopAncientAliases(n.Lhs, n.Rhs, rawdbNames, noopAncientAliases)
					return true
				case *ast.ValueSpec:
					recordNoopAncientAliases(exprsFromIdents(n.Names), n.Values, rawdbNames, noopAncientAliases)
					return true
				case *ast.CallExpr:
					if len(n.Args) < 2 {
						return true
					}
					if !isRawDBCall(n.Fun, rawdbNames, "NewChainDB") {
						return true
					}
					if !isNoopAncientExpr(n.Args[1], rawdbNames) && !isNoopAncientAlias(n.Args[1], noopAncientAliases) {
						return true
					}
					if isAllowedAuditPath(root, path, allowed) {
						return true
					}
					offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), "rawdb.NewChainDB(..., rawdb.NoopAncient{})"))
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

func auditEventLogIndexedCoverageCalls(t *testing.T, root string, allowed map[string]struct{}) []string {
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
		if isAllowedAuditPath(root, path, allowed) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "EventLogIndexedRangeCovered" {
				return true
			}
			offenders = append(offenders, formatAuditOffender(fset, root, path, selector.Pos(), selector.Sel.Name))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("audit indexed event-log coverage calls: %v", err)
	}
	sort.Strings(offenders)
	return offenders
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

func recordNoopAncientAliases(lhs, rhs []ast.Expr, rawdbNames map[string]struct{}, aliases map[string]struct{}) {
	for i, left := range lhs {
		if i >= len(rhs) {
			return
		}
		ident, ok := left.(*ast.Ident)
		if !ok || ident.Name == "_" {
			continue
		}
		if isNoopAncientExpr(rhs[i], rawdbNames) {
			aliases[ident.Name] = struct{}{}
			continue
		}
		delete(aliases, ident.Name)
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
		return nil
	})
	if err != nil {
		t.Fatalf("build audit type index: %v", err)
	}
	return index
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
	if call, ok := expr.(*ast.CallExpr); ok {
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "ChainDB" {
			return true
		}
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
	if rel == "core/tron_backend.go" && function == "ReadSectionBloomBitSet" && strings.Join(path, ".") == "m.db" {
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

func isNoopAncientAlias(expr ast.Expr, aliases map[string]struct{}) bool {
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
