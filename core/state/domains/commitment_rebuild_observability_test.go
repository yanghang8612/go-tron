package domains

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestCommitmentRebuildLogsLifecycleAndSourceProgress(t *testing.T) {
	var buf bytes.Buffer
	previous := gtronlog.Root()
	defer gtronlog.SetDefault(previous)
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)))

	db := rawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("account")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVGeneration(db, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if _, err := newStagedCommitmentStore(db).Rebuild(); err != nil {
		t.Fatal(err)
	}
	if CommitmentRebuildActive() {
		t.Fatal("rebuild remained active after completion")
	}

	out := buf.String()
	for _, field := range []string{
		`msg="Commitment branch rebuild started"`,
		"reason=bootstrap", "mode=buffered", "phase=clear",
		`msg="Commitment branch rebuild source started"`,
		"source=account-latest", "source=kv-generation", "source=kv-latest",
		`msg="Commitment branch rebuild completed"`,
		"rowsScanned=3", "bytesScanned=", "batchesFolded=1", "root=",
	} {
		if !strings.Contains(out, field) {
			t.Errorf("missing rebuild log field %q:\n%s", field, out)
		}
	}
}

func TestCommitmentRebuildPeriodicProgressReportsStallAge(t *testing.T) {
	var buf bytes.Buffer
	previous := gtronlog.Root()
	defer gtronlog.SetDefault(previous)
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)))

	progress := startCommitmentRebuildProgress(commitmentRebuildContext{reason: "test"}, "durable", 3, 32, time.Hour)
	if !CommitmentRebuildActive() {
		t.Fatal("rebuild was not marked active")
	}
	finished := false
	defer func() {
		if !finished {
			progress.finish(common.Hash{}, nil)
		}
	}()
	progress.started = time.Now().Add(-2 * time.Minute)
	progress.setSource(rawdb.LatestDomainCommitmentSourceAccounts, 0, 0)
	progress.observeScan(2048, 2048, 4096)
	progress.phase.Store("fold")
	progress.lastProgressNano.Store(time.Now().Add(-45 * time.Second).UnixNano())
	progress.logProgress()
	progress.finish(common.Hash{0xaa}, nil)
	finished = true

	out := buf.String()
	for _, field := range []string{
		`msg="Commitment branch rebuild progress"`,
		"reason=test", "mode=durable", "phase=fold", "source=account-latest",
		"rowsScanned=2048", "sourceRowsScanned=2048", "bytesScanned=4096",
		"rowsPerSecond=", "bytesPerSecond=", "elapsed=", "sinceLastProgress=",
	} {
		if !strings.Contains(out, field) {
			t.Errorf("missing periodic progress field %q:\n%s", field, out)
		}
	}
}
