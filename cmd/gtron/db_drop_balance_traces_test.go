package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/urfave/cli/v2"
)

func TestDBDropBalanceTracesCommand(t *testing.T) {
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 7, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{Number: 7},
	}); err != nil {
		t.Fatal(err)
	}
	owner := []byte{1, 2, 3}
	if err := rawdb.WriteAccountTrace(db, owner, 7, 99); err != nil {
		t.Fatal(err)
	}
	txHash := make([]byte, 32)
	txHash[0] = 0xaa
	if err := rawdb.WriteTransactionIndex(db, txHash, 7); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{
		"gtron", "db", "drop-balance-traces",
		"--datadir", datadir,
		"--db.cache", "16",
		"--db.handles", "16",
		"--yes",
		"--compact",
		"--progress", "0s",
		"--json",
	}); err != nil {
		t.Fatalf("drop balance traces: %v\nstderr: %s", err, stderr.String())
	}
	var report dropBalanceTracesOutput
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v\noutput: %s", err, stdout.String())
	}
	if !report.DroppedBlockTraces || !report.DroppedAccountTraces || !report.Compacted {
		t.Fatalf("unexpected report: %+v", report)
	}

	reopened, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if trace := rawdb.ReadBlockBalanceTrace(reopened, 7); trace != nil {
		t.Fatalf("block trace survived command: %+v", trace)
	}
	if _, ok := rawdb.ReadAccountTrace(reopened, owner, 7); ok {
		t.Fatal("account trace survived command")
	}
	if blockNum := rawdb.ReadTransactionIndex(rawdb.NewChainDB(reopened, rawdb.NoopAncient{}), txHash); blockNum == nil || *blockNum != 7 {
		t.Fatalf("transaction index changed by command: %v", blockNum)
	}
}

func TestDBDropBalanceTracesRequiresConfirmation(t *testing.T) {
	ctx := makeDBFlagSet(t, nil)
	if err := dbDropBalanceTracesCmd(ctx); err == nil {
		t.Fatal("drop balance traces accepted missing --yes")
	}
}
