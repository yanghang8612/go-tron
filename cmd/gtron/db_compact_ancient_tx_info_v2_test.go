package main

import (
	"bytes"
	"encoding/json"
	"testing"

	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/urfave/cli/v2"
	"google.golang.org/protobuf/proto"
)

func TestDBCompactAncientTxInfoV2UpgradesIndexesAndPreservesReads(t *testing.T) {
	datadir := t.TempDir()
	ancientPath := ancientDataDir(datadir)
	tables := chainfreezer.FreezerTableSet()
	ancient, err := rawdbfreezer.NewFreezer(ancientPath, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	chaindata, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	wantInfos := make([]*corepb.TransactionInfo, 4)
	if _, err := ancient.ModifyAncients(func(op rawdbfreezer.AncientWriteOp) error {
		for number := uint64(0); number < 4; number++ {
			transactions := []*corepb.Transaction{{RawData: &corepb.TransactionRaw{Timestamp: int64(number + 1)}}}
			if number == 0 {
				// Mainnet genesis contains three synthetic body transactions but
				// no execution infos. Preserve this historical 3/0 exception.
				transactions = []*corepb.Transaction{
					{RawData: &corepb.TransactionRaw{Timestamp: 1}},
					{RawData: &corepb.TransactionRaw{Timestamp: 2}},
					{RawData: &corepb.TransactionRaw{Timestamp: 3}},
				}
			}
			pb := &corepb.Block{
				BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number * 3000)}},
				Transactions: transactions,
			}
			block := types.NewBlockFromPB(pb)
			body, err := block.Marshal()
			if err != nil {
				return err
			}
			var infos []*corepb.TransactionInfo
			if number != 0 {
				hash := block.Transactions()[0].Hash()
				info := &corepb.TransactionInfo{Id: append([]byte(nil), hash[:]...), Fee: int64(100 + number), BlockNumber: int64(number)}
				wantInfos[number] = info
				infos = []*corepb.TransactionInfo{info}
				if err := rawdb.WriteTransactionIndex(chaindata, hash[:], number); err != nil {
					return err
				}
			}
			ret, err := proto.Marshal(&corepb.TransactionRet{BlockNumber: int64(number), Transactioninfo: infos})
			if err != nil {
				return err
			}
			if err := op.AppendRaw("bodies", number, body); err != nil {
				return err
			}
			if err := op.AppendRaw("tx_infos", number, ret); err != nil {
				return err
			}
			if err := op.AppendRaw("state_roots", number, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := ancient.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := ancient.Close(); err != nil {
		t.Fatal(err)
	}
	if err := chaindata.Close(); err != nil {
		t.Fatal(err)
	}

	app := &cli.App{Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{
		"gtron", "db", "migrate-ancient-v2", "--datadir", datadir, "--yes",
		"--frame-blocks", "2", "--segment-blocks", "4",
	}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	app = &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{
		"gtron", "db", "compact-ancient-tx-info-v2", "--datadir", datadir,
		"--yes", "--json", "--db.cache", "16", "--db.handles", "16",
	}); err != nil {
		t.Fatalf("compact: %v\nstderr: %s", err, stderr.String())
	}
	var output ancientTxInfoV2CompactOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("decode output: %v\n%s", err, stdout.String())
	}
	if output.Segments != 1 || output.Rows != 4 || output.IndexedTransactions != 3 || output.DuplicateBytes == 0 {
		t.Fatalf("unexpected output: %+v", output)
	}

	ancient, err = rawdbfreezer.NewFreezer(ancientPath, "", true, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	defer ancient.Close()
	db, err := rawdb.NewPebbleDBReadOnly(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	chainDB := rawdb.NewChainDB(db, rawdb.NewFreezerReader(ancient))
	for number, want := range wantInfos {
		rawRet, err := ancient.Ancient("tx_infos", uint64(number))
		if err != nil {
			t.Fatal(err)
		}
		stored := &corepb.TransactionRet{}
		if err := proto.Unmarshal(rawRet, stored); err != nil {
			t.Fatal(err)
		}
		if want == nil {
			if len(stored.Transactioninfo) != 0 {
				t.Fatalf("genesis transaction infos changed: %v", stored.Transactioninfo)
			}
			continue
		}
		if len(stored.Transactioninfo) != 1 || len(stored.Transactioninfo[0].Id) != 0 {
			t.Fatalf("stored tx_infos[%d] still contains ID", number)
		}
		got := rawdb.ReadTransactionInfo(chainDB, want.Id)
		if !proto.Equal(got, want) {
			t.Fatalf("transaction info %d changed:\n got %v\nwant %v", number, got, want)
		}
		all := rawdb.ReadTransactionInfosByBlock(chainDB, uint64(number))
		if len(all) != 1 || !proto.Equal(all[0], want) {
			t.Fatalf("block transaction info %d changed: %v", number, all)
		}
	}
}

func TestDBCompactAncientTxInfoV2RequiresYes(t *testing.T) {
	app := &cli.App{Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{"gtron", "db", "compact-ancient-tx-info-v2"}); err == nil {
		t.Fatal("rewrite without --yes succeeded")
	}
}
