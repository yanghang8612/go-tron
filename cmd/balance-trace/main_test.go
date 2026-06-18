package main

import (
	"bytes"
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

type balanceTraceReaderStub struct {
	owner   []byte
	block   int64
	balance int64
	trace   *contractpb.BlockBalanceTrace
}

func (r *balanceTraceReaderStub) BlockBalanceTrace(blockNum int64) (*contractpb.BlockBalanceTrace, bool, error) {
	if r.trace == nil || blockNum != r.block {
		return nil, false, nil
	}
	return r.trace, true, nil
}

func (r *balanceTraceReaderStub) AccountTraceAtOrBefore(owner []byte, blockNum int64) (int64, int64, bool, error) {
	if blockNum < r.block || !bytes.Equal(owner, r.owner) {
		return 0, 0, false, nil
	}
	return r.block, r.balance, true, nil
}

func TestTxPrefixReadsColdBalanceTraceFromChainDB(t *testing.T) {
	chainDB := rawdb.NewMemoryChainDB()
	target := testAddress(0x11)
	tx := testTransferTx(t, target, testAddress(0x22))
	txHash := tx.Hash()
	reader := &balanceTraceReaderStub{
		owner:   append([]byte(nil), target.Bytes()...),
		block:   7,
		balance: 1234,
		trace: &contractpb.BlockBalanceTrace{
			BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{Number: 7},
			TransactionBalanceTrace: []*contractpb.TransactionBalanceTrace{{
				TransactionIdentifier: txHash[:],
				Operation: []*contractpb.TransactionBalanceTrace_Operation{{
					OperationIdentifier: 1,
					Address:             target.Bytes(),
					Amount:              -55,
				}},
			}},
		},
	}
	chainDB.SetBalanceTraceReader(reader)

	got := txPrefix(7, 0, tx, target, chainDB)
	if !strings.Contains(got, "accountTraceBalance=1234") {
		t.Fatalf("txPrefix = %q, want cold account trace balance", got)
	}
	if !strings.Contains(got, "balanceTraceOps=1:-55") {
		t.Fatalf("txPrefix = %q, want cold block balance trace ops", got)
	}
}

func testAddress(fill byte) tcommon.Address {
	raw := make([]byte, tcommon.AddressLength)
	raw[0] = tcommon.AddressPrefixMainnet
	for i := 1; i < len(raw); i++ {
		raw[i] = fill
	}
	return tcommon.BytesToAddress(raw)
}

func testTransferTx(t *testing.T, owner, to tcommon.Address) *types.Transaction {
	t.Helper()
	param, err := anypb.New(&contractpb.TransferContract{
		OwnerAddress: owner.Bytes(),
		ToAddress:    to.Bytes(),
		Amount:       100,
	})
	if err != nil {
		t.Fatalf("pack transfer: %v", err)
	}
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
			Timestamp:  1,
			Expiration: 2,
		},
	})
}
