package jsonrpc_test

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// stubBackend is the shared jsonrpc.Backend test double. It is seeded by
// newFreezeBackend (freeze_fixtures_test.go) for the corpus/parity tests,
// embedded by wsTestBackend (subscription_test.go) for the WS tests, and
// constructed inline by the archive-routing tests (archive_test.go).
type stubBackend struct {
	chainID     int64
	blockNumber uint64
	block       *types.Block
	balance     int64
	code        []byte
	storage     common.Hash
	tx          *corepb.Transaction
	txBlock     *types.Block
	txIndex     int
	txInfo      *corepb.TransactionInfo
	callResult  []byte
	logs        []*jsonrpc.RPCLog
	gasPrice    int64
	peerCount   int

	// Archive-query stubs: when atErr is non-nil the *At methods return it
	// (used to exercise the history-disabled gate at the handler layer).
	// Otherwise they return the at* values, letting a test assert the
	// handler routed to the archive path rather than the live one.
	atErr     error
	balanceAt int64
	codeAt    []byte
	storageAt common.Hash
}

func (s *stubBackend) ChainID() int64                       { return s.chainID }
func (s *stubBackend) BlockNumber() uint64                  { return s.blockNumber }
func (s *stubBackend) GetBalance(addr common.Address) int64 { return s.balance }
func (s *stubBackend) GetCode(addr common.Address) []byte   { return s.code }
func (s *stubBackend) GetStorageAt(addr common.Address, slot common.Hash) common.Hash {
	return s.storage
}
func (s *stubBackend) GetBalanceAt(addr common.Address, blockNum uint64) (int64, error) {
	if s.atErr != nil {
		return 0, s.atErr
	}
	return s.balanceAt, nil
}
func (s *stubBackend) GetCodeAt(addr common.Address, blockNum uint64) ([]byte, error) {
	if s.atErr != nil {
		return nil, s.atErr
	}
	return s.codeAt, nil
}
func (s *stubBackend) GetStorageAtBlock(addr common.Address, slot common.Hash, blockNum uint64) (common.Hash, error) {
	if s.atErr != nil {
		return common.Hash{}, s.atErr
	}
	return s.storageAt, nil
}
func (s *stubBackend) GetBlockByNumber(num uint64) (*types.Block, error)     { return s.block, nil }
func (s *stubBackend) GetBlockByHash(hash common.Hash) (*types.Block, error) { return s.block, nil }
func (s *stubBackend) GetTransactionByHash(hash common.Hash) (*corepb.Transaction, *types.Block, int, error) {
	return s.tx, s.txBlock, s.txIndex, nil
}
func (s *stubBackend) GetTransactionInfo(hash common.Hash) (*corepb.TransactionInfo, error) {
	return s.txInfo, nil
}
func (s *stubBackend) Call(from, to *common.Address, data []byte, value int64) ([]byte, error) {
	return s.callResult, nil
}
func (s *stubBackend) GetLogs(filter jsonrpc.LogFilter) ([]*jsonrpc.RPCLog, error) {
	return s.logs, nil
}
func (s *stubBackend) GasPrice() int64 { return s.gasPrice }
func (s *stubBackend) PeerCount() int  { return s.peerCount }
func (s *stubBackend) EstimateGas(from, to *common.Address, data []byte, value int64) (uint64, error) {
	return 0, nil
}
func (s *stubBackend) SubscribeBlocks(_ chan<- *types.Block)   {}
func (s *stubBackend) UnsubscribeBlocks(_ chan<- *types.Block) {}
