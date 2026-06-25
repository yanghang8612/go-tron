package core

import (
	"errors"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestLoadStoredHeadBlockSurfacesColdLookupErrors(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	chain := rawdb.NewChainDB(db, rawdb.NoopAncient{})
	head := tcommon.Hash{0x42}
	rawdb.WriteHeadBlockHash(db, head)
	chain.SetChainIndexReader(blockchainStartupErrChainIndex{err: errors.New("cold chain index corrupt")})

	_, err := loadStoredHeadBlock(chain, blockchainStartupBlock(0))
	if err == nil || !strings.Contains(err.Error(), "cold chain index corrupt") {
		t.Fatalf("loadStoredHeadBlock err = %v, want cold chain index error", err)
	}
	if !strings.Contains(err.Error(), "load stored head") {
		t.Fatalf("loadStoredHeadBlock err = %v, want startup head context", err)
	}
}

func TestNewBlockChainWithAncientSurfacesGenesisColdReadErrors(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	_, err := NewBlockChainWithAncient(db, state.NewDatabase(db), params.MainnetChainConfig, blockchainStartupFailingAncient{
		kind:   rawdb.AncientBlocksTable,
		number: 0,
		err:    errors.New("cold genesis read failed"),
	})
	if err == nil || !strings.Contains(err.Error(), "cold genesis read failed") {
		t.Fatalf("NewBlockChainWithAncient err = %v, want cold genesis read error", err)
	}
	if !strings.Contains(err.Error(), "read genesis block") {
		t.Fatalf("NewBlockChainWithAncient err = %v, want genesis read context", err)
	}
}

func blockchainStartupBlock(number uint64) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(number) * 3000,
				Version:   params.BlockVersion,
			},
		},
	})
}

type blockchainStartupFailingAncient struct {
	kind   string
	number uint64
	err    error
}

func (a blockchainStartupFailingAncient) Ancient(kind string, number uint64) ([]byte, error) {
	if kind == a.kind && number == a.number {
		return nil, a.err
	}
	return nil, rawdb.ErrNotInAncient
}

func (a blockchainStartupFailingAncient) AncientRange(string, uint64, uint64, uint64) ([][]byte, error) {
	return nil, rawdb.ErrNotInAncient
}

func (a blockchainStartupFailingAncient) AncientCount(string) (uint64, error) {
	return 0, nil
}

func (a blockchainStartupFailingAncient) HasAncient(kind string, number uint64) (bool, error) {
	return kind == a.kind && number == a.number, nil
}

type blockchainStartupErrChainIndex struct {
	err error
}

func (i blockchainStartupErrChainIndex) BlockNumberByHash(tcommon.Hash) (uint64, bool, error) {
	return 0, false, i.err
}

func (i blockchainStartupErrChainIndex) TransactionBlockNumberByHash(tcommon.Hash) (uint64, bool, error) {
	return 0, false, i.err
}
