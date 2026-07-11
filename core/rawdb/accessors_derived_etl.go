package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// DerivedIndexCollector is a typed ETL front-end for replay-derived rawdb
// indexes. Backfill and snapshot-build callers can add rows in block execution
// order, then Load writes the final stream in physical key order.
type DerivedIndexCollector struct {
	collector *etl.Collector
}

func NewDerivedIndexCollector(opts etl.Options) (*DerivedIndexCollector, error) {
	collector, err := etl.NewCollector(opts)
	if err != nil {
		return nil, err
	}
	return &DerivedIndexCollector{collector: collector}, nil
}

func (c *DerivedIndexCollector) TempDir() string {
	if c == nil || c.collector == nil {
		return ""
	}
	return c.collector.TempDir()
}

func (c *DerivedIndexCollector) Close() error {
	if c == nil || c.collector == nil {
		return nil
	}
	return c.collector.Close()
}

func (c *DerivedIndexCollector) Load(db ethdb.KeyValueWriter) (etl.Stats, error) {
	if c == nil || c.collector == nil {
		return etl.Stats{}, errors.New("rawdb: nil derived index collector")
	}
	return c.collector.Load(db)
}

func (c *DerivedIndexCollector) PutTransactionInfo(txID []byte, info *corepb.TransactionInfo) error {
	if c == nil || c.collector == nil {
		return errors.New("rawdb: nil derived index collector")
	}
	if err := validateTransactionInfoIDForKey(txID, info, "collect transaction info"); err != nil {
		return err
	}
	data, err := proto.Marshal(info)
	if err != nil {
		return err
	}
	return c.collector.Put(txInfoKey(txID), data)
}

// DeleteTransactionInfo removes a legacy per-transaction receipt row. Canonical
// receipt reads resolve through tx- plus tib-, so offline rebuilds use this to
// discard stale duplicate ti- rows while retaining the block-level payload.
func (c *DerivedIndexCollector) DeleteTransactionInfo(txID []byte) error {
	if c == nil || c.collector == nil {
		return errors.New("rawdb: nil derived index collector")
	}
	if err := validateTransactionHashKey(txID, "collect transaction info delete"); err != nil {
		return err
	}
	return c.collector.Delete(txInfoKey(txID))
}

func (c *DerivedIndexCollector) PutTransactionInfosByBlock(blockNum uint64, infos []*corepb.TransactionInfo) error {
	if c == nil || c.collector == nil {
		return errors.New("rawdb: nil derived index collector")
	}
	if err := validateTransactionInfosForKey(blockNum, infos, "collect transaction infos by block"); err != nil {
		return err
	}
	ret := &corepb.TransactionRet{
		BlockNumber:     int64(blockNum),
		Transactioninfo: infos,
	}
	if len(infos) > 0 {
		ret.BlockTimeStamp = infos[0].BlockTimeStamp
	}
	data, err := proto.Marshal(ret)
	if err != nil {
		return err
	}
	return c.collector.Put(txInfoBlockKey(blockNum), data)
}

func (c *DerivedIndexCollector) PutTransactionIndex(txHash []byte, blockNum uint64) error {
	if c == nil || c.collector == nil {
		return errors.New("rawdb: nil derived index collector")
	}
	if err := validateTransactionHashKey(txHash, "collect transaction index"); err != nil {
		return err
	}
	var num [8]byte
	binary.BigEndian.PutUint64(num[:], blockNum)
	return c.collector.Put(txKey(txHash), num[:])
}

func (c *DerivedIndexCollector) PutAccountTrace(owner []byte, blockNum int64, balance int64) error {
	if c == nil || c.collector == nil {
		return errors.New("rawdb: nil derived index collector")
	}
	if len(owner) == 0 {
		return fmt.Errorf("account trace: empty owner")
	}
	data, err := proto.Marshal(&contractpb.AccountTrace{Balance: balance})
	if err != nil {
		return fmt.Errorf("account trace: marshal: %w", err)
	}
	return c.collector.Put(accountTraceKey(owner, blockNum), data)
}

func (c *DerivedIndexCollector) PutBlockBalanceTrace(blockNum int64, trace *contractpb.BlockBalanceTrace) error {
	if c == nil || c.collector == nil {
		return errors.New("rawdb: nil derived index collector")
	}
	if err := validateBlockBalanceTraceForKey(blockNum, trace, "collect block balance trace"); err != nil {
		return err
	}
	data, err := proto.Marshal(trace)
	if err != nil {
		return err
	}
	return c.collector.Put(balanceTraceKey(blockNum), data)
}

func (c *DerivedIndexCollector) PutSectionBloom(section, bitIndex uint64, bloom []byte) error {
	if c == nil || c.collector == nil {
		return errors.New("rawdb: nil derived index collector")
	}
	if err := validateSectionBloomCoordinates(section, bitIndex, "collect section bloom"); err != nil {
		return err
	}
	if err := validateSectionBloomValue(bloom, "collect section bloom"); err != nil {
		return err
	}
	return c.collector.Put(sectionBloomKey(section, bitIndex), bloom)
}
