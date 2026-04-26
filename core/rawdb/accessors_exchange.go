package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// ListAllExchanges returns all exchanges stored in the database.
func ListAllExchanges(db ethdb.Iteratee) []*corepb.Exchange {
	it := db.NewIterator(exchangePrefix, nil)
	defer it.Release()
	var result []*corepb.Exchange
	for it.Next() {
		ex := &corepb.Exchange{}
		if err := proto.Unmarshal(it.Value(), ex); err == nil {
			result = append(result, ex)
		}
	}
	return result
}

// WriteExchange stores an Exchange by its ExchangeId.
func WriteExchange(db ethdb.KeyValueWriter, ex *corepb.Exchange) error {
	data, err := proto.Marshal(ex)
	if err != nil {
		return err
	}
	return db.Put(exchangeKey(ex.ExchangeId), data)
}

// ReadExchange returns the Exchange with the given id, or nil if not found.
func ReadExchange(db ethdb.KeyValueReader, id int64) *corepb.Exchange {
	data, err := db.Get(exchangeKey(id))
	if err != nil || len(data) == 0 {
		return nil
	}
	var ex corepb.Exchange
	if err := proto.Unmarshal(data, &ex); err != nil {
		return nil
	}
	return &ex
}

// DeleteExchange removes the Exchange with the given id.
func DeleteExchange(db ethdb.KeyValueWriter, id int64) {
	_ = db.Delete(exchangeKey(id))
}
