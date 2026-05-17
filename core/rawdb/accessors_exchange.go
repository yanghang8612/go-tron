package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// ListAllExchanges returns all exchanges stored in the database.
func ListAllExchanges(db ethdb.Iteratee) []*corepb.Exchange {
	return listAllExchanges(db, exchangePrefix)
}

// ListAllExchangeV2 returns all exchanges stored in the java-tron exchange-v2 bucket.
func ListAllExchangeV2(db ethdb.Iteratee) []*corepb.Exchange {
	return listAllExchanges(db, exchangeV2Prefix)
}

func listAllExchanges(db ethdb.Iteratee, prefix []byte) []*corepb.Exchange {
	it := db.NewIterator(prefix, nil)
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

// WriteExchange stores an Exchange by its ExchangeId in the legacy exchange bucket.
func WriteExchange(db ethdb.KeyValueWriter, ex *corepb.Exchange) error {
	return writeExchangeAtKey(db, exchangeKey(ex.ExchangeId), ex)
}

// WriteExchangeV2 stores an Exchange by its ExchangeId in the exchange-v2 bucket.
func WriteExchangeV2(db ethdb.KeyValueWriter, ex *corepb.Exchange) error {
	return writeExchangeAtKey(db, exchangeV2Key(ex.ExchangeId), ex)
}

func writeExchangeAtKey(db ethdb.KeyValueWriter, key []byte, ex *corepb.Exchange) error {
	data, err := proto.Marshal(ex)
	if err != nil {
		return err
	}
	return db.Put(key, data)
}

// ReadExchange returns the legacy Exchange with the given id, or nil if not found.
func ReadExchange(db ethdb.KeyValueReader, id int64) *corepb.Exchange {
	return readExchangeAtKey(db, exchangeKey(id))
}

// ReadExchangeV2 returns the exchange-v2 Exchange with the given id, or nil if not found.
func ReadExchangeV2(db ethdb.KeyValueReader, id int64) *corepb.Exchange {
	return readExchangeAtKey(db, exchangeV2Key(id))
}

func readExchangeAtKey(db ethdb.KeyValueReader, key []byte) *corepb.Exchange {
	data, err := db.Get(key)
	if err != nil || len(data) == 0 {
		return nil
	}
	var ex corepb.Exchange
	if err := proto.Unmarshal(data, &ex); err != nil {
		return nil
	}
	return &ex
}

// DeleteExchange removes the legacy Exchange with the given id.
func DeleteExchange(db ethdb.KeyValueWriter, id int64) {
	_ = db.Delete(exchangeKey(id))
}

// DeleteExchangeV2 removes the exchange-v2 Exchange with the given id.
func DeleteExchangeV2(db ethdb.KeyValueWriter, id int64) {
	_ = db.Delete(exchangeV2Key(id))
}
