package snapshots

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

const (
	eventLogIdentityMaxBytes  = 32 << 20
	eventLogIdentityMaxBlocks = 65536
	// Charge identity fields and a conservative fixed map entry allowance in
	// addition to exact owned transaction-hash bytes. No bodies/logs are kept.
	eventLogIdentityBlockCharge = 160
)

type eventLogChainIdentity struct {
	bodyDigest   [sha256.Size]byte
	blockHash    common.Hash
	transactions []common.Hash
}

// This cache lives for exactly one two-pass build. It never grants authority
// to an old canonical block: a hit must match the complete raw body digest
// read again from storage, and receipts retain strict decoding/coverage checks.
// It avoids repeated protobuf allocation and transaction hashing without a
// second body spool, a cross-build cache, or assumptions about reorg timing.
type eventLogChainIdentityCache struct {
	blocks    map[uint64]eventLogChainIdentity
	bytes     uint64
	maxBytes  uint64
	maxBlocks int
}

func newEventLogChainIdentityCache() *eventLogChainIdentityCache {
	return &eventLogChainIdentityCache{blocks: make(map[uint64]eventLogChainIdentity), maxBytes: eventLogIdentityMaxBytes, maxBlocks: eventLogIdentityMaxBlocks}
}

func (c *eventLogChainIdentityCache) put(number uint64, identity eventLogChainIdentity) {
	if c == nil || len(c.blocks) >= c.maxBlocks {
		return
	}
	if _, exists := c.blocks[number]; exists {
		return
	}
	charge := uint64(len(identity.transactions))*common.HashLength + eventLogIdentityBlockCharge
	if c.bytes > c.maxBytes || charge > c.maxBytes-c.bytes {
		return
	}
	c.blocks[number] = identity
	c.bytes += charge
}

func (r eventLogV3ChainReader) readBlock(number uint64) (eventLogChainIdentity, []*corepb.TransactionInfo, error) {
	var identity eventLogChainIdentity
	data, ok, err := rawdb.ReadBlockRawStrict(r.chain, number)
	if err != nil {
		return identity, nil, err
	}
	if !ok {
		return identity, nil, fmt.Errorf("snapshots: missing block %d during V4 event-log build", number)
	}
	if r.identities != nil {
		if cached, exists := r.identities.blocks[number]; exists {
			if sha256.Sum256(data) != cached.bodyDigest {
				return identity, nil, fmt.Errorf("snapshots: event-log source block %d changed between passes", number)
			}
			infos, _, err := rawdb.ReadTransactionInfosByBlockStrict(r.chain, number)
			if err != nil {
				return identity, nil, err
			}
			// Strict decoding already checks nil rows, block numbers and ID
			// lengths. The first pass validated every canonical transaction;
			// the digest above proves those same transactions still apply.
			if number != 0 || len(infos) != 0 {
				if len(infos) != len(cached.transactions) {
					return identity, nil, fmt.Errorf("%w for block %d during V4 cached event-log build: have %d entries for %d transactions", rawdb.ErrIncompleteTransactionInfoCoverage, number, len(infos), len(cached.transactions))
				}
				for i, info := range infos {
					if len(info.Id) != 0 && !bytes.Equal(info.Id, cached.transactions[i][:]) {
						return identity, nil, fmt.Errorf("snapshots: transaction info id does not match cached canonical transaction at block %d index %d", number, i)
					}
				}
			}
			return cached, infos, nil
		}
	}
	block, err := types.UnmarshalBlock(data)
	if err != nil {
		return identity, nil, fmt.Errorf("rawdb: block %d decode: %w", number, err)
	}
	if block.Number() != number {
		return identity, nil, fmt.Errorf("rawdb: block row %d contains block number %d", number, block.Number())
	}
	infos, _, err := rawdb.ReadTransactionInfosByBlockStrict(r.chain, number)
	if err != nil {
		return identity, nil, err
	}
	txs := block.Transactions()
	if err := rawdb.ValidateTransactionInfosForBlock(number, txs, infos, "V4 event-log segment build"); err != nil {
		return identity, nil, err
	}
	identity.blockHash = block.Hash()
	identity.transactions = make([]common.Hash, len(txs))
	for i, tx := range txs {
		// Genesis may intentionally have no receipts; keep its prior special
		// case without requiring a hash for an unused transaction position.
		if tx != nil {
			identity.transactions[i] = tx.Hash()
		}
	}
	if r.identities != nil {
		identity.bodyDigest = sha256.Sum256(data)
		r.identities.put(number, identity)
	}
	return identity, infos, nil
}
