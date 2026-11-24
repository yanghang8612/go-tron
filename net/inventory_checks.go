package net

import (
	"fmt"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	inventoryWindowSeconds = 10
	transactionInvLimit    = 1000 * inventoryWindowSeconds
	blockInvLimit          = 10 * inventoryWindowSeconds
)

type inventoryBucket struct {
	second int64
	count  int
}

// peerInventoryLimits counts admitted elements in independent per-type windows.
// It uses one-second buckets like java-tron's MessageCount.
type peerInventoryLimits struct {
	mu    sync.Mutex
	block [inventoryWindowSeconds]inventoryBucket
	trx   [inventoryWindowSeconds]inventoryBucket
}

func (l *peerInventoryLimits) allow(kind corepb.Inventory_InventoryType, count int, second int64) bool {
	buckets, limit := &l.trx, transactionInvLimit
	if kind == corepb.Inventory_BLOCK {
		buckets, limit = &l.block, blockInvLimit
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, bucket := range buckets {
		if age := second - bucket.second; age >= 0 && age < inventoryWindowSeconds {
			limit -= bucket.count
		}
	}
	if count > limit {
		return false
	}
	slot := &buckets[(second%inventoryWindowSeconds+inventoryWindowSeconds)%inventoryWindowSeconds]
	if slot.second != second {
		*slot = inventoryBucket{second: second}
	}
	slot.count += count
	return true
}

func decodeInventory(payload []byte) (*corepb.Inventory, error) {
	var inventory corepb.Inventory
	if err := proto.Unmarshal(payload, &inventory); err != nil {
		return nil, err
	}
	if inventory.Type != corepb.Inventory_BLOCK && inventory.Type != corepb.Inventory_TRX {
		return nil, fmt.Errorf("unknown inventory type: %d", inventory.Type)
	}
	return &inventory, nil
}

func validateInventoryIDs(ids [][]byte) error {
	seen := make(map[tcommon.Hash]struct{}, len(ids))
	for _, id := range ids {
		if len(id) != tcommon.HashLength {
			return fmt.Errorf("inventory ID length %d, expected %d", len(id), tcommon.HashLength)
		}
		hash := tcommon.Hash(id)
		if _, duplicate := seen[hash]; duplicate {
			return fmt.Errorf("duplicate inventory ID %s", hash)
		}
		seen[hash] = struct{}{}
	}
	return nil
}
