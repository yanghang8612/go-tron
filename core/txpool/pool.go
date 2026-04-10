package txpool

import (
	"errors"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

const defaultMaxPoolSize = 10000

var (
	ErrPoolFull     = errors.New("transaction pool is full")
	ErrAlreadyKnown = errors.New("transaction already in pool")
	ErrNoContract   = errors.New("transaction has no contract")
)

// TxPool holds pending transactions waiting to be included in a block.
type TxPool struct {
	mu      sync.RWMutex
	pending map[tcommon.Hash]*types.Transaction
	maxSize int
}

// New creates a new transaction pool.
func New() *TxPool {
	return &TxPool{
		pending: make(map[tcommon.Hash]*types.Transaction),
		maxSize: defaultMaxPoolSize,
	}
}

// Add validates and adds a transaction to the pool.
func (pool *TxPool) Add(tx *types.Transaction) error {
	if tx.Contract() == nil {
		return ErrNoContract
	}

	hash := tx.Hash()

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if _, exists := pool.pending[hash]; exists {
		return ErrAlreadyKnown
	}
	if len(pool.pending) >= pool.maxSize {
		return ErrPoolFull
	}

	pool.pending[hash] = tx
	return nil
}

// Get returns a transaction by hash, or nil if not found.
func (pool *TxPool) Get(hash tcommon.Hash) *types.Transaction {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.pending[hash]
}

// Pending returns all pending transactions as a slice.
func (pool *TxPool) Pending() []*types.Transaction {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	txs := make([]*types.Transaction, 0, len(pool.pending))
	for _, tx := range pool.pending {
		txs = append(txs, tx)
	}
	return txs
}

// Remove deletes a transaction from the pool.
func (pool *TxPool) Remove(hash tcommon.Hash) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	delete(pool.pending, hash)
}

// RemoveBatch removes multiple transactions from the pool.
func (pool *TxPool) RemoveBatch(hashes []tcommon.Hash) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	for _, h := range hashes {
		delete(pool.pending, h)
	}
}

// Count returns the number of pending transactions.
func (pool *TxPool) Count() int {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return len(pool.pending)
}
