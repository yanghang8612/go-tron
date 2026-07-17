package jsonrpc

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/core/types"
)

const (
	filterIdleTimeout = 5 * time.Minute
	filterGCInterval  = 30 * time.Second
)

type filterKind int

const (
	filterKindLog   filterKind = iota
	filterKindBlock            // eth_newBlockFilter
)

type filter struct {
	kind          filterKind
	logFilter     *LogFilter // non-nil for filterKindLog
	lastPoll      time.Time
	pendingLogs   []*RPCLog
	pendingHashes []string
}

// FilterManager tracks active eth_newFilter / eth_newBlockFilter subscriptions.
// It subscribes to new blocks from the backend and fans out to registered filters.
type FilterManager struct {
	mu      sync.Mutex
	filters map[string]*filter
	backend Backend
	subMgr  *SubscriptionManager // optional; notified after poll filters on each block
	blockCh chan *types.Block
	quit    chan struct{}
	wg      sync.WaitGroup
}

// NewFilterManager creates a FilterManager backed by the given backend.
func NewFilterManager(backend Backend) *FilterManager {
	return &FilterManager{
		filters: make(map[string]*filter),
		backend: backend,
		blockCh: make(chan *types.Block, 64),
		quit:    make(chan struct{}),
	}
}

// Start subscribes to new blocks and begins the GC loop.
func (fm *FilterManager) Start() {
	fm.backend.SubscribeBlocks(fm.blockCh)
	fm.wg.Add(1)
	go fm.run()
}

// Stop unsubscribes and waits for the background goroutine to finish.
func (fm *FilterManager) Stop() {
	close(fm.quit)
	fm.wg.Wait()
	fm.backend.UnsubscribeBlocks(fm.blockCh)
}

func (fm *FilterManager) run() {
	defer fm.wg.Done()
	gc := time.NewTicker(filterGCInterval)
	defer gc.Stop()
	for {
		select {
		case <-fm.quit:
			return
		case block, ok := <-fm.blockCh:
			if ok {
				fm.fanOut(block)
			}
		case <-gc.C:
			fm.gc()
		}
	}
}

func (fm *FilterManager) fanOut(block *types.Block) {
	blockHash := block.Hash()
	hashHex := "0x" + hex.EncodeToString(blockHash[:])

	// Gather logs from block transactions for log and subscription filters.
	var logs []*RPCLog
	logIndex := uint64(0)
	for i, tx := range block.Transactions() {
		info, err := fm.backend.GetTransactionInfo(tx.Hash())
		if err != nil || info == nil {
			continue
		}
		for _, l := range info.Log {
			topics := make([]string, len(l.Topics))
			for ti, t := range l.Topics {
				topics[ti] = hexBytes(t)
			}
			txHash := tx.Hash()
			logs = append(logs, &RPCLog{
				Address:          hex20(l.Address),
				Topics:           topics,
				Data:             hexBytes(l.Data),
				BlockNumber:      hexUint64(block.Number()),
				BlockTimestamp:   hexUint64(uint64(block.Timestamp() / 1000)),
				TransactionHash:  "0x" + hex.EncodeToString(txHash[:]),
				TransactionIndex: hexUint64(uint64(i)),
				BlockHash:        hashHex,
				LogIndex:         hexUint64(logIndex),
				Removed:          false,
			})
			logIndex++
		}
	}

	fm.mu.Lock()
	for _, f := range fm.filters {
		switch f.kind {
		case filterKindBlock:
			f.pendingHashes = append(f.pendingHashes, hashHex)
		case filterKindLog:
			for _, log := range logs {
				if matchesLogFilter(log, f.logFilter) {
					f.pendingLogs = append(f.pendingLogs, log)
				}
			}
		}
	}
	fm.mu.Unlock()

	if fm.subMgr != nil {
		fm.subMgr.notify(block, logs)
	}
}

func (fm *FilterManager) gc() {
	now := time.Now()
	fm.mu.Lock()
	defer fm.mu.Unlock()
	for id, f := range fm.filters {
		if now.Sub(f.lastPoll) > filterIdleTimeout {
			delete(fm.filters, id)
		}
	}
}

func generateFilterID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "0x" + hex.EncodeToString(b), nil
}

// NewLogFilter creates an eth_newFilter subscription.
func (fm *FilterManager) NewLogFilter(lf LogFilter) (string, error) {
	id, err := generateFilterID()
	if err != nil {
		return "", err
	}
	fm.mu.Lock()
	fm.filters[id] = &filter{
		kind:      filterKindLog,
		logFilter: &lf,
		lastPoll:  time.Now(),
	}
	fm.mu.Unlock()
	return id, nil
}

// NewBlockFilter creates an eth_newBlockFilter subscription.
func (fm *FilterManager) NewBlockFilter() (string, error) {
	id, err := generateFilterID()
	if err != nil {
		return "", err
	}
	fm.mu.Lock()
	fm.filters[id] = &filter{
		kind:     filterKindBlock,
		lastPoll: time.Now(),
	}
	fm.mu.Unlock()
	return id, nil
}

// UninstallFilter removes a filter. Returns false if not found.
func (fm *FilterManager) UninstallFilter(id string) bool {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if _, ok := fm.filters[id]; !ok {
		return false
	}
	delete(fm.filters, id)
	return true
}

// GetFilterChanges returns and clears accumulated changes since last poll.
func (fm *FilterManager) GetFilterChanges(id string) (interface{}, bool) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	f, ok := fm.filters[id]
	if !ok {
		return nil, false
	}
	f.lastPoll = time.Now()
	switch f.kind {
	case filterKindBlock:
		hashes := f.pendingHashes
		f.pendingHashes = nil
		if hashes == nil {
			hashes = []string{}
		}
		return hashes, true
	case filterKindLog:
		logs := f.pendingLogs
		f.pendingLogs = nil
		if logs == nil {
			logs = []*RPCLog{}
		}
		return logs, true
	}
	return nil, false
}

// GetFilterLogs returns all current logs matching a log filter (ignores accumulated).
func (fm *FilterManager) GetFilterLogs(id string) ([]*RPCLog, bool) {
	fm.mu.Lock()
	f, ok := fm.filters[id]
	if !ok || f.kind != filterKindLog {
		fm.mu.Unlock()
		return nil, false
	}
	lf := *f.logFilter
	f.lastPoll = time.Now()
	fm.mu.Unlock()

	logs, err := fm.backend.GetLogs(lf)
	if err != nil || logs == nil {
		return []*RPCLog{}, true
	}
	return logs, true
}

// matchesLogFilter checks whether a single RPCLog matches a LogFilter.
func matchesLogFilter(log *RPCLog, lf *LogFilter) bool {
	if lf == nil {
		return true
	}
	if len(lf.Addresses) > 0 {
		found := false
		for _, addr := range lf.Addresses {
			if hex20(addr[:]) == log.Address {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for i, required := range lf.Topics {
		if len(required) == 0 {
			continue
		}
		if i >= len(log.Topics) {
			return false
		}
		matched := false
		for _, h := range required {
			if hexBytes(h[:]) == log.Topics[i] {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}
