package jsonrpc

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
	eventCh chan []FilterBlockEvent
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
	if backend, ok := fm.backend.(FilterEventBackend); ok {
		fm.eventCh = make(chan []FilterBlockEvent, 64)
		backend.SubscribeFilterEvents(fm.eventCh, func() bool {
			_, logs := fm.eventDemand()
			return logs
		})
	} else {
		fm.backend.SubscribeBlocks(fm.blockCh)
	}
	fm.wg.Add(1)
	go fm.run()
}

// Stop unsubscribes and waits for the background goroutine to finish.
func (fm *FilterManager) Stop() {
	close(fm.quit)
	fm.wg.Wait()
	if backend, ok := fm.backend.(FilterEventBackend); ok {
		backend.UnsubscribeFilterEvents(fm.eventCh)
	} else {
		fm.backend.UnsubscribeBlocks(fm.blockCh)
	}
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
		case batch := <-fm.eventCh:
			for _, event := range batch {
				fm.publish(event)
			}
		case <-gc.C:
			fm.gc()
		}
	}
}

func (fm *FilterManager) fanOut(block *types.Block) {
	blockDemand, logDemand := fm.eventDemand()
	if !blockDemand && !logDemand {
		return
	}

	// Receipts are persisted protobuf objects and expensive to read/decode. Only
	// touch them when at least one poll or WebSocket log consumer exists; block
	// filters and newHeads subscriptions need the header alone.
	var logs []*RPCLog
	if logDemand {
		for i, tx := range block.Transactions() {
			info, err := fm.backend.GetTransactionInfo(tx.Hash())
			if err == nil {
				logs = AppendFilterBlockLogs(logs, block, i, info, false)
			}
		}
	}

	fm.publish(FilterBlockEvent{Block: block, Logs: logs})
}

// AppendFilterBlockLogs formats one transaction's logs in the live filter stream.
func AppendFilterBlockLogs(logs []*RPCLog, block *types.Block, index int, info *corepb.TransactionInfo, removed bool) []*RPCLog {
	for _, entry := range info.GetLog() {
		topics := make([]string, len(entry.GetTopics()))
		for i, topic := range entry.GetTopics() {
			topics[i] = hexBytes(topic)
		}
		logs = append(logs, &RPCLog{
			Address:          hex20(entry.GetAddress()),
			Topics:           topics,
			Data:             hexBytes(entry.GetData()),
			BlockNumber:      hexUint64(block.Number()),
			BlockTimestamp:   hexUint64(uint64(block.Timestamp() / 1000)),
			TransactionHash:  hexHash(block.Transactions()[index].Hash()),
			TransactionIndex: hexUint64(uint64(index)),
			BlockHash:        hexHash(block.Hash()),
			LogIndex:         hexUint64(uint64(len(logs))),
			Removed:          removed,
		})
	}
	return logs
}

func (fm *FilterManager) publish(event FilterBlockEvent) {
	var hashHex string
	if !event.Removed {
		blockHash := event.Block.Hash()
		hashHex = "0x" + hex.EncodeToString(blockHash[:])
	}
	fm.mu.Lock()
	for _, f := range fm.filters {
		switch f.kind {
		case filterKindBlock:
			if !event.Removed {
				f.pendingHashes = append(f.pendingHashes, hashHex)
			}
		case filterKindLog:
			for _, log := range event.Logs {
				if matchesLogFilter(log, f.logFilter) {
					f.pendingLogs = append(f.pendingLogs, log)
				}
			}
		}
	}
	fm.mu.Unlock()

	if fm.subMgr != nil {
		fm.subMgr.notify(event.Block, event.Logs, event.Removed)
	}
}

// eventDemand snapshots whether the next block has header and/or log
// consumers. Each map is inspected under its owning mutex. A filter or
// subscription created concurrently after this snapshot is linearized after
// the current block; once creation returns, every later fanOut observes it.
func (fm *FilterManager) eventDemand() (block, logs bool) {
	fm.mu.Lock()
	for _, f := range fm.filters {
		switch f.kind {
		case filterKindBlock:
			block = true
		case filterKindLog:
			logs = true
		}
		if block && logs {
			break
		}
	}
	fm.mu.Unlock()

	if fm.subMgr != nil {
		subBlock, subLogs := fm.subMgr.eventDemand()
		block = block || subBlock
		logs = logs || subLogs
	}
	return block, logs
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
func (fm *FilterManager) GetFilterLogs(id string) ([]*RPCLog, error) {
	fm.mu.Lock()
	f, ok := fm.filters[id]
	if !ok || f.kind != filterKindLog {
		fm.mu.Unlock()
		return nil, fmt.Errorf("filter not found")
	}
	lf := *f.logFilter
	f.lastPoll = time.Now()
	fm.mu.Unlock()

	if err := validateLogBlockRange(lf, fm.backend.BlockNumber()); err != nil {
		return nil, err
	}
	logs, err := fm.backend.GetLogs(lf)
	if err != nil {
		return nil, err
	}
	if logs == nil {
		return []*RPCLog{}, nil
	}
	return logs, nil
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
