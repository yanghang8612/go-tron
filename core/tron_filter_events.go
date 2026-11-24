package core

import (
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
)

type filterEventSubscriber struct {
	ch        chan<- []jsonrpc.FilterBlockEvent
	logDemand func() bool
}

func (b *TronBackend) SubscribeFilterEvents(ch chan<- []jsonrpc.FilterBlockEvent, logDemand func() bool) {
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	b.filterSubs = append(b.filterSubs, filterEventSubscriber{ch, logDemand})
}

func (b *TronBackend) UnsubscribeFilterEvents(ch chan<- []jsonrpc.FilterBlockEvent) {
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	for i, sub := range b.filterSubs {
		if sub.ch == ch {
			b.filterSubs = append(b.filterSubs[:i], b.filterSubs[i+1:]...)
			return
		}
	}
}

func (b *TronBackend) filterLogDemandLocked() bool {
	for _, sub := range b.filterSubs {
		if sub.logDemand != nil && sub.logDemand() {
			return true
		}
	}
	return false
}

func (b *TronBackend) notifyFilterSubscribersLocked(batch []jsonrpc.FilterBlockEvent) {
	for _, sub := range b.filterSubs {
		select {
		case sub.ch <- batch:
		default: // drop the whole batch if the subscriber is slow
		}
	}
}

func (b *TronBackend) prepareFilterReorg(orphaned []*types.Block) func([]*types.Block) {
	b.subsMu.Lock()
	b.filterReorg = true
	var removed []jsonrpc.FilterBlockEvent
	if b.filterLogDemandLocked() {
		for _, block := range orphaned {
			event := b.captureFilterBlock(block, true, true)
			if len(event.Logs) > 0 {
				removed = append(removed, event)
			}
		}
	}
	b.subsMu.Unlock()

	return func(replacement []*types.Block) {
		b.subsMu.Lock()
		defer b.subsMu.Unlock()
		b.filterReorg = false
		if len(replacement) == 0 {
			return
		}
		if len(b.filterSubs) != 0 {
			logDemand := b.filterLogDemandLocked()
			for _, block := range replacement {
				removed = append(removed, b.captureFilterBlock(block, false, logDemand))
			}
			b.notifyFilterSubscribersLocked(removed)
		}
		for _, block := range replacement {
			b.notifyBlockSubscribersLocked(block)
		}
	}
}

func (b *TronBackend) captureFilterBlock(block *types.Block, removed, logDemand bool) jsonrpc.FilterBlockEvent {
	event := jsonrpc.FilterBlockEvent{Block: block, Removed: removed}
	if !logDemand || len(block.Proto().GetTransactions()) == 0 {
		return event
	}
	infos, found, err := rawdb.ReadTransactionInfosByBlockStrict(b.chain.chaindb, block.Number())
	if err == nil && found {
		err = rawdb.ValidateTransactionInfosForBlock(block.Number(), block.Transactions(), infos, "JSON-RPC subscription")
	}
	if err != nil {
		log.Warn("Read subscription receipts", "block", block.Number(), "hash", block.Hash(), "err", err)
		return event
	}
	if !found {
		return event
	}
	for index, info := range infos {
		event.Logs = jsonrpc.AppendFilterBlockLogs(event.Logs, block, index, info, removed)
	}
	return event
}
