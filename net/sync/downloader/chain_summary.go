// Package downloader will host the per-session block downloader extracted
// from net/sync.go in slice 4 of the refactor. For slice 1 it only carries
// the two pure helpers — BuildChainSummary and FindCommonBlock — that
// today live as methods on SyncService but do not touch sync state.
package downloader

import (
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/types"
)

// BuildChainSummary creates an exponentially-spaced list of block IDs
// from `chain`, used in SYNC_BLOCK_CHAIN messages. The result is in
// ascending order (oldest first, newest last) — matching java-tron's
// `SyncService.getBlockChainSummary` convention. java-tron's
// `SyncBlockChainMsgHandler.check` enforces
// `summary[last].num >= peer.lastSyncBlockId.num`, so the summary must
// end at our current head; sending it head-first triggers BAD_MESSAGE
// after the first inventory exchange.
func BuildChainSummary(chain *core.BlockChain) []types.BlockID {
	head := chain.CurrentBlock()
	headNum := head.Number()

	var summary []types.BlockID
	step := uint64(1)
	num := headNum

	for {
		block := chain.GetBlockByNumber(num)
		if block != nil {
			summary = append(summary, block.ID())
		}
		if num == 0 {
			break
		}
		if num < step {
			num = 0
		} else {
			num -= step
		}
		// Double step each time for exponential backoff
		step *= 2
	}

	// Reverse to ascending order: java-tron expects oldest first.
	for i, j := 0, len(summary)-1; i < j; i, j = i+1, j-1 {
		summary[i], summary[j] = summary[j], summary[i]
	}
	return summary
}

// FindCommonBlock finds the highest block in peerSummary that exists in
// `chain`. Returns 0 (genesis) when no entry matches.
func FindCommonBlock(chain *core.BlockChain, peerSummary []types.BlockID) uint64 {
	headNum := chain.CurrentBlock().Number()
	for _, bid := range peerSummary {
		if bid.Number() > headNum {
			continue
		}
		block := chain.GetBlockByNumber(bid.Number())
		if block != nil && block.ID().Hash == bid.Hash {
			return bid.Number()
		}
	}
	return 0 // fallback to genesis
}
