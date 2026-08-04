package domains

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestCommitmentFoldStatsNodeHashRounds(t *testing.T) {
	stats := new(commitmentFoldStats)
	var oneRound BranchData
	for nibble := uint8(0); nibble < 4; nibble++ {
		oneRound.SetHashChild(nibble, common.Hash{nibble + 1})
	}
	h := borrowKeccak()
	defer returnKeccak(h)
	oneRound.nodeHashWithStats(h, stats)

	var twoRound BranchData
	for nibble := uint8(0); nibble < 5; nibble++ {
		twoRound.SetHashChild(nibble, common.Hash{nibble + 1})
	}
	twoRound.nodeHashWithStats(h, stats)

	if stats.nodeHashes != 2 {
		t.Fatalf("node hashes = %d, want 2", stats.nodeHashes)
	}
	if stats.nodeHashBytes != 133+166 {
		t.Fatalf("node hash bytes = %d, want %d", stats.nodeHashBytes, 133+166)
	}
	if stats.nodeHashRounds != 3 || stats.oneRoundHashes != 1 || stats.multiRoundHashes != 1 {
		t.Fatalf("rounds/one/multi = %d/%d/%d, want 3/1/1", stats.nodeHashRounds, stats.oneRoundHashes, stats.multiRoundHashes)
	}
}

func TestCommitmentFoldStatsMerge(t *testing.T) {
	total := &commitmentFoldStats{nodeHashes: 1, nodeHashBytes: 34, nodeHashRounds: 1, oneRoundHashes: 1}
	other := &commitmentFoldStats{nodeHashes: 2, nodeHashBytes: 299, nodeHashRounds: 3, oneRoundHashes: 1, multiRoundHashes: 1}
	total.merge(other)
	if total.nodeHashes != 3 || total.nodeHashBytes != 333 || total.nodeHashRounds != 4 ||
		total.oneRoundHashes != 2 || total.multiRoundHashes != 1 {
		t.Fatalf("merged stats = %+v", total)
	}
}
