//go:build history_stress

package snapshots

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// Each row has a distinct four-byte Prev, so one output exceeds the unchanged
// R1 chunk budget. Complete 128-block outputs remain below that same limit.
func TestOfflineHistoryRepairBoundaryPartitionRespectsOutputBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("large real CDC boundary fixture")
	}
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "3")
	const blocks, rowsPerBlock = 257, 1024
	const rows = blocks * rowsPerBlock
	changes := make([]*rawdb.StateDomainChange, 0, rows)
	for i := 0; i < rows; i++ {
		block := uint64(i/rowsPerBlock + 1)
		row := binaryStateDomainChange(block, uint64(i+1), 1, "shared-key")
		row.BlockHash[1] = 1
		row.Prev = make([]byte, 4)
		binary.BigEndian.PutUint32(row.Prev, uint32(i+1))
		changes = append(changes, row)
	}
	dir := t.TempDir()
	refs := writeV6StateDomainHistorySegmentForTest(t, dir, 1, rows, changes)
	changes = nil
	compressV6StreamFixture(t, dir, refs, 4096)
	ctx := context.Background()
	_, err := CopyStateHistoryReferenceTrioRangeContext(ctx, dir, refs, 1, rows, fmtRepairTestPath(100), etl.Options{})
	if !errors.Is(err, errHistoryReferenceBudget) {
		t.Fatalf("single output should exceed the unchanged chunk budget: %v", err)
	}
	parts, err := PlanOfflineHistoryRepairBoundaryBlockSlices(ctx, dir, refs, 1, rows, 128)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 3 || parts[0].ToBlock != 128 || parts[1].ToBlock != 256 || parts[2].ToBlock != blocks {
		t.Fatalf("unexpected boundary partitions: %+v", parts)
	}
	var copied []SegmentRef
	for i, part := range parts {
		trio, err := CopyStateHistoryReferenceTrioRangeContext(ctx, dir, refs, part.FromTxNum, part.ToTxNum, fmtRepairTestPath(i+101), etl.Options{})
		if err != nil {
			t.Fatalf("part %d blocks [%d,%d]: %v", i, part.FromBlock, part.ToBlock, err)
		}
		copied = append(copied, trio...)
	}
	plan := &OfflineHistoryRepairPlan{Left: &OfflineHistoryRepairSlice{SourceRefs: refs, FromTxNum: 1, ToTxNum: rows}}
	if err := VerifyOfflineHistoryRepairBoundaryCopies(ctx, dir, plan, copied); err != nil {
		t.Fatal(fmt.Errorf("partitioned copy lost ordered rows: %w", err))
	}
}
