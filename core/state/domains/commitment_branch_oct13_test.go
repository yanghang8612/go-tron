package domains

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Independent append-format and streaming Keccak oracles preserve the existing
// wire encoding and preimage across sparse masks and both leaf representations.
func TestBranchEncodingAndOneShotHashEquivalence(t *testing.T) {
	for mask := 0; mask < 1<<16; mask += 17 {
		var branch BranchData
		expected := []byte{byte(mask >> 8), byte(mask)}
		preimage := []byte{1}
		for n := uint8(0); n < 16; n++ {
			if mask&(1<<n) == 0 {
				continue
			}
			hash := common.Hash{n + 1, byte(mask)}
			switch n % 3 {
			case 0:
				branch.SetHashChild(n, hash)
				expected = append(expected, kindHash)
			case 1:
				key := bytes.Repeat([]byte{n}, 129)
				branch.SetLeafChild(n, key, hash)
				expected = append(expected, kindLeaf)
				expected = binary.AppendUvarint(expected, uint64(len(key)))
				expected = append(expected, key...)
			case 2:
				path := bytes.Repeat([]byte{n}, 32)
				branch.setLeafChildPath(n, path, hash)
				expected = append(expected, kindLeafPath)
				expected = append(expected, path...)
			}
			expected = append(expected, hash[:]...)
			preimage = append(preimage, n)
			preimage = append(preimage, hash[:]...)
		}
		for _, capacity := range []int{0, len(expected) + 3} {
			prefix := make([]byte, 3, 3+capacity)
			copy(prefix, []byte{7, 8, 9})
			got := branch.EncodeTo(prefix)
			if !bytes.Equal(got[:3], []byte{7, 8, 9}) || !bytes.Equal(got[3:], expected) {
				t.Fatalf("mask %x encoding mismatch", mask)
			}
		}
		if got, want := branch.nodeHash(), common.Hash(crypto.Keccak256Hash(preimage)); got != want {
			t.Fatalf("mask %x hash mismatch", mask)
		}
	}
}

func TestBranchPreimageHashDoesNotAllocate(t *testing.T) {
	var branch BranchData
	for n := uint8(0); n < 16; n++ {
		branch.SetHashChild(n, common.Hash{n + 1})
	}
	h := borrowKeccak()
	defer returnKeccak(h)
	if n := testing.AllocsPerRun(1000, func() { _ = branch.nodeHashWith(h) }); n != 0 {
		t.Fatalf("node hash allocations %v", n)
	}
}

type composeFailureDB struct {
	CommitmentDB
	err    error
	writes int
}

func (d *composeFailureDB) Put(key, value []byte) error { d.writes++; return d.err }
func (d *composeFailureDB) Delete(key []byte) error     { d.writes++; return d.err }

func TestPartitionComposeFlushFailurePreventsRootPublication(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(fmt.Sprint(deleting), func(t *testing.T) {
			injected := errors.New("depth-one flush failed")
			disk := rawdb.NewMemoryDatabase()
			defer disk.Close()
			db := &composeFailureDB{CommitmentDB: disk, err: injected}
			job := &orderedCommitmentJob{store: newRawdbBranchStore(db), partitions: new(commitmentPartitionJob), stats: new(commitmentFoldStats), result: make(chan OrderedCommitmentResult, 1), completed: make(chan struct{})}
			job.changed.Store(true)
			job.partitions.changed[0] = true
			if !deleting {
				job.partitions.roots[0].SetHashChild(0, common.Hash{1})
				job.partitions.roots[0].SetHashChild(1, common.Hash{2})
			}
			p := new(OrderedCommitmentPipeline)
			p.inflight.Store(1)
			p.finishJob(job)
			result := <-job.result
			if !errors.Is(result.Err, injected) || result.Root != (common.Hash{}) || p.failed.Load() == nil || db.writes != 1 {
				t.Fatalf("failed compose published or continued: %+v writes %d", result, db.writes)
			}
		})
	}
}
