package core

import (
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
)

func TestDynPropsCommitCopyPoolReusesOnlyReplacedSnapshot(t *testing.T) {
	bc := new(BlockChain)
	source := state.NewDynamicProperties()
	source.Set("commit_copy_pool", 1)

	first := bc.copyDynPropsForCommit(source)
	bc.storeReusableDynPropsCache(first)

	source.Set("commit_copy_pool", 2)
	second := bc.copyDynPropsForCommit(source)
	if second == first {
		t.Fatal("copy reused the currently published snapshot")
	}
	bc.storeReusableDynPropsCache(second)

	source.Set("commit_copy_pool", 3)
	third := bc.copyDynPropsForCommit(source)
	if third != first {
		t.Fatal("copy did not reuse the replaced snapshot")
	}
	if got := bc.BufferedDPInt64("commit_copy_pool"); got != 2 {
		t.Fatalf("published snapshot changed while recycled copy was filled: got %d, want 2", got)
	}
	if got, ok := third.Get("commit_copy_pool"); !ok || got != 3 {
		t.Fatalf("recycled copy value = %d ok=%v, want 3/true", got, ok)
	}
}

func TestDynPropsCommitCopyPoolDoesNotRecycleExternalSnapshot(t *testing.T) {
	bc := new(BlockChain)
	external := state.NewDynamicProperties()
	bc.storeDynPropsCache(external)

	owned := bc.copyDynPropsForCommit(external)
	bc.storeReusableDynPropsCache(owned)
	next := bc.copyDynPropsForCommit(external)
	if next == external {
		t.Fatal("copy pool reused an externally owned snapshot")
	}
}

func TestDynPropsCommitCopyPoolConcurrentReaders(t *testing.T) {
	bc := new(BlockChain)
	source := state.NewDynamicProperties()
	bc.storeReusableDynPropsCache(bc.copyDynPropsForCommit(source))

	const readers = 4
	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			for range iterations {
				_ = bc.BufferedDPInt64("latest_block_header_number")
				_ = bc.cachedDynProps()
			}
		}()
	}
	for i := range iterations {
		source.SetLatestBlockHeaderNumber(int64(i + 1))
		bc.storeReusableDynPropsCache(bc.copyDynPropsForCommit(source))
	}
	wg.Wait()
}
