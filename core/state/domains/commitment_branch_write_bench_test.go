package domains

import (
	"bytes"
	"math/bits"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// commitmentDBWithoutOwnedValue preserves the normal CommitmentDB surface but
// hides blockbuffer's optional owned-value method for a before/after benchmark.
type commitmentDBWithoutOwnedValue struct{ CommitmentDB }

var benchmarkDecodedBranch BranchData
var benchmarkEncodedBranch []byte
var benchmarkBranchBits uint32
var benchmarkBranchSize int

func BenchmarkOpsBufReturn(b *testing.B) {
	key := bytes.Repeat([]byte{0xab}, 64)
	for _, size := range []int{16, 64, 256, 1024} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				buf := borrowOpsBuf(size)
				for i := range *buf {
					(*buf)[i].key = key
				}
				returnOpsBuf(buf)
			}
		})
	}
}

func BenchmarkBranchDataEncodeToLayout(b *testing.B) {
	for _, children := range []int{1, 4, 16} {
		b.Run(strconv.Itoa(children), func(b *testing.B) {
			var branch BranchData
			for nibble := 0; nibble < children; nibble++ {
				branch.SetHashChild(uint8(nibble), common.Hash{byte(nibble + 1)})
			}
			childBits, size := branch.encodingLayout()
			buf := make([]byte, 0, size)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				benchmarkEncodedBranch = branch.encodeToLayout(buf[:0], childBits, size)
			}
		})
	}
}

func BenchmarkBranchDataEncodingLayout(b *testing.B) {
	for _, children := range []int{1, 4, 16} {
		var branch BranchData
		for nibble := 0; nibble < children; nibble++ {
			hash := common.Hash{byte(nibble + 1)}
			if nibble&3 == 0 {
				branch.SetLeafChild(uint8(nibble), bytes.Repeat([]byte{byte(nibble + 1)}, 32+nibble), hash)
			} else {
				branch.SetHashChild(uint8(nibble), hash)
			}
		}
		for _, tc := range []struct {
			name string
			fn   func(*BranchData) (uint32, int)
		}{
			{name: "scan", fn: benchmarkBranchEncodingLayoutScan},
			{name: "kind-mask", fn: (*BranchData).encodingLayout},
		} {
			b.Run(strconv.Itoa(children)+"/"+tc.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					benchmarkBranchBits, benchmarkBranchSize = tc.fn(&branch)
				}
			})
		}
	}
}

func benchmarkBranchEncodingLayoutScan(branch *BranchData) (uint32, int) {
	childBits := atomic.LoadUint32(&branch.childMask)
	mask := uint16(childBits)
	size := 2
	for remaining := mask; remaining != 0; remaining &= remaining - 1 {
		i := uint8(bits.TrailingZeros16(remaining))
		child := &branch.children[i]
		size++
		if branch.childKindAt(i) == kindHash {
			size += common.HashLength
		} else {
			size += uvarintEncodedLen(uint64(len(child.leafKey))) + len(child.leafKey) + common.HashLength
		}
	}
	return childBits, size
}

func BenchmarkBranchPoolReturnLeafReferences(b *testing.B) {
	var template BranchData
	for nibble := uint8(0); nibble < 16; nibble++ {
		var hash common.Hash
		hash[0] = nibble + 1
		if nibble&1 == 0 {
			template.SetLeafChild(nibble, []byte{nibble, nibble + 1}, hash)
		} else {
			template.SetHashChild(nibble, hash)
		}
	}
	branch := borrowBranch()
	b.Cleanup(func() { returnBranch(branch) })
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		*branch = template
		returnBranch(branch)
		branch = borrowBranch()
	}
}

func BenchmarkDecodeBranchDataIntoCopiedLeafKeys(b *testing.B) {
	for _, leaves := range []int{1, 4, 16} {
		b.Run(strconv.Itoa(leaves), func(b *testing.B) {
			var branch BranchData
			for nibble := 0; nibble < leaves; nibble++ {
				key := bytes.Repeat([]byte{byte(nibble + 1)}, 64+nibble)
				branch.SetLeafChild(uint8(nibble), key, common.Hash{byte(nibble + 1)})
			}
			encoded := branch.Encode()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var decoded BranchData
				if err := DecodeBranchDataInto(encoded, &decoded); err != nil {
					b.Fatal(err)
				}
				benchmarkDecodedBranch = decoded
			}
		})
	}
}

func BenchmarkDecodeBranchDataIntoFoldArena(b *testing.B) {
	var branch BranchData
	for nibble := uint8(0); nibble < 16; nibble++ {
		branch.SetLeafChild(nibble, bytes.Repeat([]byte{nibble + 1}, 64+int(nibble)), common.Hash{nibble + 1})
	}
	encoded := branch.Encode()
	var decoded BranchData
	var arena []byte
	if err := decodeBranchDataIntoArena(encoded, &decoded, &arena); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		arena = arena[:0]
		if err := decodeBranchDataIntoArena(encoded, &decoded, &arena); err != nil {
			b.Fatal(err)
		}
	}
	benchmarkDecodedBranch = decoded
}

func BenchmarkDecodeBranchDataIntoNoCopy(b *testing.B) {
	var branch BranchData
	for nibble := uint8(0); nibble < 16; nibble++ {
		branch.SetLeafChild(nibble, bytes.Repeat([]byte{nibble + 1}, 64+int(nibble)), common.Hash{nibble + 1})
	}
	encoded := branch.Encode()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var decoded BranchData
		if err := decodeBranchDataIntoNoCopy(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
		benchmarkDecodedBranch = decoded
	}
}

func BenchmarkDecodeBranchDataIntoNoCopyReuse(b *testing.B) {
	for _, children := range []int{1, 4, 8, 10, 12, 16} {
		b.Run(strconv.Itoa(children), func(b *testing.B) {
			var branch BranchData
			for nibble := 0; nibble < children; nibble++ {
				hash := common.Hash{byte(nibble + 1)}
				if nibble&1 == 0 {
					branch.SetHashChild(uint8(nibble), hash)
				} else {
					branch.SetLeafChild(uint8(nibble), []byte{byte(nibble + 1)}, hash)
				}
			}
			encoded := branch.Encode()
			var decoded BranchData
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := decodeBranchDataIntoNoCopy(encoded, &decoded); err != nil {
					b.Fatal(err)
				}
			}
			benchmarkDecodedBranch = decoded
		})
	}
}

func BenchmarkRawdbBranchStorePutBranch(b *testing.B) {
	var branch BranchData
	for nibble := uint8(0); nibble < 16; nibble++ {
		var hash common.Hash
		hash[0] = nibble + 1
		branch.SetHashChild(nibble, hash)
	}
	prefix := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	for _, tc := range []struct {
		name      string
		hideOwned bool
	}{
		{name: "copying", hideOwned: true},
		{name: "owned", hideOwned: false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			buffer := blockbuffer.New(rawdb.NewMemoryDatabase())
			buffer.BeginBlock(common.Hash{1}, 1)
			handle, ok := buffer.NewestInflight()
			if !ok {
				b.Fatal("missing in-flight layer")
			}
			var db CommitmentDB = buffer.ViewLayer(handle)
			if tc.hideOwned {
				db = commitmentDBWithoutOwnedValue{CommitmentDB: db}
			}
			store := newRawdbBranchStore(db)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := store.PutBranch(prefix, branch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRawdbBranchStoreGetBranchInto(b *testing.B) {
	buffer := blockbuffer.New(rawdb.NewMemoryDatabase())
	buffer.BeginBlock(common.Hash{1}, 1)
	handle, ok := buffer.NewestInflight()
	if !ok {
		b.Fatal("missing in-flight layer")
	}
	store := newRawdbBranchStore(buffer.ViewLayer(handle))
	prefix := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	var branch BranchData
	for nibble := uint8(0); nibble < 16; nibble++ {
		var hash common.Hash
		hash[0] = nibble + 1
		branch.SetHashChild(nibble, hash)
	}
	if err := store.PutBranch(prefix, branch); err != nil {
		b.Fatal(err)
	}

	dst := new(BranchData)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		found, err := store.GetBranchInto(prefix, dst)
		if err != nil || !found {
			b.Fatalf("GetBranchInto = found %v err %v", found, err)
		}
	}
}

func BenchmarkRawdbBranchStorePutBranches(b *testing.B) {
	for _, count := range []int{16, 32, 64, 128, 256, 1024} {
		keys := make([]string, count)
		branches := make(map[string]*BranchData, count)
		for i := range keys {
			key := string([]byte{byte(i >> 8), byte(i)})
			branch := new(BranchData)
			for nibble := uint8(0); nibble < 16; nibble++ {
				var hash common.Hash
				hash[0] = nibble + 1
				hash[1] = byte(i)
				branch.SetHashChild(nibble, hash)
			}
			keys[i] = key
			branches[key] = branch
		}

		for _, tc := range []struct {
			name        string
			sharedArena bool
		}{
			{name: "separate-arena", sharedArena: false},
			{name: "shared-arena", sharedArena: true},
		} {
			b.Run(strconv.Itoa(count)+"/"+tc.name, func(b *testing.B) {
				buffer := blockbuffer.New(rawdb.NewMemoryDatabase())
				buffer.BeginBlock(common.Hash{1}, 1)
				handle, ok := buffer.NewestInflight()
				if !ok {
					b.Fatal("missing in-flight layer")
				}
				store := newRawdbBranchStore(buffer.ViewLayer(handle))
				store.ownedBatchArena = tc.sharedArena
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if err := store.putBranches(keys, branches, 1); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkRawdbBranchStoreMultiBlock includes the layer map allocation that
// the same-layer microbenchmark above amortizes away. Sibling sizes are fixed
// before timing; each iteration writes and commits four distinct layers.
func BenchmarkRawdbBranchStoreMultiBlock(b *testing.B) {
	for _, tc := range []struct {
		name      string
		counts    []int
		opWeights []int
	}{
		{name: "one-sibling", counts: []int{256}},
		{name: "two-siblings", counts: []int{128, 128}},
		{name: "uniform-16", counts: []int{32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32}},
		{name: "skewed-16", counts: []int{256, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16}},
		{name: "skewed-last-16", counts: []int{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 256}},
		// Equal op counts can still yield unequal branch counts when prefix
		// sharing differs. This is a deliberate negative control for the hint.
		{name: "equal-ops-skewed-branches", counts: []int{256, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16}, opWeights: []int{32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32}},
	} {
		for _, adaptive := range []bool{false, true} {
			name := "baseline"
			if adaptive {
				name = "adaptive"
			}
			b.Run(tc.name+"/"+name, func(b *testing.B) {
				weights := tc.opWeights
				if weights == nil {
					weights = tc.counts
				}
				totalCount := 0
				for _, count := range weights {
					totalCount += count
				}
				keys := make([][]string, len(tc.counts))
				branches := make([]map[string]*BranchData, len(tc.counts))
				for sibling, count := range tc.counts {
					keys[sibling] = make([]string, count)
					branches[sibling] = make(map[string]*BranchData, count)
					for i := 0; i < count; i++ {
						key := string([]byte{byte(sibling), byte(i >> 8), byte(i)})
						branch := new(BranchData)
						for nibble := uint8(0); nibble < 12; nibble++ {
							branch.SetHashChild(nibble, common.Hash{byte(sibling + 1), byte(i), nibble})
						}
						keys[sibling][i] = key
						branches[sibling][key] = branch
					}
				}
				buffer := blockbuffer.New(rawdb.NewMemoryDatabase())
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					for block := 1; block <= 4; block++ {
						buffer.BeginBlock(common.Hash{byte(block)}, uint64(block))
						h, ok := buffer.NewestInflight()
						if !ok {
							b.Fatal("missing in-flight layer")
						}
						store := newRawdbBranchStore(buffer.ViewLayer(h))
						for sibling := range keys {
							hint := len(keys)
							if adaptive {
								hint = siblingBatchReserveHint(len(keys), totalCount, weights[sibling])
							}
							if err := store.putBranchesWithReserveHint(keys[sibling], branches[sibling], len(keys), hint); err != nil {
								b.Fatal(err)
							}
						}
						buffer.CommitBlock()
					}
					buffer.Discard()
				}
			})
		}
	}
}
