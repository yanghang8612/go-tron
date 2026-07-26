package domains

import (
	"bytes"
	"math/bits"
	"strconv"
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
var benchmarkBranchMask uint16
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
			mask, size := branch.encodingLayout()
			buf := make([]byte, 0, size)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				benchmarkEncodedBranch = branch.encodeToLayout(buf[:0], mask, size)
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
			fn   func(*BranchData) (uint16, int)
		}{
			{name: "scan", fn: benchmarkBranchEncodingLayoutScan},
			{name: "kind-mask", fn: (*BranchData).encodingLayout},
		} {
			b.Run(strconv.Itoa(children)+"/"+tc.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					benchmarkBranchMask, benchmarkBranchSize = tc.fn(&branch)
				}
			})
		}
	}
}

func benchmarkBranchEncodingLayoutScan(branch *BranchData) (uint16, int) {
	mask := branch.presentMask()
	size := 2
	for remaining := mask; remaining != 0; remaining &= remaining - 1 {
		i := uint8(bits.TrailingZeros16(remaining))
		child := &branch.children[i]
		size++
		if child.kind == kindHash {
			size += common.HashLength
		} else {
			size += uvarintEncodedLen(uint64(len(child.leafKey))) + len(child.leafKey) + common.HashLength
		}
	}
	return mask, size
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

func BenchmarkRawdbBranchStorePutBranchesSorted(b *testing.B) {
	for _, count := range []int{16, 32, 64, 128, 256, 1024} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
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

			buffer := blockbuffer.New(rawdb.NewMemoryDatabase())
			buffer.BeginBlock(common.Hash{1}, 1)
			handle, ok := buffer.NewestInflight()
			if !ok {
				b.Fatal("missing in-flight layer")
			}
			store := newRawdbBranchStore(buffer.ViewLayer(handle))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := store.putBranchesSorted(keys, branches, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
