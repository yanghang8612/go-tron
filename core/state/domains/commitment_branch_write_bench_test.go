package domains

import (
	"bytes"
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
