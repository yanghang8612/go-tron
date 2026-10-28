package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestSystemHistoryRoutingSelectedMatchesFullAndDoesNotPoisonCache(t *testing.T) {
	dir, m, queries := storageRoutingFixture(t)
	m = addRoutingFixture(t, dir, m, 3)
	ref := mustRoutingRef(t, m)
	raw, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	full, err := decodeSystemHistoryRouting(raw, ref)
	if err != nil {
		t.Fatal(err)
	}
	selectedKey := stateDomainChangeBinaryAccessorLookupKey(queries[0].FlatDomain, queries[0].Owner, queries[0].Generation, queries[0].Domain, queries[0].Key)
	selected, err := decodeHistoryRoutingFamilySelected(context.Background(), raw, ref, false, map[string]bool{string(normalizedHistoryRoutingKey(selectedKey)): true})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected.keys) != 1 || !reflect.DeepEqual(selected.sources, full.sources) || !reflect.DeepEqual(selected.columns, full.columns) {
		t.Fatal("unbounded selection or changed coverage")
	}
	for col := range full.sources {
		if selected.mayContain(col, selectedKey) != full.mayContain(col, selectedKey) {
			t.Fatal("membership mismatch", col)
		}
	}
	other := stateDomainChangeBinaryAccessorLookupKey(queries[4].FlatDomain, queries[4].Owner, queries[4].Generation, queries[4].Domain, queries[4].Key)
	for col := range full.sources {
		if !selected.mayContain(col, other) {
			t.Fatal("unselected key became an absence proof")
		}
	}
	sharedSystemHistoryRouting.Lock()
	sharedSystemHistoryRouting.value = nil
	sharedSystemHistoryRouting.Unlock()
	scoped, err := loadSystemHistoryRoutingForKeys(context.Background(), dir, m, [][]byte{selectedKey})
	if err != nil {
		t.Fatal(err)
	}
	if len(scoped.keys) != 1 {
		t.Fatal("scoped reader retained unrelated keys", len(scoped.keys))
	}
	// A later request for a different generation must receive a complete answer,
	// including when the first request proved that generation absent.
	all, err := loadSystemHistoryRouting(context.Background(), dir, m)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(all.keys, full.keys) {
		t.Fatal("request-local view entered the shared full-object cache")
	}
	// Physical authentication remains before a shared decoded cache hit.
	raw[len(raw)-1] ^= 1
	if err = os.WriteFile(filepath.Join(dir, ref.Path), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = loadSystemHistoryRoutingForKeys(context.Background(), dir, m, [][]byte{selectedKey}); err == nil {
		t.Fatal("selected reader bypassed changed physical bytes")
	}
}

func selectedRoutingBytes(keys uint32) ([]byte, SegmentRef) {
	const sources = 10
	data := make([]byte, systemHistoryRoutingHeaderSize)
	copy(data, systemHistoryRoutingMagic[:])
	binary.BigEndian.PutUint32(data[8:12], 2)
	binary.BigEndian.PutUint32(data[12:16], sources)
	binary.BigEndian.PutUint32(data[16:20], keys)
	binary.BigEndian.PutUint64(data[24:32], 1)
	binary.BigEndian.PutUint64(data[32:40], 10)
	for i := 0; i < sources; i++ {
		var id [32]byte
		id[31] = byte(i)
		data = append(data, id[:]...)
	}
	for i := uint32(0); i < keys; i++ {
		var key [31]byte
		key[0] = byte(rawdb.StateFlatDomainKVLatest)
		binary.BigEndian.PutUint64(key[13:21], uint64(i))
		binary.BigEndian.PutUint16(key[29:31], uint16(kvdomains.ContractStorage))
		data = binary.BigEndian.AppendUint16(data, 31)
		data = append(data, key[:]...)
		data = append(data, 0, 0, 0, 0, 2, 1, 0)
	}
	return data, SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentSystemHistoryRouting, FromTxNum: 1, ToTxNum: 10, Rows: uint64(keys), Size: uint64(len(data)), Checksum: fmt.Sprintf("sha256:%x", sha256.Sum256(data))}
}

func TestSystemHistoryRoutingSelectedValidatesUnretainedRowsAndBoundsAllocation(t *testing.T) {
	raw, ref := selectedRoutingBytes(60000)
	first := raw[systemHistoryRoutingHeaderSize+10*32+2 : systemHistoryRoutingHeaderSize+10*32+33]
	wanted := map[string]bool{string(first): true}
	full, err := decodeSystemHistoryRouting(raw, ref)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeHistoryRoutingFamilySelected(context.Background(), raw, ref, false, wanted)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.keys) != 1 || !bytes.Equal(got.keys[string(first)], full.keys[string(first)]) {
		t.Fatal("incorrect selection")
	}
	fullAllocs := testing.AllocsPerRun(2, func() {
		if _, err := decodeHistoryRoutingFamilySelected(context.Background(), raw, ref, false, nil); err != nil {
			t.Fatal(err)
		}
	})
	selectedAllocs := testing.AllocsPerRun(2, func() {
		if _, err := decodeHistoryRoutingFamilySelected(context.Background(), raw, ref, false, wanted); err != nil {
			t.Fatal(err)
		}
	})
	if selectedAllocs*20 >= fullAllocs {
		t.Fatal("unretained row allocation scales with file keys", selectedAllocs, fullAllocs)
	}
	// Corrupt a membership which the requested key will never use. Whole-file
	// structural validation, rather than just selected-key validation, is required.
	raw[len(raw)-2] = 0
	if _, err := decodeHistoryRoutingFamilySelected(context.Background(), raw, ref, false, wanted); err == nil {
		t.Fatal("invalid unselected membership accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := decodeHistoryRoutingFamilySelected(ctx, raw, ref, false, wanted); err != context.Canceled {
		t.Fatal(err)
	}
}

func BenchmarkSystemHistoryRoutingSelected(b *testing.B) {
	raw, ref := selectedRoutingBytes(100000)
	key := string(raw[systemHistoryRoutingHeaderSize+10*32+2 : systemHistoryRoutingHeaderSize+10*32+33])
	wanted := map[string]bool{key: true}
	for _, mode := range []string{"full", "selected"} {
		b.Run(mode, func(b *testing.B) {
			selection := wanted
			if mode == "full" {
				selection = nil
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for b.Loop() {
				if _, err := decodeHistoryRoutingFamilySelected(context.Background(), raw, ref, false, selection); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
