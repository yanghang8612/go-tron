package state

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestReadPagedDelegationOwnedSparseLists(t *testing.T) {
	for _, holesOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "mixed", true: "empty-pages"}[holesOnly], func(t *testing.T) {
			account := []byte("lookup account")
			want := &corepb.DelegatedResourceAccountIndex{Account: []byte("metadata account"), Timestamp: -99}
			want.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1})
			rows := make(map[string][]byte)
			var readOrder []string
			anchor := string(rawdb.DrAccountIndexLegacyStateKey(account))
			readOrder = append(readOrder, anchor)
			var pageLengths [2][]int
			for direction, incoming := range []bool{true, false} {
				for i, count := range []int{128, 0, 1, 0, 128, 2, 0} {
					if holesOnly {
						count = 0
					}
					list := make([][]byte, count)
					for j := range list {
						// Repeated arbitrary bytes, including empty addresses, are
						// valid persisted list contents and must retain their order.
						list[j] = bytes.Clone([][]byte{nil, {}, {0xff, 0}, []byte("duplicate"), []byte("duplicate")}[j%5])
					}
					if incoming {
						want.FromAccounts = append(want.FromAccounts, list...)
					} else {
						want.ToAccounts = append(want.ToAccounts, list...)
					}
					key := string(rawdb.DrAccountIndexLegacyPageStateKey(account, incoming, uint64(i)))
					encoded, err := statecodec.MarshalDelegationPage(list)
					if err != nil {
						t.Fatal(err)
					}
					rows[key] = encoded
					readOrder = append(readOrder, key)
					pageLengths[direction] = append(pageLengths[direction], count)
				}
			}
			var err error
			rows[anchor], err = statecodec.MarshalDelegationDescriptor(want, 7, 7)
			if err != nil {
				t.Fatal(err)
			}
			var reads []string
			var scratch []byte
			got, pages, exists, err := readPagedDelegationIndex(account, func(key []byte) ([]byte, bool, error) {
				reads = append(reads, string(key))
				scratch = append(scratch[:0], rows[string(key)]...)
				return scratch, true, nil
			})
			clear(scratch)
			if err != nil || !exists || !proto.Equal(got, want) {
				t.Fatalf("decoded sparse row differs: exists=%v err=%v", exists, err)
			}
			if !reflect.DeepEqual(reads, readOrder) {
				t.Fatal("page reads were repeated or reordered")
			}
			for _, list := range [][][]byte{got.FromAccounts, got.ToAccounts} {
				if len(list) == 0 {
					if list != nil {
						t.Fatal("empty aggregate retained header storage")
					}
				} else if cap(list) != len(list)+1 {
					t.Fatal("aggregate did not retain exactly one spare header")
				}
			}
			for direction, dirPages := range [][][][]byte{pages.from, pages.to} {
				if len(dirPages) != len(pageLengths[direction]) {
					t.Fatal("empty pages were removed")
				}
				for i, page := range dirPages {
					if len(page) != pageLengths[direction][i] {
						t.Fatal("page entries moved between pages")
					}
					if len(page) != 0 {
						removeDelegationAccount(page, []byte("duplicate"))
					}
				}
			}
			if !proto.Equal(got, want) {
				t.Fatal("compacting page headers changed aggregate headers")
			}
		})
	}
}

func TestReadPagedDelegationFailureReadOrder(t *testing.T) {
	account := []byte("account")
	anchor := rawdb.DrAccountIndexLegacyStateKey(account)
	first := rawdb.DrAccountIndexLegacyPageStateKey(account, true, 0)
	failure := rawdb.DrAccountIndexLegacyPageStateKey(account, true, 1)
	valid, err := statecodec.MarshalDelegationPage([][]byte{[]byte("peer")})
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("page read failure")
	for _, scenario := range []string{"missing", "corrupt", "read-error", "huge-count"} {
		t.Run(scenario, func(t *testing.T) {
			count := uint64(3)
			if scenario == "huge-count" {
				// Valid descriptor count, intentionally impossible to reserve.
				count = uint64(^uint(0)>>1)/24 - 1
			}
			descriptor, err := statecodec.MarshalDelegationDescriptor(&corepb.DelegatedResourceAccountIndex{}, count, 1)
			if err != nil {
				t.Fatal(err)
			}
			var reads []string
			got, _, exists, err := readPagedDelegationIndex(account, func(key []byte) ([]byte, bool, error) {
				reads = append(reads, string(key))
				if bytes.Equal(key, anchor) {
					return descriptor, true, nil
				}
				if bytes.Equal(key, first) {
					return valid, true, nil
				}
				if !bytes.Equal(key, failure) {
					t.Fatalf("unexpected read %x", key)
				}
				switch scenario {
				case "corrupt":
					return []byte("DPP1\x01\x80"), true, nil
				case "read-error":
					return nil, false, sentinel
				default:
					return nil, false, nil
				}
			})
			if got != nil || !exists || err == nil {
				t.Fatalf("failure accepted: exists=%v err=%v", exists, err)
			}
			if scenario == "read-error" && !errors.Is(err, sentinel) {
				t.Fatal("read error identity changed")
			}
			if scenario == "corrupt" && !strings.Contains(err.Error(), "decode delegation page 1: invalid delegation page entry length") {
				t.Fatalf("decode error changed: %v", err)
			}
			if !reflect.DeepEqual(reads, []string{string(anchor), string(first), string(failure)}) {
				t.Fatal("read continued after failure or repeated pages")
			}
		})
	}
}
