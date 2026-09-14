package state

import (
	"bytes"
	"fmt"
	"maps"
	"math/rand"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestLegacyDelegationMembershipArenaOwnedKeys(t *testing.T) {
	for _, count := range []int{0, 1, 15, 16, 17, 257} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			values := [][]byte{nil, {}, {0}, {0xff, 0, 0x80}, []byte("a"), []byte("ab"), []byte("bc"), []byte("c"), bytes.Repeat([]byte{0x41}, 21), bytes.Repeat([]byte{0x41}, 1025)}
			list := make([][]byte, count)
			for i := range list {
				list[i] = bytes.Clone(values[i%len(values)])
			}
			want := frozenLegacyDelegationMembership(list)
			got := legacyDelegationMembership(list)
			if (got == nil) != (want == nil) || !maps.Equal(got, want) {
				t.Fatal("membership differs from frozen pre-arena builder")
			}
			for _, peer := range values {
				if legacyDelegationContains(list, got, peer) != legacyDelegationContains(list, want, peer) {
					t.Fatalf("contains differs for %x", peer)
				}
			}
			// A map key must never borrow the record's mutable bytes, even when
			// earlier entries are empty or identical and share arena boundaries.
			for i := range list {
				for j := range list[i] {
					list[i][j] ^= 0xff
				}
				list[i] = []byte("replacement")
			}
			if !maps.Equal(got, want) {
				t.Fatal("mutating the source changed owned keys")
			}
			for key := range want {
				delete(got, key)
				delete(want, key)
				break
			}
			if !maps.Equal(got, want) {
				t.Fatal("deleting a key changed another key sharing the arena")
			}
		})
	}
}

func TestLegacyDelegationMembershipArenaRandomOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(20260914))
	for iteration := 0; iteration < 200; iteration++ {
		list := make([][]byte, rng.Intn(400))
		for i := range list {
			if i > 0 && rng.Intn(3) == 0 {
				list[i] = list[rng.Intn(i)]
				continue
			}
			list[i] = make([]byte, rng.Intn(130))
			_, _ = rng.Read(list[i])
		}
		want, got := frozenLegacyDelegationMembership(list), legacyDelegationMembership(list)
		if (got == nil) != (want == nil) || !maps.Equal(got, want) {
			t.Fatalf("iteration %d: set differs", iteration)
		}
	}
}

func TestLegacyDelegationMembershipArenaDirectOversizeFallback(t *testing.T) {
	// Production complete-row admission rejects this before constructing a
	// table. A direct call still uses the original builder instead of requesting
	// an oversized/overflowed contiguous arena. Aliases keep input size small.
	peer := bytes.Repeat([]byte{0x41}, legacyDelegationCacheBytes/legacyDelegationSetMinimum+1)
	list := make([][]byte, legacyDelegationSetMinimum)
	for i := range list {
		list[i] = peer
	}
	set := legacyDelegationMembership(list)
	if len(set) != 1 || !legacyDelegationContains(nil, set, peer) {
		t.Fatal("direct oversized call lost duplicate membership")
	}
	peer[0] = 0x42
	if legacyDelegationContains(nil, set, peer) {
		t.Fatal("oversized fallback borrowed mutable bytes")
	}
}

func TestLegacyDelegationMembershipArenaEntryBudget(t *testing.T) {
	for _, degree := range []int{15, 16, 10_000, 250_000} {
		t.Run(fmt.Sprint(degree), func(t *testing.T) {
			peer := bytes.Repeat([]byte{0x41}, 21)
			list := make([][]byte, degree)
			for i := range list {
				list[i] = peer
			}
			record := &corepb.DelegatedResourceAccountIndex{Account: []byte("owner"), ToAccounts: list}
			got, want := newLegacyDelegationEntry([]byte("key"), record), frozenLegacyDelegationEntry([]byte("key"), record)
			if got.charge != want.charge || got.key != want.key || got.record != record || (got.toSet == nil) != (want.toSet == nil) || !maps.Equal(got.toSet, want.toSet) {
				t.Fatal("arena changed charge, ownership, threshold or membership")
			}
			cache := &legacyDelegationCache{entries: make(map[string]*legacyDelegationEntry)}
			cache.admit(got)
			assertDelegationCacheBounds(t, cache)
			if got.charge > legacyDelegationCacheBytes && (got.toSet != nil || cache.entries[got.key] != nil) {
				t.Fatal("oversized row built or retained membership")
			}
		})
	}
}
