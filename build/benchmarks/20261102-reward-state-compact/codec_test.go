package main

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func fixture(seed int64) *corepb.Account {
	r := rand.New(rand.NewSource(seed))
	a := &corepb.Account{
		Address: []byte{0x41, 3, 2, 1}, Balance: r.Int63(), NetUsage: -seed,
		Asset:            map[string]int64{"zero": 0, "negative": -7, "positive": 50},
		AssetV2:          map[string]int64{"1000001": 82739, "1000002": math.MaxInt64},
		Frozen:           []*corepb.Account_Frozen{{FrozenBalance: 5000000, ExpireTime: 10000000000}, {}},
		Votes:            []*corepb.Vote{{VoteAddress: []byte{0x41, 5}, VoteCount: 3}, {VoteCount: -1}},
		OwnerPermission:  &corepb.Permission{Threshold: 1, Keys: []*corepb.Key{{Address: []byte{0x41, 3, 2, 1}, Weight: 1}}},
		ActivePermission: []*corepb.Permission{{Id: 2, PermissionName: "active", Operations: []byte{1, 2, 3}, Threshold: 2}},
		AccountResource:  &corepb.Account_AccountResource{EnergyUsage: seed, FrozenBalanceForEnergy: &corepb.Account_Frozen{}, EnergyWindowOptimized: true},
		FrozenV2:         []*corepb.Account_FreezeV2{{Type: corepb.ResourceCode_ENERGY, Amount: 888}},
		UnfrozenV2:       []*corepb.Account_UnFreezeV2{{Type: corepb.ResourceCode_TRON_POWER, UnfreezeAmount: -1, UnfreezeExpireTime: math.MinInt64}},
		AssetOptimized:   true,
	}
	a.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	a.Votes[0].ProtoReflect().SetUnknown([]byte{0xa8, 0x06, 0x01})
	a.OwnerPermission.Keys[0].ProtoReflect().SetUnknown([]byte{0xb0, 0x06, 0x01})
	return a
}

func TestCompactSnapshotExactNativeRoundTrip(t *testing.T) {
	for i := int64(0); i < 100; i++ {
		in := fixture(i)
		if i == 0 {
			in = &corepb.Account{AccountResource: &corepb.Account_AccountResource{}, OwnerPermission: &corepb.Permission{}}
		}
		native, e := statecodec.Marshal(in)
		if e != nil {
			t.Fatal(e)
		}
		compact, e := encodeCompactSnapshot(native)
		if e != nil {
			t.Fatal(e)
		}
		back, e := decodeCompactSnapshot(compact)
		if e != nil || !bytes.Equal(native, back) {
			t.Fatalf("seed %d mismatch: %v", i, e)
		}
		var out corepb.Account
		if e := statecodec.Unmarshal(back, &out); e != nil || !proto.Equal(in, &out) {
			t.Fatalf("seed %d logical mismatch: %v", i, e)
		}
		for k := 0; k < len(compact); k++ {
			if _, e := decodeCompactSnapshot(compact[:k]); e == nil {
				t.Fatalf("seed %d truncation %d accepted", i, k)
			}
		}
		if _, e := decodeCompactSnapshot(append(compact, 0)); e == nil {
			t.Fatal("trailing data accepted")
		}
	}
}

func TestCompactSnapshotRejectsInvalidMetadata(t *testing.T) {
	for _, body := range [][]byte{
		{0x80, 0, 0},                         // overlong field count
		{1, 0, 0},                            // duplicate/delta zero
		{1, 127, 0},                          // undefined field
		{255, 1},                             // excessive field count
		{1, 2, 128},                          // truncated enum
		{1, 2, 0xff, 0xff, 0xff, 0xff, 0x7f}, // enum exceeds signed32
	} {
		if _, e := decodeCompactSnapshot(append(append([]byte(nil), rewardMagic...), body...)); e == nil {
			t.Fatalf("accepted %x", body)
		}
	}
	// Map keys remain in the old native byte order, so a corrupted reorder
	// cannot silently recanonicalize the value participating in MuHash.
	b := append(append([]byte(nil), rewardMagic...), 1, 6, 2)
	b = appendBlob(b, []byte("z"))
	b = binary.AppendUvarint(b, 2)
	b = appendBlob(b, []byte("a"))
	b = binary.AppendUvarint(b, 4)
	b = append(b, 0)
	if _, e := decodeCompactSnapshot(b); e == nil {
		t.Fatal("unordered map accepted")
	}
}

func BenchmarkCompactSnapshot(b *testing.B) {
	native, err := statecodec.Marshal(fixture(27))
	if err != nil {
		b.Fatal(err)
	}
	compact, err := encodeCompactSnapshot(native)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(native)), "native-B")
	b.ReportMetric(float64(len(compact)), "compact-B")
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, e := encodeCompactSnapshot(native); e != nil {
				b.Fatal(e)
			}
		}
	})
	b.Run("decode-to-native", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, e := decodeCompactSnapshot(compact); e != nil {
				b.Fatal(e)
			}
		}
	})
}
