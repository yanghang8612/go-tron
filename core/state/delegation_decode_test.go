package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestDecodeCanonicalDelegatedResource(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 256; i++ {
		want := rawdb.DelegatedResource{
			FrozenBalanceForBandwidth: int64(r.Uint64()),
			FrozenBalanceForEnergy:    math.MinInt64,
			ExpireTimeForBandwidth:    math.MaxInt64,
		}
		for j := range want.From {
			want.From[j] = byte(i)
			want.To[j] = byte(r.Uint32())
		}
		raw, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var got rawdb.DelegatedResource
		if !decodeCanonicalDelegatedResource(raw, &got) || got != want {
			t.Fatalf("canonical %d decoded as %+v, want %+v", i, got, want)
		}
	}
}

func FuzzDecodeDelegatedResource(f *testing.F) {
	canonical, err := json.Marshal(rawdb.DelegatedResource{ExpireTimeForEnergy: math.MinInt64})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(canonical)
	f.Add(bytes.Replace(canonical, []byte(`,"expire_time_for_energy"`), []byte(`, "expire_time_for_energy"`), 1))
	f.Add(append(bytes.TrimSuffix(bytes.Clone(canonical), []byte("}")), []byte(`,"expire_time_for_energy":null,"from":[42],"from":null}`)...))
	f.Add(bytes.Replace(canonical, []byte(`"expire_time_for_energy":-9223372036854775808`), []byte(`"expire_time_for_energy":-99999999999999999999`), 1))
	for _, value := range []string{
		"",
		`null`,
		`{}`,
		` {"unknown":1,"to":[0,null,255],"FROM":[1,2],"frozen_balance_for_energy":-0} `,
		`{"from":[62],"from":[1,2],"frozen_balance_for_energy":9223372036854775808}`,
		`{"from":[256],"to":[-1],"expire_time_for_bandwidth":"bad"}`,
		`{"from":[01],"expire_time_for_energy":0}`,
		`{"from":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]}`,
	} {
		f.Add([]byte(value))
	}
	f.Add(append(append([]byte(nil), canonical...), '0'))
	f.Fuzz(func(t *testing.T, raw []byte) {
		var want rawdb.DelegatedResource
		wantErr := json.Unmarshal(raw, &want)
		got, gotErr := decodeDelegatedResource(raw)
		if !reflect.DeepEqual(gotErr, wantErr) {
			t.Fatalf("error = %v (%T), want %v (%T)", gotErr, gotErr, wantErr, wantErr)
		}
		if wantErr == nil && (got == nil || *got != want) {
			t.Fatalf("decoded %+v, want %+v", got, want)
		}
	})
}

func TestReadDelegatedResourceV2PreservesStoredJSON(t *testing.T) {
	canonical, err := json.Marshal(rawdb.DelegatedResource{FrozenBalanceForBandwidth: math.MinInt64, ExpireTimeForEnergy: math.MaxInt64})
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		name      string
		raw       []byte
		canonical bool
	}{
		{"canonical_stored_addresses_differ", canonical, true},
		{"short_address", []byte(`{"from":[3,2,1],"to":[255],"frozen_balance_for_bandwidth":-9223372036854775808}`), false},
		{"long_address", []byte(`{"from":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22],"expire_time_for_energy":9223372036854775807}`), false},
		{"malformed", []byte(`{"from":[256]}`), false},
		{"trailing", []byte(`{"from":[1]} null`), false},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			s := newTestStateDB(t)
			from, to := testAddr(0x31), testAddr(0x32)
			s.CreateAccount(from, corepb.AccountType_Normal)
			s.CreateAccount(to, corepb.AccountType_Normal)
			if err := s.writeSystemDelegation(rawdb.DelegatedResourceV2StateKey(from, to, true), fixture.raw); err != nil {
				t.Fatal(err)
			}
			beforeCanonical := delegatedResourceJSONCanonical.Snapshot().Count()
			beforeFallback := delegatedResourceJSONFallback.Snapshot().Count()
			got, exists, err := s.ReadDelegatedResourceV2Strict(from, to, true)
			canonicalCalls := delegatedResourceJSONCanonical.Snapshot().Count() - beforeCanonical
			fallbackCalls := delegatedResourceJSONFallback.Snapshot().Count() - beforeFallback
			if canonicalCalls+fallbackCalls != 1 || (canonicalCalls == 1) != fixture.canonical {
				t.Fatalf("canonical calls=%d fallback calls=%d", canonicalCalls, fallbackCalls)
			}
			var want rawdb.DelegatedResource
			wantErr := json.Unmarshal(fixture.raw, &want)
			if !exists {
				t.Fatal("stored row was treated as absent")
			}
			if wantErr != nil {
				if got != nil || err == nil || err.Error() != fmt.Sprintf("decode delegated resource v2 locked=true: %v", wantErr) ||
					!reflect.DeepEqual(errors.Unwrap(err), wantErr) || s.Error() != nil {
					t.Fatalf("strict result=%v err=%v state error=%v want wrapped %v", got, err, s.Error(), wantErr)
				}
				return
			}
			if err != nil || got == nil || *got != want {
				t.Fatalf("strict result=%v err=%v want %+v", got, err, want)
			}
			if got := s.ReadDelegatedResourceV2(from, to, true); got == nil || *got != want {
				t.Fatalf("consensus result=%v want %+v", got, want)
			}
		})
	}
}

func TestEncodeCanonicalDelegatedResourceMatchesJSONMarshal(t *testing.T) {
	var minBytes, mixedBytes, maxBytes tcommon.Address
	for i := range mixedBytes {
		minBytes[i] = 0
		maxBytes[i] = 255
		mixedBytes[i] = byte((i*37 + 9) & 0xff)
	}
	mixedBytes[0] = 1
	mixedBytes[1] = 9
	mixedBytes[2] = 10
	mixedBytes[3] = 99
	mixedBytes[4] = 100
	cases := []*rawdb.DelegatedResource{
		nil,
		{},
		{
			From:                      minBytes,
			To:                        maxBytes,
			FrozenBalanceForBandwidth: math.MinInt64,
			FrozenBalanceForEnergy:    -1,
			ExpireTimeForBandwidth:    0,
			ExpireTimeForEnergy:       math.MaxInt64,
		},
		{
			From:                      mixedBytes,
			To:                        testAddr(0x42),
			FrozenBalanceForBandwidth: 1_234_567_890_123,
			FrozenBalanceForEnergy:    987_654_321,
			ExpireTimeForBandwidth:    1732700000000,
			ExpireTimeForEnergy:       1732799999999,
		},
	}
	for i, tc := range cases {
		want, err := json.Marshal(tc)
		if err != nil {
			t.Fatal(err)
		}
		got := encodeCanonicalDelegatedResource(tc)
		if !bytes.Equal(got, want) {
			t.Fatalf("case %d: got %s, want %s", i, got, want)
		}
	}
}

func BenchmarkEncodeCanonicalDelegatedResource(b *testing.B) {
	dr := &rawdb.DelegatedResource{
		From:                      testAddr(0x11),
		To:                        testAddr(0x22),
		FrozenBalanceForBandwidth: 50_000_000_000,
		FrozenBalanceForEnergy:    12_345_678_900,
		ExpireTimeForBandwidth:    1732700000000,
		ExpireTimeForEnergy:       1732799999999,
	}
	b.Run("json.Marshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := json.Marshal(dr)
			if err != nil || len(out) == 0 {
				b.Fatal(err)
			}
		}
	})
	b.Run("canonical", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out := encodeCanonicalDelegatedResource(dr)
			if len(out) == 0 {
				b.Fatal("empty")
			}
		}
	})
}
