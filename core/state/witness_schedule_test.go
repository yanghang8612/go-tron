package state

import (
	"bytes"
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var benchmarkAddressListBytes []byte
var benchmarkAddressListFound bool

func BenchmarkAddressListAppendDecodeReencode(b *testing.B) {
	const count = 4096
	addrs := make([]tcommon.Address, count)
	for i := range addrs {
		addrs[i][0] = 0x41
		addrs[i][tcommon.AddressLength-2] = byte(i >> 8)
		addrs[i][tcommon.AddressLength-1] = byte(i)
	}
	raw := encodeAddressList(addrs)
	for _, tc := range []struct {
		name string
		addr tcommon.Address
	}{
		{name: "existing", addr: addrs[count-1]},
		{name: "missing", addr: tcommon.Address{0x41, 0x99}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for b.Loop() {
				existing := decodeAddressList(raw)
				found := false
				for _, addr := range existing {
					if addr == tc.addr {
						found = true
						break
					}
				}
				benchmarkAddressListFound = found
				if !found {
					benchmarkAddressListBytes = encodeAddressList(append(existing, tc.addr))
				}
			}
		})
	}
}

func BenchmarkAddressListAppendEncoded(b *testing.B) {
	const count = 4096
	addrs := make([]tcommon.Address, count)
	for i := range addrs {
		addrs[i][0] = 0x41
		addrs[i][tcommon.AddressLength-2] = byte(i >> 8)
		addrs[i][tcommon.AddressLength-1] = byte(i)
	}
	raw := encodeAddressList(addrs)
	for _, tc := range []struct {
		name string
		addr tcommon.Address
	}{
		{name: "existing", addr: addrs[count-1]},
		{name: "missing", addr: tcommon.Address{0x41, 0x99}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for b.Loop() {
				benchmarkAddressListBytes, benchmarkAddressListFound = appendAddressListEncoded(raw, tc.addr)
			}
		})
	}
}

func wsAddr(tag byte) tcommon.Address {
	raw := make([]byte, tcommon.AddressLength)
	raw[0] = 0x41
	raw[tcommon.AddressLength-1] = tag
	return tcommon.BytesToAddress(raw)
}

func sameAddrs(a, b []tcommon.Address) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// encode/decode is a pure round-trip; empty and malformed inputs decode to nil.
func TestAddressListCodec(t *testing.T) {
	in := []tcommon.Address{wsAddr(1), wsAddr(2), wsAddr(3)}
	if got := decodeAddressList(encodeAddressList(in)); !sameAddrs(got, in) {
		t.Fatalf("round-trip mismatch: got %v want %v", got, in)
	}
	if got, err := decodeAddressListStrict("test list", encodeAddressList(in)); err != nil || !sameAddrs(got, in) {
		t.Fatalf("strict round-trip = %v err=%v, want %v nil", got, err, in)
	}
	if got, err := decodeAddressListStrict("test list", encodeAddressList(nil)); err != nil || got != nil {
		t.Fatalf("strict empty list = %v err=%v, want nil nil", got, err)
	}
	if got := decodeAddressList(encodeAddressList(nil)); got != nil {
		t.Fatalf("empty list should decode to nil, got %v", got)
	}
	if got := decodeAddressList([]byte{0, 0}); got != nil {
		t.Fatalf("short data should decode to nil, got %v", got)
	}
	// Count says 2 but only 1 address worth of bytes follows → nil.
	bad := []byte{0, 0, 0, 2, 0x41}
	if got := decodeAddressList(bad); got != nil {
		t.Fatalf("truncated data should decode to nil, got %v", got)
	}
	if got, err := decodeAddressListStrict("test list", bad); err == nil || got != nil {
		t.Fatalf("strict truncated data = %v err=%v, want nil error", got, err)
	}
}

func TestAppendAddressListEncodedMatchesDecodeReencode(t *testing.T) {
	valid := encodeAddressList([]tcommon.Address{wsAddr(1), wsAddr(2)})
	trailing := append(append([]byte(nil), valid...), 0xaa, 0xbb)
	zeroWithTrailing := []byte{0, 0, 0, 0, 0x41}
	tests := []struct {
		name string
		raw  []byte
		addr tcommon.Address
	}{
		{name: "empty", addr: wsAddr(3)},
		{name: "short", raw: []byte{0, 0}, addr: wsAddr(3)},
		{name: "truncated", raw: []byte{0, 0, 0, 2, 0x41}, addr: wsAddr(3)},
		{name: "zero-with-trailing", raw: zeroWithTrailing, addr: wsAddr(3)},
		{name: "existing", raw: valid, addr: wsAddr(2)},
		{name: "missing", raw: valid, addr: wsAddr(3)},
		{name: "existing-with-trailing", raw: trailing, addr: wsAddr(2)},
		{name: "missing-with-trailing", raw: trailing, addr: wsAddr(3)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existing := decodeAddressList(tt.raw)
			wantFound := false
			for _, addr := range existing {
				if addr == tt.addr {
					wantFound = true
					break
				}
			}
			var want []byte
			if !wantFound {
				want = encodeAddressList(append(existing, tt.addr))
			}
			got, found := appendAddressListEncoded(tt.raw, tt.addr)
			if found != wantFound || !bytes.Equal(got, want) {
				t.Fatalf("got (found=%v, %x), want (found=%v, %x)", found, got, wantFound, want)
			}
		})
	}
}

// AppendWitnessIndex grows the index and is idempotent for an existing address.
func TestWitnessIndexAppendDedup(t *testing.T) {
	sdb := newTestStateDB(t)
	if err := sdb.AppendWitnessIndex(wsAddr(1)); err != nil {
		t.Fatal(err)
	}
	if err := sdb.AppendWitnessIndex(wsAddr(2)); err != nil {
		t.Fatal(err)
	}
	if err := sdb.AppendWitnessIndex(wsAddr(1)); err != nil { // duplicate
		t.Fatal(err)
	}
	if got := sdb.ReadWitnessIndex(); !sameAddrs(got, []tcommon.Address{wsAddr(1), wsAddr(2)}) {
		t.Fatalf("dedup failed: got %v", got)
	}
}

func TestWitnessScheduleStrictSurfacesMalformedIndexes(t *testing.T) {
	sdb := newTestStateDB(t)

	if got, ok, err := sdb.ReadWitnessIndexStrict(); err != nil || ok || got != nil {
		t.Fatalf("strict absent witness index = %v ok=%v err=%v, want nil false nil", got, ok, err)
	}
	if got, ok, err := sdb.ReadActiveWitnessesStrict(); err != nil || ok || got != nil {
		t.Fatalf("strict absent active witnesses = %v ok=%v err=%v, want nil false nil", got, ok, err)
	}

	truncated := []byte{0, 0, 0, 2, 0x41}
	if err := sdb.SystemKVPut(kvdomains.SystemWitnessSchedule, witnessScheduleIndexKey, truncated); err != nil {
		t.Fatalf("write malformed witness index: %v", err)
	}
	if got := sdb.ReadWitnessIndex(); got != nil {
		t.Fatalf("compat malformed witness index = %v, want nil", got)
	}
	if got, ok, err := sdb.ReadWitnessIndexStrict(); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "witness index") {
		t.Fatalf("strict malformed witness index = %v ok=%v err=%v, want decode error", got, ok, err)
	}
	if err := sdb.AppendWitnessIndex(wsAddr(4)); err == nil || !strings.Contains(err.Error(), "witness index") {
		t.Fatalf("AppendWitnessIndex malformed index err = %v, want decode error", err)
	}

	if err := sdb.SystemKVPut(kvdomains.SystemWitnessSchedule, witnessScheduleActiveKey, truncated); err != nil {
		t.Fatalf("write malformed active witnesses: %v", err)
	}
	if got := sdb.ReadActiveWitnesses(); got != nil {
		t.Fatalf("compat malformed active witnesses = %v, want nil", got)
	}
	if got, ok, err := sdb.ReadActiveWitnessesStrict(); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "active witness list") {
		t.Fatalf("strict malformed active witnesses = %v ok=%v err=%v, want decode error", got, ok, err)
	}
}

// TestWitnessScheduleAnchorAndRewind is the Phase 3c state-layer gate: rooting a
// witness-schedule change moves the state root (anchor), and reopening an old
// root recovers the old active list AND index (rewind). Mirrors applyBlock's
// per-block parent-root open with a fresh StateDB per commit.
func TestWitnessScheduleAnchorAndRewind(t *testing.T) {
	sdb := newTestStateDB(t)

	// R1: active = {1,2}, index = {1,2}.
	if err := sdb.WriteActiveWitnesses([]tcommon.Address{wsAddr(1), wsAddr(2)}); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteWitnessIndex([]tcommon.Address{wsAddr(1), wsAddr(2)}); err != nil {
		t.Fatal(err)
	}
	r1, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit R1: %v", err)
	}

	// R2 built on R1 via a fresh StateDB: a witness joins (index {1,2,3}) and a
	// maintenance reshuffle changes the active set to {2,3}.
	sdb2, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb2.AppendWitnessIndex(wsAddr(3)); err != nil {
		t.Fatal(err)
	}
	if err := sdb2.WriteActiveWitnesses([]tcommon.Address{wsAddr(2), wsAddr(3)}); err != nil {
		t.Fatal(err)
	}
	r2, err := sdb2.Commit()
	if err != nil {
		t.Fatalf("commit R2: %v", err)
	}

	if r1 == r2 {
		t.Fatal("anchor: witness-schedule change did not move the state root")
	}

	// Flat latest is authoritative: opening R1 reads the current active/index.
	atR1, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR1.ReadActiveWitnesses(); !sameAddrs(got, []tcommon.Address{wsAddr(2), wsAddr(3)}) {
		t.Fatalf("R1-open latest active: got %v", got)
	}
	if got := atR1.ReadWitnessIndex(); !sameAddrs(got, []tcommon.Address{wsAddr(1), wsAddr(2), wsAddr(3)}) {
		t.Fatalf("R1-open latest index: got %v", got)
	}
	atR2, err := New(r2, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR2.ReadActiveWitnesses(); !sameAddrs(got, []tcommon.Address{wsAddr(2), wsAddr(3)}) {
		t.Fatalf("R2 active: got %v", got)
	}
	if got := atR2.ReadWitnessIndex(); !sameAddrs(got, []tcommon.Address{wsAddr(1), wsAddr(2), wsAddr(3)}) {
		t.Fatalf("R2 index: got %v", got)
	}
}
