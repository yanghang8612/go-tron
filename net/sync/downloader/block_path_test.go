package downloader

import "testing"

func TestBlockPathReserveAllowsSameHashAndRejectsFork(t *testing.T) {
	path := NewBlockPath()
	first := queueID(7)
	var ok bool
	path, ok = path.Reserve(first)
	if !ok {
		t.Fatal("first reservation rejected")
	}
	path, ok = path.Reserve(first)
	if !ok {
		t.Fatal("same-hash reservation rejected")
	}

	fork := queueID(7)
	fork.Hash[0] ^= 0xFF
	if !path.Conflicts(fork) {
		t.Fatal("fork at same height did not conflict")
	}
	if _, ok := path.Reserve(fork); ok {
		t.Fatal("fork reservation accepted")
	}
	if got := path[7]; got != first.Hash {
		t.Fatalf("reserved hash changed to %x, want %x", got, first.Hash)
	}
}

func TestBlockPathReserveInitializesNilPath(t *testing.T) {
	var path BlockPath
	bid := queueID(3)
	var ok bool
	path, ok = path.Reserve(bid)
	if !ok {
		t.Fatal("nil path reservation rejected")
	}
	if got := path[3]; got != bid.Hash {
		t.Fatalf("reserved hash = %x, want %x", got, bid.Hash)
	}
}

func TestBlockPathRelease(t *testing.T) {
	path := NewBlockPath()
	bid := queueID(9)
	var ok bool
	path, ok = path.Reserve(bid)
	if !ok {
		t.Fatal("reservation rejected")
	}
	path.Release(9)
	if path.Conflicts(bid) {
		t.Fatal("released reservation still conflicts")
	}
	if len(path) != 0 {
		t.Fatalf("path length = %d, want 0", len(path))
	}
}
