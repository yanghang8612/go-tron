package main

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestPreviousValuePresenceAndWidthAreSeparate(t *testing.T) {
	m := map[string]*stats{}
	for _, c := range []*rawdb.StateDomainChange{{PrevExists: false, Prev: make([]byte, 32)}, {PrevExists: true, Prev: []byte{}}, {PrevExists: true, Prev: make([]byte, 32)}, {PrevExists: true, Prev: []byte{0, 0, 0, 0, 0, 0, 1, 0}}} {
		if e := observe(m, "kv-latest/test", c); e != nil {
			t.Fatal(e)
		}
	}
	s := m["kv-latest/test"]
	if s.Rows != 4 || s.Present != 3 || s.Payload != 40 || s.Zero != 1 || s.Widths[0] != 1 || s.Widths[2] != 1 {
		t.Fatalf("bad presence/width census: %+v", s)
	}
}
