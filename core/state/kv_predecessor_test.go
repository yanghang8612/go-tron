package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestAccountKVBeforeMergesPendingDirtyAndTombstones(t *testing.T) {
	s := newTestStateDB(t)
	owner := testAddr(0xc1)
	domain := kvdomains.SystemShielded
	for _, key := range []string{"p/1", "p/2", "p/3"} {
		if err := s.SetAccountKV(owner, domain, []byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	scope := s.NewCommitScope()
	defer scope.Discard()
	gen := s.getStateObject(owner).accountKVGeneration
	scope.latestWriter.rememberDelete(owner, gen, domain, []byte("p/3"))
	scope.latestWriter.rememberPut(owner, gen, domain, []byte("p/4"), []byte("pending"))
	check := func(upper, wk, wv string) {
		t.Helper()
		var bound []byte
		if upper != "" {
			bound = []byte(upper)
		}
		k, v, ok, err := s.AccountKVBefore(owner, domain, []byte("p/"), bound)
		if err != nil || ok != (wk != "") || string(k) != wk || string(v) != wv {
			t.Fatal(upper, string(k), string(v), ok, err)
		}
	}
	check("", "p/4", "pending")
	check("p/4", "p/2", "p/2")
	if err := s.DeleteAccountKV(owner, domain, []byte("p/4")); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAccountKV(owner, domain, []byte("p/2")); err != nil {
		t.Fatal(err)
	}
	check("", "p/1", "p/1")
	if err := s.SetAccountKV(owner, domain, []byte("p/3"), []byte("dirty")); err != nil {
		t.Fatal(err)
	}
	check("", "p/3", "dirty")
	check("p/1", "", "")
}
