package rawdb

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type structuredStateKVReadCase struct {
	value    []byte
	getErr   error
	hasValue bool
	hasErr   error
	hasCalls int
}

func (r *structuredStateKVReadCase) Has([]byte) (bool, error) {
	r.hasCalls++
	return r.hasValue, r.hasErr
}

func (r *structuredStateKVReadCase) Get([]byte) ([]byte, error) {
	return r.value, r.getErr
}

func (r *structuredStateKVReadCase) GetNoCopyCachedStateKVLatest([]byte, common.AccountID, uint64, uint16, []byte) ([]byte, error) {
	return r.value, r.getErr
}

func (r *structuredStateKVReadCase) IsKeyNotFound(err error) bool {
	return errors.Is(err, errStructuredStateKVMissing)
}

func TestReadStateKVLatestNoCopyStructuredFastPath(t *testing.T) {
	owner := common.BytesToAddress([]byte{common.AddressPrefixMainnet, 0x42})
	key := []byte("slot")
	read := func(r *structuredStateKVReadCase) ([]byte, bool, error) {
		return ReadStateKVLatestNoCopy(r, owner, 7, kvdomains.ContractStorage, key)
	}

	hit := &structuredStateKVReadCase{value: EncodeStateKVLatestValue([]byte{0x12, 0x34})}
	value, ok, err := read(hit)
	if err != nil || !ok || len(value) != 2 || value[0] != 0x12 || value[1] != 0x34 || hit.hasCalls != 0 {
		t.Fatalf("hit: value=%x present=%v err=%v Has calls=%d", value, ok, err, hit.hasCalls)
	}
	if &value[0] != &hit.value[1] {
		t.Fatal("hit lost no-copy value ownership")
	}

	missing := &structuredStateKVReadCase{getErr: errStructuredStateKVMissing}
	value, ok, err = read(missing)
	if err != nil || ok || value != nil || missing.hasCalls != 0 {
		t.Fatalf("classified miss: value=%x present=%v err=%v Has calls=%d", value, ok, err, missing.hasCalls)
	}

	readFailure := errors.New("get failed")
	underlyingMiss := &structuredStateKVReadCase{getErr: readFailure}
	value, ok, err = read(underlyingMiss)
	if err != nil || ok || value != nil || underlyingMiss.hasCalls != 1 {
		t.Fatalf("unclassified miss: value=%x present=%v err=%v Has calls=%d", value, ok, err, underlyingMiss.hasCalls)
	}

	presentFailure := &structuredStateKVReadCase{getErr: readFailure, hasValue: true}
	_, ok, err = read(presentFailure)
	wantReadErr := fmt.Sprintf("rawdb: read state kv latest for %s generation 7 domain %#04x: get failed", owner.Hex(), uint16(kvdomains.ContractStorage))
	if ok || !errors.Is(err, readFailure) || presentFailure.hasCalls != 1 ||
		err.Error() != wantReadErr {
		t.Fatalf("present read failure: present=%v err=%v Has calls=%d", ok, err, presentFailure.hasCalls)
	}

	presenceFailure := errors.New("has failed")
	failedHas := &structuredStateKVReadCase{getErr: readFailure, hasErr: presenceFailure}
	_, ok, err = read(failedHas)
	if ok || !errors.Is(err, presenceFailure) || failedHas.hasCalls != 1 ||
		!strings.Contains(err.Error(), "presence after get error") {
		t.Fatalf("presence failure: present=%v err=%v Has calls=%d", ok, err, failedHas.hasCalls)
	}

	malformed := &structuredStateKVReadCase{value: []byte{0x00}}
	_, ok, err = read(malformed)
	if ok || err == nil || !strings.Contains(err.Error(), "domain 0x") || malformed.hasCalls != 0 {
		t.Fatalf("malformed value: present=%v err=%v Has calls=%d", ok, err, malformed.hasCalls)
	}
}
