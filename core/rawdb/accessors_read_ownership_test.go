package rawdb

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

type ownershipReadFixture struct {
	source         []byte
	exists         bool
	hasErr, getErr error
	owned          bool
	trace          []string
	lastGet        []byte
}

func (r *ownershipReadFixture) Has([]byte) (bool, error) {
	r.trace = append(r.trace, "Has")
	return r.exists, r.hasErr
}
func (r *ownershipReadFixture) Get([]byte) ([]byte, error) {
	r.trace = append(r.trace, "Get")
	if r.getErr != nil {
		return nil, r.getErr
	}
	r.lastGet = r.source
	if r.owned {
		r.lastGet = bytes.Clone(r.source)
	}
	return r.lastGet, nil
}

type ownershipDeclaredReader struct {
	*ownershipReadFixture
	enabled bool
	checked int
}

func (r *ownershipDeclaredReader) GetReturnsOwnedBytes() bool { r.checked++; return r.enabled }

type ownershipPresenceReader struct{ *ownershipDeclaredReader }

func (r *ownershipPresenceReader) GetWithPresence([]byte) ([]byte, bool, error) {
	r.trace = append(r.trace, "GetWithPresence")
	return r.source, r.exists, r.getErr
}

func TestReadPresentValueOwnedOracleAndErrors(t *testing.T) {
	failure := errors.New("fixture failure")
	for _, test := range []struct {
		name           string
		value          []byte
		exists         bool
		hasErr, getErr error
	}{
		{"present", []byte("original"), true, nil, nil},
		{"empty", []byte{}, true, nil, nil},
		{"nil", nil, true, nil, nil},
		{"missing", nil, false, nil, failure},
		{"has_error", nil, true, failure, nil},
		{"get_error", nil, true, nil, failure},
	} {
		for _, declared := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/owned=%t", test.name, declared), func(t *testing.T) {
				makeReader := func() *ownershipDeclaredReader {
					return &ownershipDeclaredReader{ownershipReadFixture: &ownershipReadFixture{source: bytes.Clone(test.value), exists: test.exists, hasErr: test.hasErr, getErr: test.getErr, owned: declared}, enabled: declared}
				}
				old, newReader := makeReader(), makeReader()
				want, wp, we := readPresentValueBeforeOwnedOracle(old, []byte("key"), "fixture")
				got, gp, ge := readPresentValue(newReader, []byte("key"), "fixture")
				if !reflect.DeepEqual(got, want) || gp != wp || fmt.Sprint(ge) != fmt.Sprint(we) || !reflect.DeepEqual(old.trace, newReader.trace) {
					t.Fatalf("got (%v,%t,%v,%v), want (%v,%t,%v,%v)", got, gp, ge, newReader.trace, want, wp, we, old.trace)
				}
				if we != nil && !errors.Is(ge, failure) {
					t.Fatalf("lost error identity: %v", ge)
				}
				if (!test.exists || test.hasErr != nil || test.getErr != nil) && newReader.checked != 0 {
					t.Fatal("ownership queried before successful Has/Get")
				}
				if len(got) > 0 {
					saved := bytes.Clone(got)
					newReader.source[0] ^= 0xff
					if !bytes.Equal(got, saved) {
						t.Fatal("result aliases source bytes")
					}
					got[0] ^= 0x55
					if declared && &got[0] != &newReader.lastGet[0] {
						t.Fatal("owned transfer did not retain the Get result")
					}
				}
			})
		}
	}
}

func TestReadPresentValueUnmarkedAndPresenceUnchanged(t *testing.T) {
	want, wp, we := readPresentValueBeforeOwnedOracle(nil, nil, "nil source")
	gotNil, gp, ge := readPresentValue(nil, nil, "nil source")
	if !reflect.DeepEqual(gotNil, want) || gp != wp || fmt.Sprint(ge) != fmt.Sprint(we) {
		t.Fatal("changed nil source error", ge, we)
	}
	r := &ownershipReadFixture{source: []byte("plain"), exists: true}
	got, ok, err := readPresentValue(r, nil, "plain")
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	r.source[0] = 'X'
	if string(got) != "plain" {
		t.Fatal("unmarked source no longer copied")
	}
	for _, failure := range []error{nil, errors.New("coupled failure")} {
		declared := &ownershipDeclaredReader{ownershipReadFixture: &ownershipReadFixture{source: []byte("coupled"), exists: true, getErr: failure}, enabled: true}
		p := &ownershipPresenceReader{declared}
		value, present, err := readPresentValue(p, nil, "coupled")
		if !reflect.DeepEqual(p.trace, []string{"GetWithPresence"}) || p.checked != 0 {
			t.Fatal("changed presence-coupled read path", p.trace, p.checked)
		}
		if failure != nil {
			if !errors.Is(err, failure) || present || value != nil {
				t.Fatal("changed coupled error", present, err)
			}
		} else if err != nil || !present || &value[0] != &p.source[0] {
			t.Fatal("changed coupled output ownership")
		}
	}
}
