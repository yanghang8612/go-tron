package rawdb

import (
	"bytes"
	"testing"
)

func TestStateHistoryKeyspaceBounds(t *testing.T) {
	changeSetStart, changeSetLimit, changeIndexStart, changeIndexLimit := StateHistoryKeyspaceBounds()
	for _, tc := range []struct {
		name         string
		prefix       []byte
		start, limit []byte
	}{
		{name: "changeset", prefix: stateChangeSetPrefix, start: changeSetStart, limit: changeSetLimit},
		{name: "change index", prefix: stateChangeInversePrefix, start: changeIndexStart, limit: changeIndexLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !bytes.Equal(tc.start, tc.prefix) {
				t.Fatalf("start = %q, want %q", tc.start, tc.prefix)
			}
			if bytes.Compare(tc.limit, append(append([]byte(nil), tc.prefix...), 0xff)) <= 0 {
				t.Fatalf("limit %q does not cover prefix %q", tc.limit, tc.prefix)
			}
		})
	}
	changeSetStart[0] ^= 0xff
	changeIndexLimit[0] ^= 0xff
	if bytes.Equal(changeSetStart, stateChangeSetPrefix) || bytes.Equal(changeIndexLimit, prefixUpperBound(stateChangeInversePrefix)) {
		t.Fatal("returned bounds alias schema storage")
	}
}
