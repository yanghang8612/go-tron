package rawdb

import (
	"bytes"
	"testing"
)

func TestStateHistoryKeyspaceBounds(t *testing.T) {
	changeSetStart, changeSetLimit := StateHistoryKeyspaceBounds()
	for _, tc := range []struct {
		name         string
		prefix       []byte
		start, limit []byte
	}{
		{name: "changeset", prefix: stateChangeSetPrefix, start: changeSetStart, limit: changeSetLimit},
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
	changeSetLimit[0] ^= 0xff
	if bytes.Equal(changeSetStart, stateChangeSetPrefix) || bytes.Equal(changeSetLimit, prefixUpperBound(stateChangeSetPrefix)) {
		t.Fatal("returned bounds alias schema storage")
	}
}

func TestStateHistoryPostingKeyspaceBounds(t *testing.T) {
	postingStart, postingLimit, directoryStart, directoryLimit := StateHistoryPostingKeyspaceBounds()
	for _, tc := range []struct {
		name         string
		prefix       []byte
		start, limit []byte
	}{
		{name: "posting", prefix: stateChangePostingPrefix, start: postingStart, limit: postingLimit},
		{name: "directory", prefix: stateChangeKeyDirectoryPrefix, start: directoryStart, limit: directoryLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !bytes.Equal(tc.start, tc.prefix) || bytes.Compare(tc.limit, append(append([]byte(nil), tc.prefix...), 0xff)) <= 0 {
				t.Fatalf("bounds [%q,%q) do not cover %q", tc.start, tc.limit, tc.prefix)
			}
		})
	}
}
