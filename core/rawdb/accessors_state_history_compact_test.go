package rawdb

import (
	"bytes"
	"testing"
)

func TestStateHistoryBlockRangeBounds(t *testing.T) {
	start, end, err := StateHistoryBlockRangeBounds(7, 9)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []uint64{6, 7, 8, 9, 10} {
		for _, seq := range []uint64{0, 1, ^uint64(0)} {
			key := stateChangeSetKey(n, seq)
			inside := bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0
			if inside != (n >= 7 && n <= 9) {
				t.Fatalf("block=%d seq=%d inside=%t", n, seq, inside)
			}
		}
	}
	if _, _, err := StateHistoryBlockRangeBounds(9, 7); err == nil {
		t.Fatal("inverted range accepted")
	}
	start, end, err = StateHistoryBlockRangeBounds(^uint64(0), ^uint64(0))
	last := stateChangeSetKey(^uint64(0), ^uint64(0))
	if err != nil || bytes.Compare(start, last) > 0 || bytes.Compare(last, end) >= 0 {
		t.Fatal("maximum block overflow")
	}
}

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
