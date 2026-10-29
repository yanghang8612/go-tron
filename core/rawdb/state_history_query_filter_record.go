package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/golang/snappy"
	"golang.org/x/crypto/blake2b"
)

var historyQueryPackFiltersMagic = [8]byte{'G', 'T', 'H', 'Q', 'P', 'F', '0', '1'}

// The small query record avoids reading and decoding every owner's row spans
// merely to inspect three fixed-size filters. It is never authoritative state.
func queryFilterRecordFromOwner(index []byte, block uint64) ([]byte, error) {
	r, err := decodeHistoryOwnerSpans(index, block)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(index, historyOwnerSpansMagic[:]) {
		return nil, nil
	}
	p := binary.AppendUvarint(nil, block)
	p = append(p, r.digest[:]...)
	p = append(p, historyQueryFilterTag[:]...)
	p = binary.AppendUvarint(p, uint64(r.queryKnown))
	for group, filter := range r.queryFilters {
		if r.queryKnown&(1<<group) != 0 {
			p = append(p, filter...)
		}
	}
	digest := blake2b.Sum256(p)
	out := append([]byte(nil), historyQueryPackFiltersMagic[:]...)
	out = append(out, digest[:]...)
	return append(out, snappy.Encode(nil, p)...), nil
}

func decodeQueryFilterRecord(raw []byte, block uint64) (*historyOwnerSpans, error) {
	bad := errors.New("rawdb: invalid optional history pack filters")
	if len(raw) < 40 || !bytes.Equal(raw[:8], historyQueryPackFiltersMagic[:]) {
		return nil, bad
	}
	n, e := snappy.DecodedLen(raw[40:])
	if e != nil || n > 2816 || n < 42 {
		return nil, bad
	}
	p, e := snappy.Decode(nil, raw[40:])
	if e != nil {
		return nil, bad
	}
	digest := blake2b.Sum256(p)
	if !bytes.Equal(raw[8:40], digest[:]) {
		return nil, bad
	}
	b, n := binary.Uvarint(p)
	if n <= 0 || b != block || len(p)-n < 32 {
		return nil, bad
	}
	p = p[n:]
	r := &historyOwnerSpans{}
	copy(r.digest[:], p[:32])
	if e = decodeHistoryQueryFilters(p[32:], r); e != nil {
		return nil, e
	}
	return r, nil
}
