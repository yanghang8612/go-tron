package main

// This is a SEMANTIC PROJECTION experiment. A full account snapshot cannot be
// reconstructed from its output. Product integration requires an explicit new
// state/MuHash scheme and presence-aware reward API. No production code changes.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var nativeMagic = []byte{0, 'G', 'T', 'S', 'V', 1}
var emptySnapshot = append(append([]byte(nil), nativeMagic...), 0, 0)
var accountDescriptor = new(corepb.Account).ProtoReflect().Descriptor()

type decoder struct {
	b []byte
	i int
}

func (d *decoder) take(n uint64) ([]byte, error) {
	if n > uint64(len(d.b)-d.i) {
		return nil, fmt.Errorf("truncated native data")
	}
	b := d.b[d.i : d.i+int(n)]
	d.i += int(n)
	return b, nil
}
func (d *decoder) varint() (uint64, error) {
	v, n := binary.Uvarint(d.b[d.i:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid native varint")
	}
	var tmp [10]byte
	if n != binary.PutUvarint(tmp[:], v) {
		return 0, fmt.Errorf("noncanonical native varint")
	}
	d.i += n
	return v, nil
}
func (d *decoder) blob() ([]byte, error) {
	n, e := d.varint()
	if e != nil {
		return nil, e
	}
	return d.take(n)
}
func (d *decoder) done() error {
	if d.i != len(d.b) {
		return fmt.Errorf("trailing native data")
	}
	return nil
}

// projectVotes copies the exact old native field-5 frame, including the full
// native Vote values and their unknown-field trailers. All non-Votes Account
// fields and Account-level unknown fields are deliberately excluded.
func projectVotes(native []byte) (projected, originalVotePayload []byte, err error) {
	if !bytes.HasPrefix(native, nativeMagic) {
		return nil, nil, fmt.Errorf("non-native account snapshot")
	}
	d := decoder{b: native[len(nativeMagic):]}
	n, e := d.varint()
	if e != nil {
		return nil, nil, e
	}
	if n > uint64(accountDescriptor.Fields().Len()) {
		return nil, nil, fmt.Errorf("invalid Account field count")
	}
	var prev uint64
	var frame []byte
	for j := uint64(0); j < n; j++ {
		start := d.i
		num, e := d.varint()
		if e != nil || num <= prev || num > math.MaxInt32 {
			return nil, nil, fmt.Errorf("invalid Account field order")
		}
		prev = num
		t, e := d.take(1)
		if e != nil {
			return nil, nil, e
		}
		p, e := d.blob()
		if e != nil {
			return nil, nil, e
		}
		if num == 5 {
			if t[0] != 0x40|byte(protoreflect.MessageKind) {
				return nil, nil, fmt.Errorf("Votes native field shape mismatch")
			}
			frame = d.b[start:d.i]
			originalVotePayload = p
		}
	}
	if _, e := d.blob(); e != nil {
		return nil, nil, e
	}
	if e := d.done(); e != nil {
		return nil, nil, e
	}
	projected = append(make([]byte, 0, len(frame)+8), nativeMagic...)
	if frame == nil {
		projected = append(projected, 0)
	} else {
		projected = append(projected, 1)
		projected = append(projected, frame...)
	}
	projected = append(projected, 0) // no Account-level unknown fields in this NEW model
	return projected, originalVotePayload, nil
}

// verifyVotes independently parses the original native list and compares the
// projected generated Vote objects one by one, preserving order, duplicates,
// signed count bits, exact address bytes, and per-Vote unknown fields.
func verifyVotes(original []byte, votes []*corepb.Vote) (uint64, error) {
	if original == nil {
		if len(votes) != 0 {
			return 0, fmt.Errorf("absent Votes gained entries")
		}
		return 0, nil
	}
	d := decoder{b: original}
	n, e := d.varint()
	if e != nil || n == 0 || n > uint64(len(original)) {
		return 0, fmt.Errorf("invalid original Votes list")
	}
	if n != uint64(len(votes)) {
		return 0, fmt.Errorf("Votes count mismatch: %d != %d", n, len(votes))
	}
	var unknownBytes uint64
	for i := uint64(0); i < n; i++ {
		raw, e := d.blob()
		if e != nil {
			return 0, e
		}
		v := decoder{b: raw}
		fields, e := v.varint()
		if e != nil || fields > 2 {
			return 0, fmt.Errorf("invalid original Vote fields")
		}
		var prev uint64
		var address []byte
		var count int64
		for j := uint64(0); j < fields; j++ {
			num, e := v.varint()
			if e != nil || num <= prev {
				return 0, fmt.Errorf("invalid Vote field order")
			}
			prev = num
			kind, e := v.take(1)
			if e != nil {
				return 0, e
			}
			p, e := v.blob()
			if e != nil {
				return 0, e
			}
			switch num {
			case 1:
				if kind[0] != byte(protoreflect.BytesKind) {
					return 0, fmt.Errorf("invalid Vote address kind")
				}
				address = p
			case 2:
				if kind[0] != byte(protoreflect.Int64Kind) || len(p) != 8 {
					return 0, fmt.Errorf("invalid Vote count kind/length")
				}
				count = int64(binary.BigEndian.Uint64(p))
			default:
				return 0, fmt.Errorf("unknown declared native Vote field %d", num)
			}
		}
		u, e := v.blob()
		if e != nil {
			return 0, e
		}
		if e := v.done(); e != nil {
			return 0, e
		}
		if votes[i] == nil || !bytes.Equal(address, votes[i].VoteAddress) || count != votes[i].VoteCount || !bytes.Equal(u, votes[i].ProtoReflect().GetUnknown()) {
			return 0, fmt.Errorf("Vote %d semantic or unknown mismatch", i)
		}
		unknownBytes += uint64(len(u))
	}
	return unknownBytes, d.done()
}

// projectedPresence is intentionally separate from nil protobuf byte slices:
// an existing empty Account snapshot must not become a missing snapshot.
func projectedPresence(projected []byte, exists bool) (present bool, empty bool) {
	return exists, exists && bytes.Equal(projected, emptySnapshot)
}
