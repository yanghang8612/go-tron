package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

// This preconditioner preserves a page's canonical virtual bytes. References
// point only to registered account values already emitted in that same page.
const (
	accountDeltaPageMax    = 1 << 20
	accountDeltaValueMax   = 4096
	accountDeltaDepthMax   = 8
	accountDeltaHeaderSize = 44
	accountDeltaValuesMax  = accountDeltaPageMax/21 + 1
)

var errAccountDelta = errors.New("snapshots: invalid account delta page")

type accountDeltaValue struct{ Offset, Length, KeyID uint32 }
type accountDeltaBase struct {
	offset, length uint32
	depth          uint8
}
type accountDeltaStats struct {
	Values, Full, Deltas, Identical, PrefixSuffix, Masks, Numeric uint64
	MaxDepth                                                      uint8
}

func appendAccountDeltaVar(dst []byte, n uint64) []byte { return binary.AppendUvarint(dst, n) }

func accountDeltaPatch(base, value []byte) []byte {
	pre := 0
	for pre < len(base) && pre < len(value) && base[pre] == value[pre] {
		pre++
	}
	suf := 0
	for suf < len(base)-pre && suf < len(value)-pre && base[len(base)-1-suf] == value[len(value)-1-suf] {
		suf++
	}
	p := []byte{0}
	p = appendAccountDeltaVar(p, uint64(pre))
	p = appendAccountDeltaVar(p, uint64(suf))
	p = append(p, value[pre:len(value)-suf]...)
	if numeric := accountDeltaNumericPatch(base, value); len(numeric) > 0 && len(numeric) < len(p) {
		p = numeric
	}
	if len(base) != len(value) {
		return p
	}
	mask := make([]byte, (len(value)+7)/8)
	changed := make([]byte, 0, len(value))
	for i, b := range value {
		if b != base[i] {
			mask[i/8] |= 1 << uint(i%8)
			changed = append(changed, b)
		}
	}
	if 1+len(mask)+len(changed) >= len(p) {
		return p
	}
	out := append([]byte{1}, mask...)
	return append(out, changed...)
}

// notes are sorted, non-overlapping canonical account Prev intervals. The
// caller excludes absent/empty/oversized and page-crossing values.
func encodeAccountDeltaPage(raw []byte, notes []accountDeltaValue, limit uint8) ([]byte, accountDeltaStats, error) {
	var stats accountDeltaStats
	if len(raw) == 0 || len(raw) > accountDeltaPageMax || limit == 0 || limit > accountDeltaDepthMax || len(notes) > accountDeltaValuesMax {
		return nil, stats, errAccountDelta
	}
	counts := make(map[uint32]int)
	end := uint64(0)
	for _, n := range notes {
		if n.Length == 0 || n.Length > accountDeltaValueMax || uint64(n.Offset) < end || uint64(n.Offset)+uint64(n.Length) > uint64(len(raw)) {
			return nil, stats, errAccountDelta
		}
		end = uint64(n.Offset) + uint64(n.Length)
		counts[n.KeyID]++
	}
	out := make([]byte, accountDeltaHeaderSize, len(raw)+accountDeltaHeaderSize)
	copy(out, "ADP1")
	binary.BigEndian.PutUint32(out[4:8], uint32(len(raw)))
	out[8] = limit
	sum := sha256.Sum256(raw)
	copy(out[12:44], sum[:])
	bases := make(map[uint32]accountDeltaBase)
	pos := uint32(0)
	literal := func(to uint32) {
		if to > pos {
			out = append(out, 0)
			out = appendAccountDeltaVar(out, uint64(to-pos))
			out = append(out, raw[pos:to]...)
			pos = to
		}
	}
	for _, n := range notes {
		if counts[n.KeyID] < 2 {
			continue
		}
		literal(n.Offset)
		value := raw[n.Offset : n.Offset+n.Length]
		base, have := bases[n.KeyID]
		full := appendAccountDeltaVar([]byte{1}, uint64(n.Length))
		full = append(full, value...)
		encoded := full
		depth := uint8(0)
		kind := uint8(1)
		if have && base.depth < limit {
			previous := raw[base.offset : base.offset+base.length]
			if bytes.Equal(previous, value) {
				encoded = appendAccountDeltaVar([]byte{3}, uint64(n.Offset-base.offset))
				kind = 3
			} else {
				patch := appendAccountDeltaVar([]byte{2}, uint64(n.Length))
				patch = appendAccountDeltaVar(patch, uint64(n.Offset-base.offset))
				patch = append(patch, accountDeltaPatch(previous, value)...)
				if len(patch) < len(full) {
					encoded = patch
					kind = 2
				}
			}
			if kind != 1 {
				depth = base.depth + 1
			}
		}
		out = append(out, encoded...)
		pos = n.Offset + n.Length
		bases[n.KeyID] = accountDeltaBase{n.Offset, n.Length, depth}
		stats.Values++
		stats.MaxDepth = max(stats.MaxDepth, depth)
		if kind == 1 {
			stats.Full++
		} else {
			stats.Deltas++
			if kind == 3 {
				stats.Identical++
			} else { // patch kind follows the two canonical varints
				_, k := binary.Uvarint(encoded[1:])
				_, j := binary.Uvarint(encoded[1+k:])
				if encoded[1+k+j] == 0 {
					stats.PrefixSuffix++
				} else if encoded[1+k+j] == 1 {
					stats.Masks++
				} else {
					stats.Numeric++
				}
			}
		}
	}
	literal(uint32(len(raw)))
	return out, stats, nil
}

type accountDeltaParser struct{ p []byte }

func (r *accountDeltaParser) take(n int) ([]byte, error) {
	if n < 0 || n > len(r.p) {
		return nil, errAccountDelta
	}
	p := r.p[:n]
	r.p = r.p[n:]
	return p, nil
}
func (r *accountDeltaParser) number() (uint64, error) {
	v, n := binary.Uvarint(r.p)
	if n <= 0 || n != accountDeltaVarLen(v) {
		return 0, errAccountDelta
	}
	r.p = r.p[n:]
	return v, nil
}
func accountDeltaVarLen(n uint64) int {
	if n == 0 {
		return 1
	}
	return (bits.Len64(n) + 6) / 7
}

func decodeAccountDeltaPage(encoded []byte, expected uint64, dst []byte) ([]byte, accountDeltaStats, error) {
	var stats accountDeltaStats
	fail := func() ([]byte, accountDeltaStats, error) { return nil, stats, errAccountDelta }
	if expected == 0 || expected > accountDeltaPageMax || len(encoded) < accountDeltaHeaderSize || len(encoded) > 2*accountDeltaPageMax+accountDeltaHeaderSize || string(encoded[:4]) != "ADP1" || uint64(binary.BigEndian.Uint32(encoded[4:8])) != expected {
		return fail()
	}
	limit := encoded[8]
	if limit == 0 || limit > accountDeltaDepthMax || encoded[9] != 0 || encoded[10] != 0 || encoded[11] != 0 {
		return fail()
	}
	if uint64(cap(dst)) < expected {
		dst = make([]byte, 0, int(expected))
	} else {
		dst = dst[:0]
	}
	r := accountDeltaParser{encoded[accountDeltaHeaderSize:]}
	bases := make(map[uint32]accountDeltaBase)
	for len(r.p) > 0 {
		op := r.p[0]
		r.p = r.p[1:]
		n, err := r.number()
		if err != nil || len(dst) >= int(expected) {
			return fail()
		}
		start := len(dst)
		depth := uint8(0)
		if op == 0 || op == 1 {
			if n == 0 || n > expected-uint64(start) || op == 1 && n > accountDeltaValueMax {
				return fail()
			}
			p, e := r.take(int(n))
			if e != nil {
				return fail()
			}
			dst = append(dst, p...)
		} else if op == 2 || op == 3 {
			distance := n
			if op == 2 {
				distance, err = r.number()
				if err != nil {
					return fail()
				}
			}
			if distance == 0 || distance > uint64(start) {
				return fail()
			}
			base, ok := bases[uint32(uint64(start)-distance)]
			if !ok || base.depth >= limit || uint64(base.offset)+uint64(base.length) > uint64(start) {
				return fail()
			}
			previous := dst[base.offset : base.offset+base.length]
			depth = base.depth + 1
			if op == 3 {
				n = uint64(base.length)
			}
			if n == 0 || n > accountDeltaValueMax || n > expected-uint64(start) {
				return fail()
			}
			if op == 3 {
				dst = append(dst, previous...)
				stats.Identical++
			} else {
				mode, e := r.take(1)
				if e != nil {
					return fail()
				}
				if mode[0] == 0 {
					pre, e := r.number()
					if e != nil {
						return fail()
					}
					suf, e := r.number()
					if e != nil || pre > n || suf > n-pre || pre > uint64(len(previous)) || suf > uint64(len(previous))-pre {
						return fail()
					}
					middle, e := r.take(int(n - pre - suf))
					if e != nil {
						return fail()
					}
					dst = append(dst, previous[:pre]...)
					dst = append(dst, middle...)
					dst = append(dst, previous[uint64(len(previous))-suf:]...)
					stats.PrefixSuffix++
				} else if mode[0] == 1 {
					if n != uint64(len(previous)) {
						return fail()
					}
					mask, e := r.take((int(n) + 7) / 8)
					if e != nil {
						return fail()
					}
					if n%8 != 0 && mask[len(mask)-1]>>uint(n%8) != 0 {
						return fail()
					}
					dst = append(dst, previous...)
					for i := 0; i < int(n); i++ {
						if mask[i/8]&(1<<uint(i%8)) != 0 {
							p, e := r.take(1)
							if e != nil {
								return fail()
							}
							dst[start+i] = p[0]
						}
					}
					stats.Masks++
				} else if mode[0] == 2 {
					value, e := decodeAccountDeltaNumericPatch(&r, previous)
					if e != nil || uint64(len(value)) != n {
						return fail()
					}
					dst = append(dst, value...)
					stats.Numeric++
				} else {
					return fail()
				}
			}
		} else {
			return fail()
		}
		if op != 0 {
			if len(bases) >= accountDeltaValuesMax {
				return fail()
			}
			bases[uint32(start)] = accountDeltaBase{uint32(start), uint32(n), depth}
			stats.Values++
			stats.MaxDepth = max(stats.MaxDepth, depth)
			if op == 1 {
				stats.Full++
			} else {
				stats.Deltas++
			}
		}
	}
	if uint64(len(dst)) != expected {
		return fail()
	}
	sum := sha256.Sum256(dst)
	if !bytes.Equal(sum[:], encoded[12:44]) {
		return nil, stats, fmt.Errorf("%w: canonical digest", errAccountDelta)
	}
	return dst, stats, nil
}
