package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

const accountDeltaColumnHeader = 56

// Separating record metadata from account payloads lets Zstd compress both
// streams coherently. Layout intervals reassemble the original virtual page.
// The account payload stream still has only page-local bounded dependencies.
func encodeAccountDeltaColumns(raw []byte, notes []accountDeltaValue, depth uint8, deltas bool) ([]byte, accountDeltaStats, error) {
	return encodeAccountDeltaColumnsOrder(raw, notes, depth, deltas, false)
}

func encodeAccountDeltaColumnsOrder(raw []byte, notes []accountDeltaValue, depth uint8, deltas, ordered bool) ([]byte, accountDeltaStats, error) {
	var stats accountDeltaStats
	if len(raw) == 0 || len(raw) > accountDeltaPageMax || len(notes) == 0 || len(notes) > accountDeltaValuesMax {
		return nil, stats, errAccountDelta
	}
	var layout, literals, values []byte
	var packed []accountDeltaValue
	pos := uint32(0)
	order := make([]int, len(notes))
	rank := make([]int, len(notes))
	for i := range order {
		order[i] = i
	}
	if ordered {
		sort.SliceStable(order, func(i, j int) bool { return notes[order[i]].KeyID < notes[order[j]].KeyID })
	}
	for i, j := range order {
		rank[j] = i
	}
	for i, n := range notes {
		if n.Length == 0 || n.Length > accountDeltaValueMax || n.Offset < pos || uint64(n.Offset)+uint64(n.Length) > uint64(len(raw)) {
			return nil, stats, errAccountDelta
		}
		layout = appendAccountDeltaVar(layout, uint64(n.Offset-pos))
		layout = appendAccountDeltaVar(layout, uint64(n.Length))
		if ordered {
			layout = appendAccountDeltaVar(layout, uint64(rank[i]))
		}
		literals = append(literals, raw[pos:n.Offset]...)

		pos = n.Offset + n.Length
	}
	literals = append(literals, raw[pos:]...)
	for _, j := range order {
		n := notes[j]
		packed = append(packed, accountDeltaValue{uint32(len(values)), n.Length, n.KeyID})
		values = append(values, raw[n.Offset:n.Offset+n.Length]...)
	}
	payload := values
	flag := byte(0)
	if deltas {
		var e error
		payload, stats, e = encodeAccountDeltaPage(values, packed, depth)
		if e != nil {
			return nil, stats, e
		}
		flag = 1
	}
	out := make([]byte, accountDeltaColumnHeader, accountDeltaColumnHeader+len(layout)+len(literals)+len(payload))
	copy(out, "ADP2")
	if ordered {
		copy(out, "ADP3")
	}
	for i, n := range []int{len(raw), len(notes), len(layout), len(literals), len(payload)} {
		binary.BigEndian.PutUint32(out[4+i*4:8+i*4], uint32(n))
	}
	sum := sha256.Sum256(raw)
	copy(out[24:56], sum[:])
	out = append(out, flag)
	out = append(out, layout...)
	out = append(out, literals...)
	out = append(out, payload...)
	return out, stats, nil
}

func decodeAccountDeltaColumns(p []byte, expected uint64, dst []byte) ([]byte, accountDeltaStats, error) {
	var stats accountDeltaStats
	if expected == 0 || expected > accountDeltaPageMax || len(p) < accountDeltaColumnHeader+1 || len(p) > 3*accountDeltaPageMax+accountDeltaColumnHeader+accountDeltaHeaderSize || (string(p[:4]) != "ADP2" && string(p[:4]) != "ADP3") {
		return nil, stats, errAccountDelta
	}
	n := uint64(binary.BigEndian.Uint32(p[4:8]))
	count := uint64(binary.BigEndian.Uint32(p[8:12]))
	ll := uint64(binary.BigEndian.Uint32(p[12:16]))
	lit := uint64(binary.BigEndian.Uint32(p[16:20]))
	pl := uint64(binary.BigEndian.Uint32(p[20:24]))
	flag := p[accountDeltaColumnHeader]
	if n != expected || count == 0 || count > accountDeltaValuesMax || ll > count*10 || lit >= n || pl == 0 || ll+lit+pl+accountDeltaColumnHeader+1 != uint64(len(p)) || flag > 1 {
		return nil, stats, errAccountDelta
	}
	lr := accountDeltaParser{p[accountDeltaColumnHeader+1 : accountDeltaColumnHeader+1+ll]}
	literal := p[accountDeltaColumnHeader+1+ll : accountDeltaColumnHeader+1+ll+lit]
	values := p[accountDeltaColumnHeader+1+ll+lit:]
	if flag == 1 {
		var e error
		values, stats, e = decodeAccountDeltaPage(values, n-lit, nil)
		if e != nil {
			return nil, stats, e
		}
	} else if uint64(len(values)) != n-lit {
		return nil, stats, errAccountDelta
	}
	if uint64(cap(dst)) < n {
		dst = make([]byte, 0, int(n))
	} else {
		dst = dst[:0]
	}
	var lp, vp uint64
	type interval struct{ gap, length, rank uint64 }
	intervals := make([]interval, count)
	lengths := make([]uint64, count)
	seen := make([]bool, count)
	for i := range intervals {
		gap, e := lr.number()
		if e != nil {
			return nil, stats, e
		}
		length, e := lr.number()
		if e != nil || length == 0 || length > accountDeltaValueMax {
			return nil, stats, errAccountDelta
		}
		rank := uint64(i)
		if string(p[:4]) == "ADP3" {
			rank, e = lr.number()
			if e != nil {
				return nil, stats, e
			}
		}
		if rank >= count || seen[rank] {
			return nil, stats, errAccountDelta
		}
		seen[rank] = true
		lengths[rank] = length
		intervals[i] = interval{gap, length, rank}
	}
	offsets := make([]uint64, count)
	var total uint64
	for i, length := range lengths {
		offsets[i] = total
		total += length
	}
	if total != uint64(len(values)) {
		return nil, stats, errAccountDelta
	}
	for i := uint64(0); i < count; i++ {
		gap, length := intervals[i].gap, intervals[i].length
		at := offsets[intervals[i].rank]
		var e error
		if e != nil || length == 0 || length > accountDeltaValueMax || gap > uint64(len(literal))-lp || length > uint64(len(values))-vp || gap+length > n-uint64(len(dst)) {
			return nil, stats, errAccountDelta
		}
		dst = append(dst, literal[lp:lp+gap]...)
		dst = append(dst, values[at:at+length]...)
		lp += gap
		vp += length
	}
	if len(lr.p) != 0 || vp != uint64(len(values)) || n-uint64(len(dst)) != uint64(len(literal))-lp {
		return nil, stats, errAccountDelta
	}
	dst = append(dst, literal[lp:]...)
	sum := sha256.Sum256(dst)
	if !bytes.Equal(sum[:], p[24:56]) {
		return nil, stats, errAccountDelta
	}
	return dst, stats, nil
}

// Templates group similarly shaped canonical account envelopes, including
// values from different owners. No owner/field is omitted from reconstruction.
// A collision only changes the candidate base; complete byte deltas and the
// canonical digest still determine the decoded value.
func accountDeltaTemplateNotes(raw []byte, notes []accountDeltaValue) []accountDeltaValue {
	out := make([]accountDeltaValue, len(notes))
	groups := make(map[string]uint32)
	for i, n := range notes {
		value := raw[n.Offset : n.Offset+n.Length]
		k := string(binary.BigEndian.AppendUint32(nil, n.Length)) + string(value[:min(8, len(value))])
		id, ok := groups[k]
		if !ok {
			id = uint32(len(groups))
			groups[k] = id
		}
		out[i] = n
		out[i].KeyID = id
	}
	return out
}
