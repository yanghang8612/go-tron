package snapshots

import (
	"bytes"
	"encoding/json"
	"strconv"
	"unicode/utf8"
)

const jsonHex = "0123456789abcdef"

// jsonInvalidUTF8 matches the active Go toolchain's encoding/json invalid-UTF-8
// replacement (Go 1.25 emits `\ufffd`; Go 1.27 emits literal UTF-8 U+FFFD).
var jsonInvalidUTF8 = func() string {
	b, err := json.Marshal("\xff")
	if err != nil || len(b) < 2 {
		return `\ufffd`
	}
	return string(b[1 : len(b)-1])
}()

// safeJSONByte mirrors encoding/json's HTML-escaped ASCII safeSet so
// marshalManifestJSON matches json.Marshal byte-for-byte without reflection.
var safeJSONByte = [128]bool{
	' ': true, '!': true, '#': true, '$': true, '%': true, '\'': true, '(': true, ')': true,
	'*': true, '+': true, ',': true, '-': true, '.': true, '/': true,
	'0': true, '1': true, '2': true, '3': true, '4': true, '5': true, '6': true, '7': true, '8': true, '9': true,
	':': true, ';': true, '=': true, '?': true, '@': true,
	'A': true, 'B': true, 'C': true, 'D': true, 'E': true, 'F': true, 'G': true, 'H': true, 'I': true, 'J': true,
	'K': true, 'L': true, 'M': true, 'N': true, 'O': true, 'P': true, 'Q': true, 'R': true, 'S': true, 'T': true,
	'U': true, 'V': true, 'W': true, 'X': true, 'Y': true, 'Z': true,
	'[': true, ']': true, '^': true, '_': true, '`': true,
	'a': true, 'b': true, 'c': true, 'd': true, 'e': true, 'f': true, 'g': true, 'h': true, 'i': true, 'j': true,
	'k': true, 'l': true, 'm': true, 'n': true, 'o': true, 'p': true, 'q': true, 'r': true, 's': true, 't': true,
	'u': true, 'v': true, 'w': true, 'x': true, 'y': true, 'z': true,
	'{': true, '|': true, '}': true, '~': true, 0x7f: true,
}

func marshalManifestJSON(m *Manifest) ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	buf := make([]byte, 0, estimateManifestJSONSize(m))
	buf = append(buf, '{')
	wrote := false

	if m.LatestStateTxNum != 0 {
		buf = append(buf, `"latestStateTxNum":`...)
		buf = strconv.AppendUint(buf, m.LatestStateTxNum, 10)
		wrote = true
	}

	if wrote {
		buf = append(buf, ',')
	}
	buf = append(buf, `"sourceRows":[`...)
	buf = strconv.AppendUint(buf, m.SourceRows[0], 10)
	buf = append(buf, ',')
	buf = strconv.AppendUint(buf, m.SourceRows[1], 10)
	buf = append(buf, ',')
	buf = strconv.AppendUint(buf, m.SourceRows[2], 10)
	buf = append(buf, `],"commitmentScheme":`...)
	buf = appendJSONString(buf, m.CommitmentScheme)
	buf = append(buf, `,"elementVersion":`...)
	buf = strconv.AppendUint(buf, uint64(m.ElementVersion), 10)
	buf = append(buf, `,"version":`...)
	buf = strconv.AppendUint(buf, uint64(m.Version), 10)

	if m.Generation != 0 {
		buf = append(buf, `,"generation":`...)
		buf = strconv.AppendUint(buf, m.Generation, 10)
	}
	if m.HistoryStagingResetEpoch != 0 {
		buf = append(buf, `,"historyStagingResetEpoch":`...)
		buf = strconv.AppendUint(buf, m.HistoryStagingResetEpoch, 10)
	}

	buf = append(buf, `,"publishedUnix":`...)
	buf = strconv.AppendInt(buf, m.PublishedUnix, 10)
	buf = append(buf, `,"visibleTxStart":`...)
	buf = strconv.AppendUint(buf, m.VisibleTxStart, 10)
	buf = append(buf, `,"visibleTxEnd":`...)
	buf = strconv.AppendUint(buf, m.VisibleTxEnd, 10)

	if m.Chain != nil {
		buf = append(buf, `,"chain":`...)
		buf = appendChainIdentityJSON(buf, m.Chain)
	}
	if m.Progress != nil {
		buf = append(buf, `,"progress":`...)
		buf = appendProgressJSON(buf, m.Progress)
	}

	buf = append(buf, `,"segments":`...)
	if m.Segments == nil {
		buf = append(buf, "null"...)
	} else {
		buf = appendSegmentRefsJSON(buf, m.Segments)
	}

	if len(m.Retired) != 0 {
		buf = append(buf, `,"retired":`...)
		buf = appendSegmentRefsJSON(buf, m.Retired)
	}

	buf = append(buf, '}')
	// manifestCacheCharge accounts for cap(data). Avoid retaining oversized
	// spare capacity if rare string escaping triggered a slice growth.
	if cap(buf)-len(buf) > 64<<10 {
		buf = bytes.Clone(buf)
	}
	return buf, nil
}

func appendChainIdentityJSON(buf []byte, c *ChainIdentity) []byte {
	buf = append(buf, `{"chainId":`...)
	buf = strconv.AppendInt(buf, c.ChainID, 10)
	buf = append(buf, `,"networkId":`...)
	buf = strconv.AppendInt(buf, int64(c.NetworkID), 10)
	buf = append(buf, `,"genesisHash":`...)
	buf = appendJSONString(buf, c.GenesisHash)
	if c.ForkConfigHash != "" {
		buf = append(buf, `,"forkConfigHash":`...)
		buf = appendJSONString(buf, c.ForkConfigHash)
	}
	buf = append(buf, '}')
	return buf
}

func appendProgressJSON(buf []byte, p *Progress) []byte {
	buf = append(buf, '{')
	wrote := false
	writeUint := func(key string, val uint64) {
		if val == 0 {
			return
		}
		if wrote {
			buf = append(buf, ',')
		}
		buf = append(buf, key...)
		buf = strconv.AppendUint(buf, val, 10)
		wrote = true
	}
	writeUint(`"latestBuildTxNum":`, p.LatestBuildTxNum)
	writeUint(`"historyBuildTxNum":`, p.HistoryBuildTxNum)
	writeUint(`"accessorBuildTxNum":`, p.AccessorBuildTxNum)
	writeUint(`"commitmentFlushTxNum":`, p.CommitmentFlushTxNum)
	writeUint(`"hotPruneTxNum":`, p.HotPruneTxNum)
	writeUint(`"hotPruneBlockNum":`, p.HotPruneBlockNum)
	writeUint(`"stateChangeIndexPruneBlockNum":`, p.StateChangeIndexPruneBlockNum)
	buf = append(buf, '}')
	return buf
}

func appendSegmentRefsJSON(buf []byte, refs []SegmentRef) []byte {
	buf = append(buf, '[')
	for i := range refs {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendSegmentRefJSON(buf, &refs[i])
	}
	buf = append(buf, ']')
	return buf
}

func appendSegmentRefJSON(buf []byte, seg *SegmentRef) []byte {
	buf = append(buf, '{')
	wrote := false
	if seg.Rows != 0 {
		buf = append(buf, `"rows":`...)
		buf = strconv.AppendUint(buf, seg.Rows, 10)
		wrote = true
	}
	if seg.Dataset != "" {
		if wrote {
			buf = append(buf, ',')
		}
		buf = append(buf, `"dataset":`...)
		buf = appendJSONString(buf, string(seg.Dataset))
		wrote = true
	}
	if seg.Domain != 0 {
		if wrote {
			buf = append(buf, ',')
		}
		buf = append(buf, `"domain":`...)
		buf = strconv.AppendUint(buf, uint64(seg.Domain), 10)
		wrote = true
	}
	if wrote {
		buf = append(buf, ',')
	}
	buf = append(buf, `"kind":`...)
	buf = appendJSONString(buf, string(seg.Kind))
	buf = append(buf, `,"fromTxNum":`...)
	buf = strconv.AppendUint(buf, seg.FromTxNum, 10)
	buf = append(buf, `,"toTxNum":`...)
	buf = strconv.AppendUint(buf, seg.ToTxNum, 10)
	if seg.AggregationSteps != 0 {
		buf = append(buf, `,"aggregationSteps":`...)
		buf = strconv.AppendUint(buf, seg.AggregationSteps, 10)
	}
	buf = append(buf, `,"path":`...)
	buf = appendJSONString(buf, seg.Path)
	buf = append(buf, `,"size":`...)
	buf = strconv.AppendUint(buf, seg.Size, 10)
	if seg.Checksum != "" {
		buf = append(buf, `,"checksum":`...)
		buf = appendJSONString(buf, seg.Checksum)
	}
	if seg.LookupRoot != "" {
		buf = append(buf, `,"lookupRoot":`...)
		buf = appendJSONString(buf, seg.LookupRoot)
	}
	buf = append(buf, '}')
	return buf
}

func appendJSONString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if safeJSONByte[b] {
				i++
				continue
			}
			buf = append(buf, s[start:i]...)
			switch b {
			case '\\', '"':
				buf = append(buf, '\\', b)
			case '\b':
				buf = append(buf, '\\', 'b')
			case '\f':
				buf = append(buf, '\\', 'f')
			case '\n':
				buf = append(buf, '\\', 'n')
			case '\r':
				buf = append(buf, '\\', 'r')
			case '\t':
				buf = append(buf, '\\', 't')
			default:
				buf = append(buf, '\\', 'u', '0', '0', jsonHex[b>>4], jsonHex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			buf = append(buf, s[start:i]...)
			buf = append(buf, jsonInvalidUTF8...)
			i += size
			start = i
			continue
		}
		if c == '\u2028' || c == '\u2029' {
			buf = append(buf, s[start:i]...)
			buf = append(buf, '\\', 'u', '2', '0', '2', jsonHex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	buf = append(buf, s[start:]...)
	buf = append(buf, '"')
	return buf
}

func uintDecimalLen(v uint64) int {
	n := 1
	for v >= 10 {
		v /= 10
		n++
	}
	return n
}

func intDecimalLen(v int64) int {
	if v < 0 {
		return 1 + uintDecimalLen(uint64(-v))
	}
	return uintDecimalLen(uint64(v))
}

func estimateChainIdentityJSONSize(c *ChainIdentity) int {
	size := len(`{"chainId":`) + intDecimalLen(c.ChainID) +
		len(`,"networkId":`) + intDecimalLen(int64(c.NetworkID)) +
		len(`,"genesisHash":`) + 2 + len(c.GenesisHash) + 1
	if c.ForkConfigHash != "" {
		size += len(`,"forkConfigHash":`) + 2 + len(c.ForkConfigHash)
	}
	return size
}

func estimateProgressJSONSize(p *Progress) int {
	size := 2
	wrote := false
	addUint := func(key string, val uint64) {
		if val == 0 {
			return
		}
		if wrote {
			size++
		}
		size += len(key) + uintDecimalLen(val)
		wrote = true
	}
	addUint(`"latestBuildTxNum":`, p.LatestBuildTxNum)
	addUint(`"historyBuildTxNum":`, p.HistoryBuildTxNum)
	addUint(`"accessorBuildTxNum":`, p.AccessorBuildTxNum)
	addUint(`"commitmentFlushTxNum":`, p.CommitmentFlushTxNum)
	addUint(`"hotPruneTxNum":`, p.HotPruneTxNum)
	addUint(`"hotPruneBlockNum":`, p.HotPruneBlockNum)
	addUint(`"stateChangeIndexPruneBlockNum":`, p.StateChangeIndexPruneBlockNum)
	return size
}

func estimateSegmentRefsJSONSize(refs []SegmentRef) int {
	size := 2
	if len(refs) > 1 {
		size += len(refs) - 1
	}
	for i := range refs {
		size += estimateSegmentRefJSONSize(&refs[i])
	}
	return size
}

func estimateManifestJSONSize(m *Manifest) int {
	if m == nil {
		return 4
	}
	size := 2
	wrote := false
	if m.LatestStateTxNum != 0 {
		size += len(`"latestStateTxNum":`) + uintDecimalLen(m.LatestStateTxNum)
		wrote = true
	}
	if wrote {
		size++
	}
	size += len(`"sourceRows":[`) +
		uintDecimalLen(m.SourceRows[0]) + 1 +
		uintDecimalLen(m.SourceRows[1]) + 1 +
		uintDecimalLen(m.SourceRows[2]) +
		len(`],"commitmentScheme":`) + 2 + len(m.CommitmentScheme) +
		len(`,"elementVersion":`) + uintDecimalLen(uint64(m.ElementVersion)) +
		len(`,"version":`) + uintDecimalLen(uint64(m.Version))
	if m.Generation != 0 {
		size += len(`,"generation":`) + uintDecimalLen(m.Generation)
	}
	if m.HistoryStagingResetEpoch != 0 {
		size += len(`,"historyStagingResetEpoch":`) + uintDecimalLen(m.HistoryStagingResetEpoch)
	}
	size += len(`,"publishedUnix":`) + intDecimalLen(m.PublishedUnix) +
		len(`,"visibleTxStart":`) + uintDecimalLen(m.VisibleTxStart) +
		len(`,"visibleTxEnd":`) + uintDecimalLen(m.VisibleTxEnd)
	if m.Chain != nil {
		size += len(`,"chain":`) + estimateChainIdentityJSONSize(m.Chain)
	}
	if m.Progress != nil {
		size += len(`,"progress":`) + estimateProgressJSONSize(m.Progress)
	}
	size += len(`,"segments":`)
	if m.Segments == nil {
		size += 4
	} else {
		size += estimateSegmentRefsJSONSize(m.Segments)
	}
	if len(m.Retired) != 0 {
		size += len(`,"retired":`) + estimateSegmentRefsJSONSize(m.Retired)
	}
	return size
}

func estimateSegmentRefJSONSize(seg *SegmentRef) int {
	size := 2
	wrote := false
	if seg.Rows != 0 {
		size += len(`"rows":`) + uintDecimalLen(seg.Rows)
		wrote = true
	}
	if seg.Dataset != "" {
		if wrote {
			size++
		}
		size += len(`"dataset":`) + 2 + len(seg.Dataset)
		wrote = true
	}
	if seg.Domain != 0 {
		if wrote {
			size++
		}
		size += len(`"domain":`) + uintDecimalLen(uint64(seg.Domain))
		wrote = true
	}
	if wrote {
		size++
	}
	size += len(`"kind":`) + 2 + len(seg.Kind) +
		len(`,"fromTxNum":`) + uintDecimalLen(seg.FromTxNum) +
		len(`,"toTxNum":`) + uintDecimalLen(seg.ToTxNum)
	if seg.AggregationSteps != 0 {
		size += len(`,"aggregationSteps":`) + uintDecimalLen(seg.AggregationSteps)
	}
	size += len(`,"path":`) + 2 + len(seg.Path) +
		len(`,"size":`) + uintDecimalLen(seg.Size)
	if seg.Checksum != "" {
		size += len(`,"checksum":`) + 2 + len(seg.Checksum)
	}
	if seg.LookupRoot != "" {
		size += len(`,"lookupRoot":`) + 2 + len(seg.LookupRoot)
	}
	return size
}
