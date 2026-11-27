package snapshots

import (
	"strconv"
	"unicode/utf8"
)

const jsonHex = "0123456789abcdef"

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
			buf = append(buf, "\ufffd"...)
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

func estimateManifestJSONSize(m *Manifest) int {
	size := 384 + len(m.CommitmentScheme)
	if m.Chain != nil {
		size += 128 + len(m.Chain.GenesisHash) + len(m.Chain.ForkConfigHash)
	}
	if m.Progress != nil {
		size += 256
	}
	for i := range m.Segments {
		size += estimateSegmentRefJSONSize(&m.Segments[i])
	}
	for i := range m.Retired {
		size += estimateSegmentRefJSONSize(&m.Retired[i])
	}
	return size
}

func estimateSegmentRefJSONSize(seg *SegmentRef) int {
	// Typical compact JSON wrapper and integer fields for SegmentRef (~220B)
	// plus variable string fields, avoiding a second slice growth allocation.
	return 240 + len(seg.Dataset) + len(seg.Kind) + len(seg.Path) + len(seg.Checksum) + len(seg.LookupRoot)
}
