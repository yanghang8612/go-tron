package state

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var (
	delegatedResourceJSONCanonical = metrics.NewRegisteredCounter("state/delegated_resource/json_canonical", nil)
	delegatedResourceJSONFallback  = metrics.NewRegisteredCounter("state/delegated_resource/json_fallback", nil)
)

func decodeDelegatedResource(data []byte) (*rawdb.DelegatedResource, error) {
	dr := new(rawdb.DelegatedResource)
	if decodeCanonicalDelegatedResource(data, dr) {
		delegatedResourceJSONCanonical.Inc(1)
		return dr, nil
	}
	delegatedResourceJSONFallback.Inc(1)
	*dr = rawdb.DelegatedResource{}
	if err := json.Unmarshal(data, dr); err != nil {
		return nil, err
	}
	return dr, nil
}

// decodeCanonicalDelegatedResource recognizes the compact field order and
// fixed-size address arrays emitted by json.Marshal for a DelegatedResource.
// Every unrecognized shape is decoded by encoding/json, preserving its
// compatibility rules and error details.
func decodeCanonicalDelegatedResource(data []byte, dr *rawdb.DelegatedResource) bool {
	data, ok := bytes.CutPrefix(data, []byte(`{"from":[`))
	if !ok {
		return false
	}
	data, ok = consumeDelegatedResourceAddress(data, &dr.From)
	if !ok {
		return false
	}
	data, ok = bytes.CutPrefix(data, []byte(`,"to":[`))
	if !ok {
		return false
	}
	data, ok = consumeDelegatedResourceAddress(data, &dr.To)
	if !ok {
		return false
	}
	for _, field := range [...]struct {
		prefix string
		dest   *int64
	}{
		{`,"frozen_balance_for_bandwidth":`, &dr.FrozenBalanceForBandwidth},
		{`,"frozen_balance_for_energy":`, &dr.FrozenBalanceForEnergy},
		{`,"expire_time_for_bandwidth":`, &dr.ExpireTimeForBandwidth},
		{`,"expire_time_for_energy":`, &dr.ExpireTimeForEnergy},
	} {
		data, ok = bytes.CutPrefix(data, []byte(field.prefix))
		if !ok {
			return false
		}
		end := 0
		if len(data) > 0 && data[0] == '-' {
			end++
		}
		start := end
		for end < len(data) && data[end] >= '0' && data[end] <= '9' {
			if end-start == 19 {
				return false
			}
			end++
		}
		if end == start || (data[start] == '0' && end > 1) {
			return false
		}
		value, err := strconv.ParseInt(string(data[:end]), 10, 64)
		if err != nil {
			return false
		}
		*field.dest = value
		data = data[end:]
	}
	return len(data) == 1 && data[0] == '}'
}

// consumeDelegatedResourceAddress consumes the bytes and closing bracket
// after the opening bracket of a compact JSON address array.
func consumeDelegatedResourceAddress(data []byte, addr *tcommon.Address) ([]byte, bool) {
	for i := range addr {
		if len(data) < 2 || data[0] < '0' || data[0] > '9' {
			return nil, false
		}
		value := int(data[0] - '0')
		end := 1
		if value != 0 {
			for end < len(data) && end < 3 && data[end] >= '0' && data[end] <= '9' {
				value = value*10 + int(data[end]-'0')
				end++
			}
		}
		delimiter := byte(',')
		if i == len(addr)-1 {
			delimiter = ']'
		}
		if value > 255 || end >= len(data) || data[end] != delimiter {
			return nil, false
		}
		addr[i] = byte(value)
		data = data[end+1:]
	}
	return data, true
}

// encodeCanonicalDelegatedResource produces byte-identical compact JSON to
// encoding/json.Marshal for *rawdb.DelegatedResource without reflection.
func encodeCanonicalDelegatedResource(dr *rawdb.DelegatedResource) []byte {
	if dr == nil {
		return []byte("null")
	}
	buf := make([]byte, 0, 320)
	buf = append(buf, `{"from":[`...)
	buf = appendDelegatedResourceAddress(buf, dr.From)
	buf = append(buf, `],"to":[`...)
	buf = appendDelegatedResourceAddress(buf, dr.To)
	buf = append(buf, `],"frozen_balance_for_bandwidth":`...)
	buf = strconv.AppendInt(buf, dr.FrozenBalanceForBandwidth, 10)
	buf = append(buf, `,"frozen_balance_for_energy":`...)
	buf = strconv.AppendInt(buf, dr.FrozenBalanceForEnergy, 10)
	buf = append(buf, `,"expire_time_for_bandwidth":`...)
	buf = strconv.AppendInt(buf, dr.ExpireTimeForBandwidth, 10)
	buf = append(buf, `,"expire_time_for_energy":`...)
	buf = strconv.AppendInt(buf, dr.ExpireTimeForEnergy, 10)
	buf = append(buf, '}')
	return buf
}

func appendDelegatedResourceAddress(buf []byte, addr tcommon.Address) []byte {
	for i, b := range addr {
		if i > 0 {
			buf = append(buf, ',')
		}
		if b >= 100 {
			buf = append(buf, '0'+b/100, '0'+(b/10)%10, '0'+b%10)
		} else if b >= 10 {
			buf = append(buf, '0'+b/10, '0'+b%10)
		} else {
			buf = append(buf, '0'+b)
		}
	}
	return buf
}
