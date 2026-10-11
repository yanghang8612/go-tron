package statecodec

import (
	"bytes"
	"encoding/binary"
	"fmt"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// DelegationPageEntries bounds the amount rewritten by one ordinary edge edit.
const DelegationPageEntries = 128

var delegationDescriptorMagic = []byte("DLP1")
var delegationPageMagic = []byte("DPP1")

// MarshalDelegationDescriptor preserves scalar and unknown protobuf fields in
// a small row. Lists live exclusively in ordered, independently versioned pages.
func MarshalDelegationDescriptor(record *corepb.DelegatedResourceAccountIndex, fromPages, toPages uint64) ([]byte, error) {
	if record == nil {
		return nil, fmt.Errorf("nil delegation descriptor")
	}
	metadata := &corepb.DelegatedResourceAccountIndex{Account: record.Account, Timestamp: record.Timestamp}
	metadata.ProtoReflect().SetUnknown(record.ProtoReflect().GetUnknown())
	encoded, err := MarshalDelegationIndex(metadata)
	if err != nil {
		return nil, err
	}
	out := append([]byte{}, delegationDescriptorMagic...)
	out = binary.BigEndian.AppendUint64(out, fromPages)
	out = binary.BigEndian.AppendUint64(out, toPages)
	return append(out, encoded...), nil
}

func UnmarshalDelegationDescriptor(data []byte) (*corepb.DelegatedResourceAccountIndex, uint64, uint64, error) {
	if len(data) < 20 || !bytes.Equal(data[:4], delegationDescriptorMagic) {
		return nil, 0, 0, fmt.Errorf("aggregate delegation requires DLP1 pages; rebuild the database from genesis")
	}
	record, err := UnmarshalDelegationIndex(data[20:])
	if err != nil {
		return nil, 0, 0, fmt.Errorf("delegation descriptor metadata: %w", err)
	}
	if len(record.FromAccounts) != 0 || len(record.ToAccounts) != 0 {
		return nil, 0, 0, fmt.Errorf("delegation descriptor contains embedded lists")
	}
	from, to := binary.BigEndian.Uint64(data[4:12]), binary.BigEndian.Uint64(data[12:20])
	maxPages := uint64(^uint(0)>>1) / 24
	if from > maxPages || to > maxPages || from > maxPages-to {
		return nil, 0, 0, fmt.Errorf("delegation descriptor page count overflows addressable memory")
	}
	return record, from, to, nil
}

// Pages compact removals within the page only. Empty pages remain addressed so
// later page numbers never change and list order survives removes and re-adds.
func MarshalDelegationPage(list [][]byte) ([]byte, error) {
	if len(list) > DelegationPageEntries {
		return nil, fmt.Errorf("delegation page has %d entries (limit %d)", len(list), DelegationPageEntries)
	}
	size := 4 + delegationUvarintSize(len(list))
	for _, address := range list {
		size += delegationUvarintSize(len(address)) + len(address)
	}
	out := append(make([]byte, 0, size), delegationPageMagic...)
	out = binary.AppendUvarint(out, uint64(len(list)))
	for _, address := range list {
		out = binary.AppendUvarint(out, uint64(len(address)))
		out = append(out, address...)
	}
	return out, nil
}

func UnmarshalDelegationPage(data []byte) ([][]byte, error) {
	if len(data) < 5 || !bytes.Equal(data[:4], delegationPageMagic) {
		return nil, fmt.Errorf("invalid delegation page header")
	}
	count, n := binary.Uvarint(data[4:])
	if n <= 0 || count > DelegationPageEntries {
		return nil, fmt.Errorf("invalid delegation page count")
	}
	// Keep one owned byte arena and one header array per bounded page.
	owned := bytes.Clone(data)
	rest := owned[4+n:]
	out := make([][]byte, 0, int(count))
	for i := uint64(0); i < count; i++ {
		size, n := binary.Uvarint(rest)
		if n <= 0 {
			return nil, fmt.Errorf("invalid delegation page entry length")
		}
		rest = rest[n:]
		if size > uint64(len(rest)) {
			return nil, fmt.Errorf("truncated delegation page entry")
		}
		out = append(out, rest[:int(size):int(size)])
		rest = rest[int(size):]
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("trailing delegation page bytes")
	}
	return out, nil
}
