package state

import (
	"encoding/binary"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Phase 3d roots the Bancor exchange store into the reserved system account's
// SystemExchange KV so it rewinds with the full state root, replacing the flat
// `ex-`/`ex2-` rawdb buckets.
//
// java-tron keeps exchanges in two stores that coexist forever once V2 exists:
//
//   - V1 (ExchangeStore, `ex-`): the legacy bucket, keyed by exchange id, whose
//     token ids are the human-readable asset names. Frozen after the
//     AllowSameTokenName fork — no new V1 records are written post-fork, but the
//     historical ones must stay self-describing in every pre-fork root.
//   - V2 (ExchangeV2Store, `ex2-`): the post-fork bucket, identical layout but
//     with numeric token ids. Always written; the only bucket read post-fork.
//
// Both legs are rooted so a state root taken before the fork fully describes the
// exchange set under either bucket (the locked design decision). Within the one
// domain the two legs are disambiguated by a 1-byte discriminator prefixed to
// the 8-byte big-endian exchange id, mirroring the old two-prefix split:
//
//	V1 key: 0x00 || u64-BE(id)
//	V2 key: 0x01 || u64-BE(id)
//
// The value encoding reuses the existing Exchange proto wire format verbatim
// (proto.Marshal), so no new on-disk encoding lineage is introduced — a rooted
// record is byte-identical to what the flat bucket held.
//
// Enumeration (ListExchanges) cannot prefix-scan the KV trie because its keys
// are Keccak256(domain||key) hashes; instead it walks ids 1..latestExchangeNum,
// exactly as java-tron's RpcApiService.getExchangeList does off
// getLatestExchangeNum. The caller supplies the count from the same rooted
// snapshot it enumerates against.
const (
	exchangeKVDiscriminatorV1 byte = 0x00
	exchangeKVDiscriminatorV2 byte = 0x01
)

// exchangeKVKey is the in-domain logical key: discriminator || u64-BE(id).
func exchangeKVKey(discriminator byte, id int64) []byte {
	k := make([]byte, 1+8)
	k[0] = discriminator
	binary.BigEndian.PutUint64(k[1:], uint64(id))
	return k
}

// readExchange resolves one exchange leg, swallowing a KV error to nil to match
// the prior rawdb reader's defensive behavior (read sites treat nil as absent).
func (s *StateDB) readExchange(discriminator byte, id int64) *corepb.Exchange {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemExchange, exchangeKVKey(discriminator, id))
	if err != nil || !ok || len(raw) == 0 {
		return nil
	}
	ex := &corepb.Exchange{}
	if err := proto.Unmarshal(raw, ex); err != nil {
		return nil
	}
	return ex
}

// writeExchange stages one exchange leg into the system-KV. The error is non-nil
// only for a proto marshal failure or an unregistered domain (a programmer
// error), since SystemExchange is registered at init.
func (s *StateDB) writeExchange(discriminator byte, ex *corepb.Exchange) error {
	data, err := proto.Marshal(ex)
	if err != nil {
		return err
	}
	return s.SystemKVPut(kvdomains.SystemExchange, exchangeKVKey(discriminator, ex.ExchangeId), data)
}

// ReadExchange returns the rooted V1 (legacy) exchange with id, or nil if absent.
func (s *StateDB) ReadExchange(id int64) *corepb.Exchange {
	return s.readExchange(exchangeKVDiscriminatorV1, id)
}

// ReadExchangeV2 returns the rooted V2 exchange with id, or nil if absent.
func (s *StateDB) ReadExchangeV2(id int64) *corepb.Exchange {
	return s.readExchange(exchangeKVDiscriminatorV2, id)
}

// WriteExchange stages the V1 (legacy) exchange keyed by its ExchangeId.
func (s *StateDB) WriteExchange(ex *corepb.Exchange) error {
	return s.writeExchange(exchangeKVDiscriminatorV1, ex)
}

// WriteExchangeV2 stages the V2 exchange keyed by its ExchangeId.
func (s *StateDB) WriteExchangeV2(ex *corepb.Exchange) error {
	return s.writeExchange(exchangeKVDiscriminatorV2, ex)
}

// DeleteExchange stages a tombstone for the V1 (legacy) exchange with id.
func (s *StateDB) DeleteExchange(id int64) error {
	return s.SystemKVDelete(kvdomains.SystemExchange, exchangeKVKey(exchangeKVDiscriminatorV1, id))
}

// DeleteExchangeV2 stages a tombstone for the V2 exchange with id.
func (s *StateDB) DeleteExchangeV2(id int64) error {
	return s.SystemKVDelete(kvdomains.SystemExchange, exchangeKVKey(exchangeKVDiscriminatorV2, id))
}

// ListExchanges enumerates the V1 (legacy) bucket over ids 1..latestID,
// skipping any id with no stored record. latestID is the caller's
// latest_exchange_num read from the same rooted snapshot.
func (s *StateDB) ListExchanges(latestID int64) []*corepb.Exchange {
	return s.listExchanges(exchangeKVDiscriminatorV1, latestID)
}

// ListExchangesV2 enumerates the V2 bucket over ids 1..latestID.
func (s *StateDB) ListExchangesV2(latestID int64) []*corepb.Exchange {
	return s.listExchanges(exchangeKVDiscriminatorV2, latestID)
}

func (s *StateDB) listExchanges(discriminator byte, latestID int64) []*corepb.Exchange {
	var out []*corepb.Exchange
	for id := int64(1); id <= latestID; id++ {
		if ex := s.readExchange(discriminator, id); ex != nil {
			out = append(out, ex)
		}
	}
	return out
}
