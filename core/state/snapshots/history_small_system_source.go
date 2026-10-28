package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// Called only after hashing every physical byte of this exact accessor against
// its manifest checksum and checking the history dictionary binding. The
// selective walk saves decoding unrelated owner/slot keys; it does not replace
// physical authentication with a file identity or CRC. Strict import separately
// walks the entire dictionary and compares the exact resulting memberships.
func iterateSmallSystemRoutingDictionaryKeys(ctx context.Context, r io.ReaderAt, size uint64, h stateDomainChangeBinaryAccessorV6Header, visit func([]byte) error) error {
	search := historyAccessorKeySearch{r: r, h: h}
	var records []stateDomainChangeBinaryAccessorV6Record
	var cached uint64
	have := false
	blockAt := func(index uint64) ([]stateDomainChangeBinaryAccessorV6Record, error) {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		if have && cached == index {
			return records, nil
		}
		b, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r, h, index)
		if err != nil {
			return nil, err
		}
		rows, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, size, h, b, uint32(index*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			return nil, err
		}
		cached, records, have = index, rows, true
		return rows, nil
	}
	// Find the first dictionary record >= key, including keys preceding the
	// first directory fence and gaps between adjacent dictionary blocks.
	lower := func(key []byte) ([]byte, error) {
		if h.blockCount == 0 {
			return nil, nil
		}
		_, index, _, err := search.find(key)
		if err != nil {
			return nil, err
		}
		for index < h.blockCount {
			rows, err := blockAt(index)
			if err != nil {
				return nil, err
			}
			at := sort.Search(len(rows), func(i int) bool { return bytes.Compare(rows[i].key, key) >= 0 })
			if at < len(rows) {
				return rows[at].key, nil
			}
			index++
		}
		return nil, nil
	}
	prefix := func(key []byte) error {
		if h.blockCount == 0 {
			return nil
		}
		_, index, _, err := search.find(key)
		if err != nil {
			return err
		}
		for index < h.blockCount {
			rows, err := blockAt(index)
			if err != nil {
				return err
			}
			for _, row := range rows {
				if bytes.Compare(row.key, key) < 0 {
					continue
				}
				if !bytes.HasPrefix(row.key, key) {
					return nil
				}
				if err := visit(row.key); err != nil {
					return err
				}
			}
			index++
		}
		return nil
	}
	for _, flat := range []rawdb.StateFlatDomain{rawdb.StateFlatDomainAccountLatest, rawdb.StateFlatDomainKVGeneration} {
		for _, id := range []common.AccountID{common.SystemAccountID, {}} {
			key := append([]byte{byte(flat)}, id[:]...)
			found, err := lower(key)
			if err != nil {
				return err
			}
			if bytes.Equal(found, key) {
				if err := visit(found); err != nil {
					return err
				}
			}
		}
	}
	owner := append([]byte{byte(rawdb.StateFlatDomainKVLatest)}, common.SystemAccountID[:]...)
	found, err := lower(owner)
	if err != nil {
		return err
	}
	generations := 0
	for bytes.HasPrefix(found, owner) {
		if len(found) < 31 {
			return errors.New("snapshots: truncated system routing dictionary key")
		}
		generations++
		if generations > smallSystemHistoryRoutingMaxKeys {
			return errSystemHistoryRoutingBudget
		}
		gen := binary.BigEndian.Uint64(found[21:29])
		for _, domain := range []kvdomains.KVDomain{kvdomains.SystemDynamicProperty, kvdomains.SystemForkVote} {
			key := append([]byte(nil), owner...)
			key = binary.BigEndian.AppendUint64(key, gen)
			key = binary.BigEndian.AppendUint16(key, uint16(domain))
			if err := prefix(key); err != nil {
				return err
			}
		}
		if gen == math.MaxUint64 {
			break
		}
		next := append([]byte(nil), owner...)
		next = binary.BigEndian.AppendUint64(next, gen+1)
		found, err = lower(next)
		if err != nil {
			return err
		}
	}
	return contextError(ctx)
}
