package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// SystemKVRow identifies one logical row in the rooted system-account state.
// It is exposed for offline diagnostics and corruption tests that must inject
// malformed values without duplicating the canonical key encoding.
type SystemKVRow struct {
	Domain kvdomains.KVDomain
	Key    []byte
}

func systemKVRow(domain kvdomains.KVDomain, key []byte) SystemKVRow {
	return SystemKVRow{Domain: domain, Key: append([]byte(nil), key...)}
}

func WitnessIndexSystemRow() SystemKVRow {
	return systemKVRow(kvdomains.SystemWitnessSchedule, witnessScheduleIndexKey)
}

func PendingVotesSystemRow(voter tcommon.Address) SystemKVRow {
	return systemKVRow(kvdomains.WitnessVoteState, votesStoreKey(voter))
}

func PendingVotesIndexSystemRow() SystemKVRow {
	return systemKVRow(kvdomains.WitnessVoteState, votesStoreIndexKey)
}

func MarketOrderSystemRow(orderID []byte) SystemKVRow {
	return systemKVRow(kvdomains.SystemMarket, marketOrderKVKey(orderID))
}

func MarketAccountOrderSystemRow(ownerAddr []byte) SystemKVRow {
	return systemKVRow(kvdomains.SystemMarket, marketAccountOrderKVKey(ownerAddr))
}

func MarketOrderBookSystemRow(sellTokenID, buyTokenID []byte, pk [16]byte) SystemKVRow {
	return systemKVRow(kvdomains.SystemMarket, marketOrderBookKVKey(sellTokenID, buyTokenID, pk))
}

func MarketPriceListSystemRow(sellTokenID, buyTokenID []byte) SystemKVRow {
	return systemKVRow(kvdomains.SystemMarket, marketPriceListKVKey(sellTokenID, buyTokenID))
}

func MarketPairIndexSystemRow() SystemKVRow {
	return systemKVRow(kvdomains.SystemMarket, marketPairIndexKVKey())
}
