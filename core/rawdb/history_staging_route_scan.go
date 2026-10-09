package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
)

// ScanRoutes is a lightweight scheduling/diagnostic page. It deliberately
// avoids claim proofs, receipts and cold bindings and cannot authorize work.
func (m *HistoryStagingManager) ScanRoutes(ctx context.Context, after []byte, maxRows uint64) ([]HistoryStagingRoute, []byte, bool, error) {
	if m == nil || ctx == nil || maxRows == 0 || maxRows > 256 || (len(after) != 0 && len(after) != 8) {
		return nil, nil, false, errors.New("rawdb: invalid staging route scan")
	}
	it := m.hot.NewIterator(historyStagingRoutePrefix, after)
	defer it.Release()
	var routes []HistoryStagingRoute
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, false, err
		}
		key := it.Key()
		if len(key) != len(historyStagingRoutePrefix)+8 || !bytes.HasPrefix(key, historyStagingRoutePrefix) {
			return nil, nil, false, ErrHistoryStagingConflict
		}
		bucket := binary.BigEndian.Uint64(key[len(historyStagingRoutePrefix):])
		if len(after) != 0 && bucket <= binary.BigEndian.Uint64(after) {
			continue
		}
		if uint64(len(routes)) == maxRows {
			next := make([]byte, 8)
			binary.BigEndian.PutUint64(next, routes[len(routes)-1].Bucket)
			return routes, next, false, nil
		}
		var route HistoryStagingRoute
		if err := decodeHistoryStaging(it.Value(), &route); err != nil {
			return nil, nil, false, err
		}
		if route.Bucket != bucket || route.Version != HistoryStagingFormatVersion {
			return nil, nil, false, ErrHistoryStagingConflict
		}
		routes = append(routes, route)
	}
	return routes, nil, true, it.Error()
}
