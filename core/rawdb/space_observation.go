package rawdb

import (
	"fmt"
	"os"

	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

const diskSpaceObserverEnv = "GTRON_DB_SPACE_OBSERVER"

// withDiskSpaceObservation resolves the process switch once before a writable
// database is opened. The rawdb entry points always select the schema-owned
// families; caller-supplied ranges cannot bypass the disabled default.
// Read-only database opens deliberately do not call this helper.
func withDiskSpaceObservation(tune PebbleOptions) (PebbleOptions, error) {
	switch value := os.Getenv(diskSpaceObserverEnv); value {
	case "", "0":
		tune.DiskSpaceRanges = nil
	case "1":
		ranges := DiskSpaceObservationRanges()
		tune.DiskSpaceRanges = make([]pebbledb.DiskSpaceRange, len(ranges))
		for i, keyspace := range ranges {
			tune.DiskSpaceRanges[i] = pebbledb.DiskSpaceRange{
				Name: keyspace.Name, Start: keyspace.Start, End: keyspace.End,
			}
		}
	default:
		return tune, fmt.Errorf("%s must be empty, 0, or 1 (got %q)", diskSpaceObserverEnv, value)
	}
	return tune, nil
}
