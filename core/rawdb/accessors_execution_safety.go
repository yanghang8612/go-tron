package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

const (
	executionSafetyIncidentEncodingVersion = byte(1)
	executionSafetyIncidentEncodedSize     = 1 + 1 + 8 + common.HashLength
	executionSafetyQualificationVersion    = byte(1)
)

// ExecutionSafetyIncidentKind identifies the event that permanently
// disqualified a datadir from speculative-execution rollout. Values are part
// of a local on-disk format and must not be renumbered.
type ExecutionSafetyIncidentKind byte

const (
	ExecutionSafetyIncidentSpeculativePublication ExecutionSafetyIncidentKind = iota + 1
	ExecutionSafetyIncidentCreateTransferRepair
	ExecutionSafetyIncidentParallelVMRepair
	ExecutionSafetyIncidentCOSTRepair
	ExecutionSafetyIncidentWINKCodeRepair
	ExecutionSafetyIncidentHistoricalRepairUnknown
)

func (k ExecutionSafetyIncidentKind) valid() bool {
	return k >= ExecutionSafetyIncidentSpeculativePublication && k <= ExecutionSafetyIncidentHistoricalRepairUnknown
}

func (k ExecutionSafetyIncidentKind) String() string {
	switch k {
	case ExecutionSafetyIncidentSpeculativePublication:
		return "speculative-publication"
	case ExecutionSafetyIncidentCreateTransferRepair:
		return "create-transfer-failure-repair"
	case ExecutionSafetyIncidentParallelVMRepair:
		return "parallel-vm-missed-payment-repair"
	case ExecutionSafetyIncidentCOSTRepair:
		return "cost-missed-reward-repair"
	case ExecutionSafetyIncidentWINKCodeRepair:
		return "wink-missing-runtime-repair"
	case ExecutionSafetyIncidentHistoricalRepairUnknown:
		return "historical-repair-status-unknown"
	default:
		return fmt.Sprintf("unknown-%d", byte(k))
	}
}

// WriteExecutionSafetyQualification records that a safety-aware binary saw
// this datadir before the first known mainnet repair height. Any repair that
// subsequently activates persists the stronger incident marker, which always
// takes precedence. The marker is node-local rollout evidence, not consensus
// state.
func WriteExecutionSafetyQualification(db ethdb.KeyValueWriter) error {
	if db == nil {
		return errors.New("rawdb: nil execution safety qualification writer")
	}
	if err := db.Put(executionSafetyQualifiedKey, []byte{executionSafetyQualificationVersion}); err != nil {
		return fmt.Errorf("rawdb: write execution safety qualification: %w", err)
	}
	if syncer, ok := db.(interface{ SyncKeyValue() error }); ok {
		if err := syncer.SyncKeyValue(); err != nil {
			return fmt.Errorf("rawdb: sync execution safety qualification: %w", err)
		}
	}
	return nil
}

// ReadExecutionSafetyQualification reads the fail-closed rollout credential.
// Corrupt or unknown encodings are errors rather than absence so startup
// cannot silently reinterpret damaged local metadata as safe.
func ReadExecutionSafetyQualification(db ethdb.KeyValueReader) (bool, error) {
	if db == nil {
		return false, nil
	}
	exists, err := db.Has(executionSafetyQualifiedKey)
	if err != nil {
		return false, fmt.Errorf("rawdb: read execution safety qualification presence: %w", err)
	}
	if !exists {
		return false, nil
	}
	encoded, err := db.Get(executionSafetyQualifiedKey)
	if err != nil {
		return false, fmt.Errorf("rawdb: read execution safety qualification: %w", err)
	}
	if len(encoded) != 1 || encoded[0] != executionSafetyQualificationVersion {
		return false, fmt.Errorf("rawdb: malformed execution safety qualification %x", encoded)
	}
	return true, nil
}

// ExecutionSafetyIncident is node-local rollout evidence. It is deliberately
// outside all consensus state and commitment keyspaces.
type ExecutionSafetyIncident struct {
	Kind      ExecutionSafetyIncidentKind
	BlockNum  uint64
	BlockHash common.Hash
}

// WriteExecutionSafetyIncident durably records that this datadir must not
// resume speculative publication. Production Pebble databases expose
// SyncKeyValue; forcing it after Put closes the crash window left by their
// normal asynchronous write mode. Other ethdb implementations retain their
// own Put durability contract.
func WriteExecutionSafetyIncident(db ethdb.KeyValueWriter, incident ExecutionSafetyIncident) error {
	if db == nil {
		return errors.New("rawdb: nil execution safety incident writer")
	}
	if !incident.Kind.valid() {
		return fmt.Errorf("rawdb: invalid execution safety incident kind %d", incident.Kind)
	}
	encoded := make([]byte, executionSafetyIncidentEncodedSize)
	encoded[0] = executionSafetyIncidentEncodingVersion
	encoded[1] = byte(incident.Kind)
	binary.BigEndian.PutUint64(encoded[2:10], incident.BlockNum)
	copy(encoded[10:], incident.BlockHash[:])
	if err := db.Put(executionSafetyIncidentKey, encoded); err != nil {
		return fmt.Errorf("rawdb: write execution safety incident: %w", err)
	}
	if syncer, ok := db.(interface{ SyncKeyValue() error }); ok {
		if err := syncer.SyncKeyValue(); err != nil {
			return fmt.Errorf("rawdb: sync execution safety incident: %w", err)
		}
	}
	return nil
}

// ReadExecutionSafetyIncident reads the persistent fail-closed marker.
// Malformed markers are errors rather than absence so startup cannot silently
// re-enable speculative execution after local metadata corruption.
func ReadExecutionSafetyIncident(db ethdb.KeyValueReader) (ExecutionSafetyIncident, bool, error) {
	if db == nil {
		return ExecutionSafetyIncident{}, false, nil
	}
	exists, err := db.Has(executionSafetyIncidentKey)
	if err != nil {
		return ExecutionSafetyIncident{}, false, fmt.Errorf("rawdb: read execution safety incident presence: %w", err)
	}
	if !exists {
		return ExecutionSafetyIncident{}, false, nil
	}
	encoded, err := db.Get(executionSafetyIncidentKey)
	if err != nil {
		return ExecutionSafetyIncident{}, false, fmt.Errorf("rawdb: read execution safety incident: %w", err)
	}
	if len(encoded) != executionSafetyIncidentEncodedSize {
		return ExecutionSafetyIncident{}, true, fmt.Errorf("rawdb: malformed execution safety incident length %d", len(encoded))
	}
	if encoded[0] != executionSafetyIncidentEncodingVersion {
		return ExecutionSafetyIncident{}, true, fmt.Errorf("rawdb: unsupported execution safety incident version %d", encoded[0])
	}
	incident := ExecutionSafetyIncident{
		Kind:     ExecutionSafetyIncidentKind(encoded[1]),
		BlockNum: binary.BigEndian.Uint64(encoded[2:10]),
	}
	copy(incident.BlockHash[:], encoded[10:])
	if !incident.Kind.valid() {
		return ExecutionSafetyIncident{}, true, fmt.Errorf("rawdb: invalid persisted execution safety incident kind %d", incident.Kind)
	}
	return incident, true, nil
}
