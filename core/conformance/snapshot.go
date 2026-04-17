package conformance

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"google.golang.org/protobuf/proto"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Snapshot is the wire format the capture tool emits per block: java-tron's
// post-state for the range-wide touched-address closure. fixture-digest
// turns this into an OracleEntry. The file is deliberately java-tron-
// neutral — the capture side just fills it in, regardless of whether it
// came from gRPC, HTTP, or a direct DB dump.
type Snapshot struct {
	BlockNum       uint64                      `json:"blockNum"`
	Accounts       []SnapshotAccount           `json:"accounts,omitempty"`
	ContractStates []SnapshotContractState     `json:"contractStates,omitempty"`
	Code           []SnapshotCode              `json:"code,omitempty"`
	DP             map[string]int64            `json:"dp"`
	Closure        []string                    `json:"closure"` // 41-hex; matches range-wide closure
	Extra          map[string]json.RawMessage  `json:"extra,omitempty"`
}

type SnapshotAccount struct {
	Address      string `json:"address"`
	AccountProto string `json:"accountProto"` // base64 of corepb.Account
}

type SnapshotContractState struct {
	Address            string `json:"address"`
	ContractStateProto string `json:"contractStateProto"` // base64 of corepb.ContractState
}

type SnapshotCode struct {
	Address string `json:"address"`
	CodeHex string `json:"code"`
}

// LoadSnapshot parses a Snapshot JSON and returns a *Loaded with state
// seeded to the values in the snapshot. Unlike LoadSeed, this is meant for
// operations like "compute the digest java-tron would see" — it doesn't
// care whether the inputs match any particular start-of-range convention.
func LoadSnapshot(r io.Reader) (*Loaded, *Snapshot, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, fmt.Errorf("read snapshot: %w", err)
	}
	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return nil, nil, fmt.Errorf("parse snapshot: %w", err)
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	sdb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), state.NewDatabase(diskdb))
	if err != nil {
		return nil, nil, fmt.Errorf("new statedb: %w", err)
	}

	dp := state.NewDynamicProperties()
	for k, v := range snap.DP {
		dp.Set(normalizeDPKey(k), v)
	}
	sdb.SetDynamicProperties(dp)

	for _, a := range snap.Accounts {
		addr, err := ParseAddress(a.Address)
		if err != nil {
			return nil, nil, fmt.Errorf("account %q: %w", a.Address, err)
		}
		accBytes, err := base64.StdEncoding.DecodeString(a.AccountProto)
		if err != nil {
			return nil, nil, fmt.Errorf("account %s: base64 decode: %w", a.Address, err)
		}
		var pb corepb.Account
		if err := proto.Unmarshal(accBytes, &pb); err != nil {
			return nil, nil, fmt.Errorf("account %s: proto unmarshal: %w", a.Address, err)
		}
		// Let StateDB construct the account, then replace its underlying
		// proto with the incoming one so every field survives (frozenV2,
		// delegated resources, assets, allowance, latestOpTime, ...).
		// Using proto.Reset + proto.Merge instead of a struct copy keeps
		// the embedded protoimpl.MessageState intact (copylocks-safe).
		sdb.CreateAccount(addr, pb.Type)
		if obj := sdb.GetAccount(addr); obj != nil {
			dst := obj.Proto()
			proto.Reset(dst)
			proto.Merge(dst, &pb)
		}
	}

	for _, c := range snap.Code {
		addr, err := ParseAddress(c.Address)
		if err != nil {
			return nil, nil, fmt.Errorf("code %q: %w", c.Address, err)
		}
		code, err := hex.DecodeString(c.CodeHex)
		if err != nil {
			return nil, nil, fmt.Errorf("code %s: hex decode: %w", c.Address, err)
		}
		if sdb.GetAccount(addr) == nil {
			sdb.CreateAccount(addr, corepb.AccountType_Contract)
		}
		sdb.SetCode(addr, code)
	}

	for _, cs := range snap.ContractStates {
		addr, err := ParseAddress(cs.Address)
		if err != nil {
			return nil, nil, fmt.Errorf("contractState %q: %w", cs.Address, err)
		}
		csBytes, err := base64.StdEncoding.DecodeString(cs.ContractStateProto)
		if err != nil {
			return nil, nil, fmt.Errorf("contractState %s: base64 decode: %w", cs.Address, err)
		}
		csState, err := types.NewContractStateFromBytes(csBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("contractState %s: decode: %w", cs.Address, err)
		}
		if err := rawdb.WriteContractState(diskdb, addr, csState); err != nil {
			return nil, nil, fmt.Errorf("contractState %s: write: %w", cs.Address, err)
		}
	}

	closure := make([]tcommon.Address, 0, len(snap.Closure))
	for _, s := range snap.Closure {
		a, err := ParseAddress(s)
		if err != nil {
			return nil, nil, fmt.Errorf("closure %q: %w", s, err)
		}
		closure = append(closure, a)
	}

	return &Loaded{
		StateDB:  sdb,
		DynProps: dp,
		Closure:  closure,
		DiskDB:   diskdb,
	}, &snap, nil
}

// DumpSnapshot extracts a Snapshot from the given StateDB+DP for the closure
// so tests (and operators diagnosing fixture issues) can round-trip state
// through the snapshot format.
func DumpSnapshot(l *Loaded, blockNum uint64) (*Snapshot, error) {
	snap := &Snapshot{
		BlockNum: blockNum,
		DP:       map[string]int64{},
		Closure:  make([]string, 0, len(l.Closure)),
	}
	for _, a := range l.Closure {
		snap.Closure = append(snap.Closure, hex.EncodeToString(a[:]))
	}
	for _, a := range l.Closure {
		if acc := l.StateDB.GetAccount(a); acc != nil {
			b, err := proto.Marshal(acc.Proto())
			if err != nil {
				return nil, fmt.Errorf("marshal account %s: %w", hex.EncodeToString(a[:]), err)
			}
			snap.Accounts = append(snap.Accounts, SnapshotAccount{
				Address:      hex.EncodeToString(a[:]),
				AccountProto: base64.StdEncoding.EncodeToString(b),
			})
		}
		if code := l.StateDB.GetCode(a); len(code) > 0 {
			snap.Code = append(snap.Code, SnapshotCode{
				Address: hex.EncodeToString(a[:]),
				CodeHex: hex.EncodeToString(code),
			})
		}
		if cs := rawdb.ReadContractState(l.DiskDB, a); cs != nil {
			b, err := cs.Bytes()
			if err != nil {
				return nil, fmt.Errorf("marshal contractState %s: %w", hex.EncodeToString(a[:]), err)
			}
			snap.ContractStates = append(snap.ContractStates, SnapshotContractState{
				Address:            hex.EncodeToString(a[:]),
				ContractStateProto: base64.StdEncoding.EncodeToString(b),
			})
		}
	}
	for _, k := range l.DynProps.Keys() {
		v, _ := l.DynProps.Get(k)
		snap.DP[k] = v
	}
	return snap, nil
}
