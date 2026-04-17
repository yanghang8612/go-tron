package conformance

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Loaded bundles the artifacts LoadSeed produces. DiskDB is exposed because
// ProcessBlock + rawdb accessors need a KeyValueStore handle independent of
// the StateDB wrapper.
type Loaded struct {
	StateDB  *state.StateDB
	DynProps *state.DynamicProperties
	Closure  []tcommon.Address
	DiskDB   ethdb.KeyValueStore
	Meta     *Seed
}

// LoadSeed reads seed.json and constructs an in-memory StateDB + DP seeded to
// the state at StartHeight-1 for the range's touched-address closure.
func LoadSeed(path string) (*Loaded, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read seed: %w", err)
	}
	var seed Seed
	if err := json.Unmarshal(raw, &seed); err != nil {
		return nil, fmt.Errorf("parse seed: %w", err)
	}
	if seed.Schema != SchemaVersion {
		return nil, fmt.Errorf("seed schema %d != %d", seed.Schema, SchemaVersion)
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	sdb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), state.NewDatabase(diskdb))
	if err != nil {
		return nil, fmt.Errorf("new statedb: %w", err)
	}

	dp := state.NewDynamicProperties()
	for k, v := range seed.DynamicProps {
		goKey := normalizeDPKey(k)
		dp.Set(goKey, v)
	}
	sdb.SetDynamicProperties(dp)

	closure := make([]tcommon.Address, 0, len(seed.ClosureAddresses))
	for _, s := range seed.ClosureAddresses {
		a, err := ParseAddress(s)
		if err != nil {
			return nil, fmt.Errorf("closure address %q: %w", s, err)
		}
		closure = append(closure, a)
	}

	for _, a := range seed.Accounts {
		addr, err := ParseAddress(a.Address)
		if err != nil {
			return nil, fmt.Errorf("account address %q: %w", a.Address, err)
		}
		if err := applySeedAccount(sdb, addr, a); err != nil {
			return nil, fmt.Errorf("apply account %s: %w", a.Address, err)
		}
	}

	for _, c := range seed.Contracts {
		addr, err := ParseAddress(c.Address)
		if err != nil {
			return nil, fmt.Errorf("contract address %q: %w", c.Address, err)
		}
		if err := applySeedContract(sdb, addr, c); err != nil {
			return nil, fmt.Errorf("apply contract %s: %w", c.Address, err)
		}
	}

	return &Loaded{
		StateDB:  sdb,
		DynProps: dp,
		Closure:  closure,
		DiskDB:   diskdb,
		Meta:     &seed,
	}, nil
}

// ParseAddress decodes a 21-byte TRON address from 41-prefixed hex (42 chars).
func ParseAddress(s string) (tcommon.Address, error) {
	if len(s) != 42 {
		return tcommon.Address{}, fmt.Errorf("address %q: want 42 hex chars, got %d", s, len(s))
	}
	if s[0] != '4' || s[1] != '1' {
		return tcommon.Address{}, fmt.Errorf("address %q: missing 41 prefix", s)
	}
	bs, err := hex.DecodeString(s)
	if err != nil {
		return tcommon.Address{}, fmt.Errorf("hex decode %q: %w", s, err)
	}
	var a tcommon.Address
	copy(a[:], bs)
	return a, nil
}

// ParseAddresses is the slice form used by fixture.json activeWitnesses.
func ParseAddresses(ss []string) ([]tcommon.Address, error) {
	out := make([]tcommon.Address, 0, len(ss))
	for _, s := range ss {
		a, err := ParseAddress(s)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

// normalizeDPKey accepts either go-tron snake_case keys or java-tron getter
// names (e.g. "getEnergyFee") and returns the go-tron form. Silently passes
// through unknown keys so tests can set synthetic keys that aren't in the
// translation table yet.
func normalizeDPKey(k string) string {
	if strings.HasPrefix(k, "get") && len(k) > 3 && k[3] >= 'A' && k[3] <= 'Z' {
		if mapped := state.JavaGetterToGoKey(k); mapped != "" {
			return mapped
		}
	}
	return k
}

func applySeedAccount(sdb *state.StateDB, addr tcommon.Address, a SeedAccount) error {
	if a.Raw != nil {
		return fmt.Errorf("account %s: raw proto-json not yet supported", a.Address)
	}
	sdb.CreateAccount(addr, corepb.AccountType(a.AccountType))
	if a.Balance != 0 {
		sdb.AddBalance(addr, a.Balance)
	}
	if a.FrozenV1Net != 0 {
		return fmt.Errorf("account %s: frozenV1Net seeding not yet wired", a.Address)
	}
	return nil
}

func applySeedContract(sdb *state.StateDB, addr tcommon.Address, c SeedContract) error {
	if c.Raw != nil {
		return fmt.Errorf("contract %s: raw proto-json not yet supported", c.Address)
	}
	if c.CodeHex != "" {
		code, err := hex.DecodeString(c.CodeHex)
		if err != nil {
			return fmt.Errorf("contract %s: decode code: %w", c.Address, err)
		}
		sdb.CreateAccount(addr, corepb.AccountType_Contract)
		sdb.SetCode(addr, code)
	}
	return nil
}

// LoadFixtureMeta reads fixture.json at the given path.
func LoadFixtureMeta(path string) (*FixtureMeta, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read fixture meta: %w", err)
	}
	var m FixtureMeta
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("parse fixture meta: %w", err)
	}
	if m.Schema != SchemaVersion {
		return nil, fmt.Errorf("fixture meta schema %d != %d", m.Schema, SchemaVersion)
	}
	return &m, nil
}
