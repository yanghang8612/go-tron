package conformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"google.golang.org/protobuf/proto"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

// DigestB is the cheap per-block fingerprint: sha256 over a canonical
// encoding of the touched-address closure's account/code/contract-state
// bytes plus every known DP key/value. Used as the primary pass/fail signal
// during replay.
func DigestB(sdb *state.StateDB, db ethdb.KeyValueStore, addrs []tcommon.Address, dp *state.DynamicProperties) [32]byte {
	sorted := append([]tcommon.Address(nil), addrs...)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i][:], sorted[j][:]) < 0
	})

	h := sha256.New()
	h.Write([]byte("conformance/digestB/v1"))

	for _, a := range sorted {
		h.Write(a[:])
		// Account proto (canonical bytes). GetAccount returns nil for
		// missing accounts; write a length-0 marker in that case.
		var accBytes []byte
		if acc := sdb.GetAccount(a); acc != nil {
			b, err := proto.Marshal(acc.Proto())
			if err == nil {
				accBytes = b
			}
		}
		writeLenPrefixed(h, accBytes)

		// Code bytes (empty for non-contract accounts).
		writeLenPrefixed(h, sdb.GetCode(a))

		// Per-contract dynamic-energy state (nil → length-0 marker).
		var csBytes []byte
		if cs := rawdb.ReadContractState(db, a); cs != nil {
			b, err := cs.Bytes()
			if err == nil {
				csBytes = b
			}
		}
		writeLenPrefixed(h, csBytes)
	}

	keys := dp.Keys()
	sort.Strings(keys)
	for _, k := range keys {
		v, _ := dp.Get(k)
		writeLenPrefixed(h, []byte(k))
		writeInt64BE(h, v)
	}

	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// DigestC emits the same data as DigestB but as structured JSON for human
// diffing. Map keys are stable.
func DigestC(sdb *state.StateDB, db ethdb.KeyValueStore, addrs []tcommon.Address, dp *state.DynamicProperties) json.RawMessage {
	sorted := append([]tcommon.Address(nil), addrs...)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i][:], sorted[j][:]) < 0
	})

	accounts := make(map[string]map[string]any, len(sorted))
	for _, a := range sorted {
		entry := map[string]any{}
		if acc := sdb.GetAccount(a); acc != nil {
			pb := acc.Proto()
			entry["balance"] = pb.Balance
			entry["accountType"] = int32(pb.Type)
			entry["isWitness"] = pb.IsWitness
			entry["createTime"] = pb.CreateTime
			entry["accountName"] = string(pb.AccountName)
			if len(pb.FrozenSupply) > 0 {
				entry["frozenSupply"] = len(pb.FrozenSupply)
			}
			if len(pb.FrozenV2) > 0 {
				frozen := map[string]int64{}
				for _, f := range pb.FrozenV2 {
					frozen[f.Type.String()] = f.Amount
				}
				entry["frozenV2"] = frozen
			}
		}
		if code := sdb.GetCode(a); len(code) > 0 {
			entry["codeHash"] = hex.EncodeToString(sdb.GetCodeHash(a).Bytes())
			entry["codeLen"] = len(code)
		}
		if cs := rawdb.ReadContractState(db, a); cs != nil {
			entry["contractState"] = map[string]int64{
				"updateCycle":  cs.UpdateCycle(),
				"energyFactor": cs.EnergyFactor(),
				"energyUsage":  cs.EnergyUsage(),
			}
		}
		if len(entry) > 0 {
			accounts[hex.EncodeToString(a[:])] = entry
		}
	}

	dpMap := map[string]int64{}
	for _, k := range dp.Keys() {
		v, _ := dp.Get(k)
		dpMap[k] = v
	}

	out, err := json.Marshal(map[string]any{
		"accounts": accounts,
		"dp":       dpMap,
	})
	if err != nil {
		return json.RawMessage(fmt.Sprintf(`{"error":%q}`, err.Error()))
	}
	return out
}

func writeLenPrefixed(h interface{ Write([]byte) (int, error) }, b []byte) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(b)))
	h.Write(buf[:n])
	h.Write(b)
}

func writeInt64BE(h interface{ Write([]byte) (int, error) }, v int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	h.Write(buf[:])
}
