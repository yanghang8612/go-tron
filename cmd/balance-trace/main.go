// balance-trace prints every block and transaction in the local gtron datadir
// whose serialized form references a target 41-hex address. One-shot
// diagnostic for "where did this account's TRX come from / go to?" — the
// witness 41061e3f4e108d8aaf5cd75b499f811ae30ed04b77 case at block 37729 was
// the first user.
//
// Usage:
//
//	balance-trace --datadir=/tmp/gtron-mainnet-test --addr=41061e3f...
package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/log"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/state"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

func main() {
	log.SetupCLI()
	datadir := flag.String("datadir", "/tmp/gtron-mainnet-test", "gtron datadir (contains gtron/chaindata)")
	addrHex := flag.String("addr", "", "41-hex address to trace (e.g. 41061e3f4e108d8aaf5cd75b499f811ae30ed04b77)")
	from := flag.Uint64("from", 0, "start block number")
	to := flag.Uint64("to", 0, "end block number (0=head)")
	flag.Parse()
	if *addrHex == "" {
		log.Crit("--addr is required")
	}
	addrBytes, err := hex.DecodeString(*addrHex)
	if err != nil || len(addrBytes) != 21 {
		log.Crit("--addr must be 21-byte 41-prefixed hex")
	}
	target := tcommon.BytesToAddress(addrBytes)

	dbPath := filepath.Join(*datadir, "gtron", "chaindata")
	if _, err := os.Stat(dbPath); err != nil {
		log.Crit("datadir not accessible", "path", dbPath, "err", err)
	}
	db, err := rawdb.NewPebbleDB(dbPath, 256, 500)
	if err != nil {
		log.Crit("open pebble", "err", err)
	}
	defer db.Close()
	var ancient rawdb.AncientReader = rawdb.NoopAncient{}
	ancientPath := filepath.Join(*datadir, "gtron", "ancient")
	if info, err := os.Stat(ancientPath); err == nil && info.IsDir() {
		fz, err := rawdbfreezer.NewFreezer(ancientPath, "", true, rawdbfreezer.FreezerTableSize, chainfreezer.FreezerTableSet())
		if err != nil {
			log.Crit("open ancient", "path", ancientPath, "err", err)
		}
		defer fz.Close()
		ancient = rawdb.NewFreezerReader(fz)
	}
	snapshotPath := filepath.Join(*datadir, "gtron", "state-snapshots")
	snapshotManager, err := statesnapshots.OpenManager(snapshotPath)
	if err != nil {
		log.Crit("open state snapshots", "path", snapshotPath, "err", err)
	}
	ancient = rawdb.NewFallbackAncientReader(ancient, snapshotManager)
	chaindb := rawdb.NewChainDB(db, ancient)
	chaindb.SetChainIndexReader(snapshotManager)
	chaindb.SetBalanceTraceReader(snapshotManager)
	chaindb.SetSectionBloomReader(snapshotManager)
	chaindb.SetEventLogReader(snapshotManager)

	headHash := rawdb.ReadHeadBlockHash(db)
	if headHash == (tcommon.Hash{}) {
		log.Crit("no head block", "path", dbPath)
	}
	headNum, ok, err := readBalanceTraceBlockNumber(chaindb, headHash)
	if err != nil {
		log.Crit("read head block number", "hash", fmt.Sprintf("%x", headHash[:]), "err", err)
	}
	if !ok {
		log.Crit("head block has no number entry", "hash", fmt.Sprintf("%x", headHash[:]))
	}
	end := *to
	if end == 0 || end > headNum {
		end = headNum
	}
	fmt.Printf("scanning blocks %d..%d for address %x (head=%d)\n", *from, end, target.Bytes(), headNum)

	hits := 0
	for h := *from; h <= end; h++ {
		blk, err := readBalanceTraceBlock(chaindb, h)
		if err != nil {
			log.Crit("read block", "block", h, "err", err)
		}
		if blk == nil {
			continue
		}
		// Cheap pre-filter: marshal once, byte-search.
		raw, err := blk.Marshal()
		if err != nil || !bytes.Contains(raw, addrBytes) {
			continue
		}
		// Witness-only match (block produced by the address) is interesting too.
		if blk.WitnessAddress() == target {
			fmt.Printf("[%d] WITNESS=%x produced this block (txs=%d)\n", h, target.Bytes(), len(blk.Transactions()))
		}
		for i, tx := range blk.Transactions() {
			matched, err := inspectTx(h, i, tx, target, chaindb)
			if err != nil {
				log.Crit("inspect transaction", "block", h, "index", i, "err", err)
			}
			if !matched {
				continue
			}
			hits++
		}
	}

	fmt.Printf("\n--- final state view ---\n")
	headRoot, ok, err := readBalanceTraceBlockStateRoot(chaindb, headHash)
	if err != nil {
		log.Crit("read head state root", "hash", fmt.Sprintf("%x", headHash[:]), "err", err)
	}
	if !ok {
		headRoot = rawdb.ReadGenesisStateRoot(db)
	}
	stateDB := state.NewDatabase(rawdb.WrapKeyValueStore(db))
	statedb, err := state.New(headRoot, stateDB)
	if err != nil {
		log.Crit("open statedb", "root", fmt.Sprintf("%x", headRoot[:]), "err", err)
	}
	if !statedb.AccountExists(target) {
		fmt.Printf("account %x DOES NOT EXIST in our state\n", target.Bytes())
	} else {
		fmt.Printf("account %x: balance=%d allowance=%d isWitness=%v\n",
			target.Bytes(),
			statedb.GetBalance(target),
			statedb.GetAllowance(target),
			statedb.IsWitness(target),
		)
	}

	dp := state.LoadDynamicProperties(db, statedb)
	fmt.Printf("\n--- DP / maintenance state ---\n")
	fmt.Printf("LatestBlockHeaderNumber=%d\n", dp.LatestBlockHeaderNumber())
	fmt.Printf("NextMaintenanceTime=%d interval=%d\n",
		dp.NextMaintenanceTime(), dp.MaintenanceTimeInterval())
	fmt.Printf("CurrentCycleNumber=%d\n", dp.CurrentCycleNumber())
	fmt.Printf("WitnessStandbyAllowance=%d\n", dp.WitnessStandbyAllowance())
	fmt.Printf("WitnessPayPerBlock=%d\n", dp.WitnessPayPerBlock())
	fmt.Printf("ChangeDelegation=%v\n", dp.ChangeDelegation())
	fmt.Printf("\n%d transactions touched %x in [%d..%d]\n", hits, target.Bytes(), *from, end)
}

func readBalanceTraceBlockNumber(chaindb *rawdb.ChainDB, hash tcommon.Hash) (uint64, bool, error) {
	return rawdb.ReadBlockNumberStrict(chaindb, hash)
}

func readBalanceTraceBlockStateRoot(chaindb *rawdb.ChainDB, hash tcommon.Hash) (tcommon.Hash, bool, error) {
	return rawdb.ReadBlockStateRootStrict(chaindb, hash)
}

func readBalanceTraceBlock(chaindb *rawdb.ChainDB, number uint64) (*types.Block, error) {
	block, ok, err := rawdb.ReadBlockStrict(chaindb, number)
	if err != nil || !ok {
		return nil, err
	}
	return block, nil
}

// inspectTx tries each known contract type and prints a one-line summary if
// the contract references the target. Returns true when at least one match
// was printed.
func inspectTx(blockNum uint64, idx int, tx *types.Transaction, target tcommon.Address, chaindb *rawdb.ChainDB) (bool, error) {
	c := tx.Contract()
	if c == nil {
		return false, nil
	}
	switch c.Type {
	case corepb.Transaction_Contract_TransferContract:
		v := &contractpb.TransferContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil {
			if eq(v.OwnerAddress, target) || eq(v.ToAddress, target) {
				dir := "→"
				if eq(v.OwnerAddress, target) {
					dir = "← OUT"
				} else {
					dir = "→ IN"
				}
				return true, printTxLine(blockNum, idx, tx, target, chaindb, "Transfer %s amount=%d from=%x to=%x", dir, v.Amount, v.OwnerAddress, v.ToAddress)
			}
		}
	case corepb.Transaction_Contract_TransferAssetContract:
		v := &contractpb.TransferAssetContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil {
			if eq(v.OwnerAddress, target) || eq(v.ToAddress, target) {
				return true, printTxLine(blockNum, idx, tx, target, chaindb, "TransferAsset asset=%q amount=%d from=%x to=%x", string(v.AssetName), v.Amount, v.OwnerAddress, v.ToAddress)
			}
		}
	case corepb.Transaction_Contract_FreezeBalanceContract:
		v := &contractpb.FreezeBalanceContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil && eq(v.OwnerAddress, target) {
			return true, printTxLine(blockNum, idx, tx, target, chaindb, "FreezeBalance amount=%d duration=%d resource=%v", v.FrozenBalance, v.FrozenDuration, v.Resource)
		}
	case corepb.Transaction_Contract_UnfreezeBalanceContract:
		v := &contractpb.UnfreezeBalanceContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil && eq(v.OwnerAddress, target) {
			return true, printTxLine(blockNum, idx, tx, target, chaindb, "UnfreezeBalance resource=%v", v.Resource)
		}
	case corepb.Transaction_Contract_WithdrawBalanceContract:
		v := &contractpb.WithdrawBalanceContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil && eq(v.OwnerAddress, target) {
			return true, printTxLine(blockNum, idx, tx, target, chaindb, "WithdrawBalance owner=%x", v.OwnerAddress)
		}
	case corepb.Transaction_Contract_VoteWitnessContract:
		v := &contractpb.VoteWitnessContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil {
			matchOwner := eq(v.OwnerAddress, target)
			matchVoted := false
			for _, vote := range v.Votes {
				if eq(vote.VoteAddress, target) {
					matchVoted = true
				}
			}
			if matchOwner || matchVoted {
				return true, printTxLine(blockNum, idx, tx, target, chaindb, "VoteWitness owner=%x votes=%d matchOwner=%v matchVoted=%v",
					v.OwnerAddress, len(v.Votes), matchOwner, matchVoted)
			}
		}
	case corepb.Transaction_Contract_WitnessCreateContract:
		v := &contractpb.WitnessCreateContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil && eq(v.OwnerAddress, target) {
			return true, printTxLine(blockNum, idx, tx, target, chaindb, "WitnessCreate url=%q", string(v.Url))
		}
	case corepb.Transaction_Contract_AccountCreateContract:
		v := &contractpb.AccountCreateContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil {
			if eq(v.OwnerAddress, target) || eq(v.AccountAddress, target) {
				return true, printTxLine(blockNum, idx, tx, target, chaindb, "AccountCreate owner=%x new=%x", v.OwnerAddress, v.AccountAddress)
			}
		}
	case corepb.Transaction_Contract_AssetIssueContract:
		v := &contractpb.AssetIssueContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil && eq(v.OwnerAddress, target) {
			return true, printTxLine(blockNum, idx, tx, target, chaindb, "AssetIssue owner=%x name=%q totalSupply=%d", v.OwnerAddress, string(v.Name), v.TotalSupply)
		}
	case corepb.Transaction_Contract_ParticipateAssetIssueContract:
		v := &contractpb.ParticipateAssetIssueContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil {
			if eq(v.OwnerAddress, target) || eq(v.ToAddress, target) {
				return true, printTxLine(blockNum, idx, tx, target, chaindb, "ParticipateAssetIssue from=%x to=%x asset=%q amount=%d", v.OwnerAddress, v.ToAddress, string(v.AssetName), v.Amount)
			}
		}
	case corepb.Transaction_Contract_CreateSmartContract:
		v := &contractpb.CreateSmartContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil {
			contractAddr := []byte(nil)
			if v.NewContract != nil {
				contractAddr = v.NewContract.ContractAddress
			}
			if eq(v.OwnerAddress, target) || eq(contractAddr, target) {
				return true, printTxLine(blockNum, idx, tx, target, chaindb, "CreateSmartContract owner=%x contract=%x", v.OwnerAddress, contractAddr)
			}
		}
	case corepb.Transaction_Contract_TriggerSmartContract:
		v := &contractpb.TriggerSmartContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil {
			matchOwner := eq(v.OwnerAddress, target)
			matchContract := eq(v.ContractAddress, target)
			matchData := bytes.Contains(v.Data, target.Bytes()[1:])
			if matchOwner || matchContract || matchData {
				selector := v.Data
				if len(selector) > 4 {
					selector = selector[:4]
				}
				return true, printTxLine(blockNum, idx, tx, target, chaindb, "TriggerSmartContract owner=%x contract=%x callValue=%d selector=%x matchOwner=%v matchContract=%v matchData=%v",
					v.OwnerAddress, v.ContractAddress, v.CallValue, selector, matchOwner, matchContract, matchData)
			}
		}
	case corepb.Transaction_Contract_AccountPermissionUpdateContract:
		v := &contractpb.AccountPermissionUpdateContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil && eq(v.OwnerAddress, target) {
			return true, printTxLine(blockNum, idx, tx, target, chaindb, "AccountPermissionUpdate owner=%x", v.OwnerAddress)
		}
	default:
		// Fallback: unmarshal Parameter.Value bytes and search the marshaled
		// bytes for the target. Keeps catch-all for contracts not enumerated
		// above (Proposal*, Exchange*, MarketSell*, TVM Trigger*).
		if c.Parameter == nil {
			return false, nil
		}
		bs, err := proto.Marshal(c.Parameter)
		if err != nil {
			return false, nil
		}
		if !bytes.Contains(bs, target.Bytes()) {
			return false, nil
		}
		return true, printTxLine(blockNum, idx, tx, target, chaindb, "%v contract references target (unmarshaled scan)", c.Type)
	}
	return false, nil
}

func printTxLine(blockNum uint64, idx int, tx *types.Transaction, target tcommon.Address, chaindb *rawdb.ChainDB, format string, args ...interface{}) error {
	prefix, err := txPrefix(blockNum, idx, tx, target, chaindb)
	if err != nil {
		return err
	}
	fmt.Printf("%s %s\n", prefix, fmt.Sprintf(format, args...))
	return nil
}

func txPrefix(blockNum uint64, idx int, tx *types.Transaction, target tcommon.Address, chaindb *rawdb.ChainDB) (string, error) {
	hash := tx.Hash()
	parts := []string{
		fmt.Sprintf("[%d.%d]", blockNum, idx),
		fmt.Sprintf("tx=%x", hash[:]),
	}
	info, ok, err := rawdb.ReadTransactionInfoStrict(chaindb, hash[:])
	if err != nil {
		return "", fmt.Errorf("transaction info %x: %w", hash[:], err)
	}
	if ok {
		receipt := info.GetReceipt()
		parts = append(parts, fmt.Sprintf(
			"infoFee=%d netFee=%d netUsage=%d energyFee=%d energyUsage=%d energyUsageTotal=%d result=%v",
			info.GetFee(),
			receipt.GetNetFee(),
			receipt.GetNetUsage(),
			receipt.GetEnergyFee(),
			receipt.GetEnergyUsage(),
			receipt.GetEnergyUsageTotal(),
			receipt.GetResult(),
		))
	} else {
		parts = append(parts, "info=<missing>")
	}
	bal, ok, err := rawdb.ReadAccountTraceStrict(chaindb, target.Bytes(), int64(blockNum))
	if err != nil {
		return "", fmt.Errorf("account trace block %d owner %x: %w", blockNum, target.Bytes(), err)
	}
	if ok {
		parts = append(parts, fmt.Sprintf("accountTraceBalance=%d", bal))
	}
	ops, err := balanceTraceOps(chaindb, blockNum, hash, target)
	if err != nil {
		return "", err
	}
	if ops != "" {
		parts = append(parts, ops)
	}
	return strings.Join(parts, " "), nil
}

func balanceTraceOps(chaindb *rawdb.ChainDB, blockNum uint64, hash tcommon.Hash, target tcommon.Address) (string, error) {
	trace, ok, err := rawdb.ReadBlockBalanceTraceStrict(chaindb, int64(blockNum))
	if err != nil {
		return "", fmt.Errorf("block balance trace block %d: %w", blockNum, err)
	}
	if !ok {
		return "", nil
	}
	var ops []string
	for _, txTrace := range trace.TransactionBalanceTrace {
		if txTrace == nil || !bytes.Equal(txTrace.TransactionIdentifier, hash[:]) {
			continue
		}
		for _, op := range txTrace.Operation {
			if op == nil || !bytes.Equal(op.Address, target.Bytes()) {
				continue
			}
			ops = append(ops, fmt.Sprintf("%d:%d", op.OperationIdentifier, op.Amount))
		}
	}
	if len(ops) == 0 {
		return "", nil
	}
	return "balanceTraceOps=" + strings.Join(ops, ","), nil
}

func eq(b []byte, addr tcommon.Address) bool {
	if len(b) != 21 {
		return false
	}
	return bytes.Equal(b, addr.Bytes())
}
