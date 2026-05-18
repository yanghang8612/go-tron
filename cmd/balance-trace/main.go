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

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
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

	headHash := rawdb.ReadHeadBlockHash(db)
	if headHash == (tcommon.Hash{}) {
		log.Crit("no head block", "path", dbPath)
	}
	headNum := rawdb.ReadBlockNumber(db, headHash)
	if headNum == nil {
		log.Crit("head block has no number entry", "hash", fmt.Sprintf("%x", headHash[:]))
	}
	end := *to
	if end == 0 || end > *headNum {
		end = *headNum
	}
	fmt.Printf("scanning blocks %d..%d for address %x (head=%d)\n", *from, end, target.Bytes(), *headNum)

	hits := 0
	for h := *from; h <= end; h++ {
		blk := rawdb.ReadBlock(db, h)
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
			if !inspectTx(h, i, tx, target) {
				continue
			}
			hits++
		}
	}

	fmt.Printf("\n--- final state view ---\n")
	headRoot := rawdb.ReadBlockStateRoot(db, headHash)
	if headRoot == (tcommon.Hash{}) {
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

	dp := state.LoadDynamicProperties(db)
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

// inspectTx tries each known contract type and prints a one-line summary if
// the contract references the target. Returns true when at least one match
// was printed.
func inspectTx(blockNum uint64, idx int, tx *types.Transaction, target tcommon.Address) bool {
	c := tx.Contract()
	if c == nil {
		return false
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
				fmt.Printf("[%d.%d] Transfer %s amount=%d from=%x to=%x\n", blockNum, idx, dir, v.Amount, v.OwnerAddress, v.ToAddress)
				return true
			}
		}
	case corepb.Transaction_Contract_TransferAssetContract:
		v := &contractpb.TransferAssetContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil {
			if eq(v.OwnerAddress, target) || eq(v.ToAddress, target) {
				fmt.Printf("[%d.%d] TransferAsset asset=%q amount=%d from=%x to=%x\n", blockNum, idx, string(v.AssetName), v.Amount, v.OwnerAddress, v.ToAddress)
				return true
			}
		}
	case corepb.Transaction_Contract_FreezeBalanceContract:
		v := &contractpb.FreezeBalanceContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil && eq(v.OwnerAddress, target) {
			fmt.Printf("[%d.%d] FreezeBalance amount=%d duration=%d resource=%v\n", blockNum, idx, v.FrozenBalance, v.FrozenDuration, v.Resource)
			return true
		}
	case corepb.Transaction_Contract_UnfreezeBalanceContract:
		v := &contractpb.UnfreezeBalanceContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil && eq(v.OwnerAddress, target) {
			fmt.Printf("[%d.%d] UnfreezeBalance resource=%v\n", blockNum, idx, v.Resource)
			return true
		}
	case corepb.Transaction_Contract_WithdrawBalanceContract:
		v := &contractpb.WithdrawBalanceContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil && eq(v.OwnerAddress, target) {
			fmt.Printf("[%d.%d] WithdrawBalance owner=%x\n", blockNum, idx, v.OwnerAddress)
			return true
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
				fmt.Printf("[%d.%d] VoteWitness owner=%x votes=%d (matchOwner=%v matchVoted=%v)\n",
					blockNum, idx, v.OwnerAddress, len(v.Votes), matchOwner, matchVoted)
				return true
			}
		}
	case corepb.Transaction_Contract_WitnessCreateContract:
		v := &contractpb.WitnessCreateContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil && eq(v.OwnerAddress, target) {
			fmt.Printf("[%d.%d] WitnessCreate url=%q\n", blockNum, idx, string(v.Url))
			return true
		}
	case corepb.Transaction_Contract_AccountCreateContract:
		v := &contractpb.AccountCreateContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil {
			if eq(v.OwnerAddress, target) || eq(v.AccountAddress, target) {
				fmt.Printf("[%d.%d] AccountCreate owner=%x new=%x\n", blockNum, idx, v.OwnerAddress, v.AccountAddress)
				return true
			}
		}
	case corepb.Transaction_Contract_AssetIssueContract:
		v := &contractpb.AssetIssueContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil && eq(v.OwnerAddress, target) {
			fmt.Printf("[%d.%d] AssetIssue owner=%x name=%q totalSupply=%d\n", blockNum, idx, v.OwnerAddress, string(v.Name), v.TotalSupply)
			return true
		}
	case corepb.Transaction_Contract_ParticipateAssetIssueContract:
		v := &contractpb.ParticipateAssetIssueContract{}
		if err := c.Parameter.UnmarshalTo(v); err == nil {
			if eq(v.OwnerAddress, target) || eq(v.ToAddress, target) {
				fmt.Printf("[%d.%d] ParticipateAssetIssue from=%x to=%x asset=%q amount=%d\n", blockNum, idx, v.OwnerAddress, v.ToAddress, string(v.AssetName), v.Amount)
				return true
			}
		}
	default:
		// Fallback: unmarshal Parameter.Value bytes and search the marshaled
		// bytes for the target. Keeps catch-all for contracts not enumerated
		// above (Proposal*, Exchange*, MarketSell*, TVM Trigger*).
		if c.Parameter == nil {
			return false
		}
		bs, err := proto.Marshal(c.Parameter)
		if err != nil {
			return false
		}
		if !bytes.Contains(bs, target.Bytes()) {
			return false
		}
		fmt.Printf("[%d.%d] %v contract references target (unmarshaled scan)\n", blockNum, idx, c.Type)
		return true
	}
	return false
}

func eq(b []byte, addr tcommon.Address) bool {
	if len(b) != 21 {
		return false
	}
	return bytes.Equal(b, addr.Bytes())
}
