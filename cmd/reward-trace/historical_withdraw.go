package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const (
	historicalWithdrawMaxCycles      = 4096
	historicalWithdrawMaxKeys        = 100_000
	historicalWithdrawMaxOutputBytes = 16 << 20
	historicalWithdrawMaxTimeout     = 15 * time.Minute
)

type historicalWithdrawReaderDB interface {
	ethdb.KeyValueReader
	ethdb.Iteratee
}

type historicalWithdrawExportOptions struct {
	DB              historicalWithdrawReaderDB
	ChainDB         *rawdb.ChainDB
	ColdHistory     *snapshots.Manager
	LiveState       *state.StateDB
	DataDir         string
	SnapshotDir     string
	HeadNumber      uint64
	Owner           tcommon.Address
	PrestateBlock   uint64
	WithdrawTxID    string
	ExpectedBalance int64
	OutputPath      string
	Timeout         time.Duration
	MaxCycles       int
	MaxKeys         int
	MaxOutputBytes  int
}

type historicalRawInt64 struct {
	KeyHex  string `json:"keyHex"`
	Present bool   `json:"present"`
	RawHex  string `json:"rawHex,omitempty"`
	Value   int64  `json:"value"`
}

type historicalRawBigInt struct {
	KeyHex  string `json:"keyHex"`
	Present bool   `json:"present"`
	RawHex  string `json:"rawHex,omitempty"`
	Value   string `json:"value"`
}

type historicalVote struct {
	Witness string `json:"witness"`
	Count   int64  `json:"count"`
}

type historicalRewardRow struct {
	Witness     string             `json:"witness"`
	UserVotes   int64              `json:"userVotes"`
	CycleVote   historicalRawInt64 `json:"cycleVote"`
	CycleReward historicalRawInt64 `json:"cycleReward"`
}

type historicalRewardCycle struct {
	Cycle int64                 `json:"cycle"`
	Rows  []historicalRewardRow `json:"rows"`
}

type historicalVIEndpoint struct {
	Cycle   int64               `json:"cycle"`
	Witness string              `json:"witness"`
	Value   historicalRawBigInt `json:"value"`
}

type historicalRewardSegment struct {
	Source      string                       `json:"source"`
	BeginCycle  int64                        `json:"beginCycle"`
	EndCycle    int64                        `json:"endCycle"`
	Votes       []historicalVote             `json:"votes"`
	Cycles      []historicalRewardCycle      `json:"cycles"`
	VIEndpoints []historicalVIEndpoint       `json:"viEndpoints"`
	Computation historicalSegmentComputation `json:"computation"`
}

type historicalSegmentComputation struct {
	DeployedR1Raw      int64 `json:"deployedR1Raw"`
	DeployedR1Credited int64 `json:"deployedR1Credited"`
	FixedJavaRaw       int64 `json:"fixedJavaRaw"`
	FixedJavaCredited  int64 `json:"fixedJavaCredited"`
}

type historicalRewardPlan struct {
	EarlyReturnReason string                    `json:"earlyReturnReason,omitempty"`
	Segments          []historicalRewardSegment `json:"segments"`
	PostBeginCycle    int64                     `json:"postBeginCycle"`
	PostEndCycle      int64                     `json:"postEndCycle"`
}

type historicalRewardInputs struct {
	AccountBalance    int64                      `json:"accountBalance"`
	AccountAllowance  int64                      `json:"accountAllowance"`
	AccountVotes      []historicalVote           `json:"accountVotes"`
	BeginCycle        historicalRawInt64         `json:"beginCycle"`
	EndCycle          historicalRawInt64         `json:"endCycle"`
	BeginSnapshot     *historicalAccountSnapshot `json:"beginCycleAccountVoteSnapshot,omitempty"`
	CurrentCycle      historicalRawInt64         `json:"currentCycle"`
	NewAlgoCycle      historicalRawInt64         `json:"newRewardAlgorithmEffectiveCycle"`
	AllowOldRewardOpt historicalRawInt64         `json:"allowOldRewardOpt"`
	ChangeDelegation  historicalRawInt64         `json:"changeDelegation"`
}

type historicalAccountSnapshot struct {
	KeyHex    string           `json:"keyHex"`
	Present   bool             `json:"present"`
	RawHex    string           `json:"rawHex,omitempty"`
	Allowance int64            `json:"allowance,omitempty"`
	Votes     []historicalVote `json:"votes,omitempty"`
}

type historicalRewardComputation struct {
	PendingReward  int64 `json:"pendingReward"`
	Allowance      int64 `json:"allowance"`
	WithdrawAmount int64 `json:"withdrawAmount"`
}

type historicalWithdrawExport struct {
	FormatVersion int                        `json:"formatVersion"`
	CreatedUTC    string                     `json:"createdUtc"`
	Manifest      historicalManifestIdentity `json:"manifest"`
	Chain         struct {
		HeadNumber        uint64                           `json:"headNumber"`
		PrestateBlock     uint64                           `json:"prestateBlock"`
		PrestateHash      string                           `json:"prestateHash"`
		WithdrawBlock     uint64                           `json:"withdrawBlock"`
		WithdrawBlockHash string                           `json:"withdrawBlockHash"`
		WithdrawTxIndex   int                              `json:"withdrawTxIndex"`
		WithdrawTxID      string                           `json:"withdrawTxId"`
		PrecedingTxs      []historicalPrecedingTransaction `json:"precedingTransactions"`
		StateTxRange      *rawdb.StateTxRange              `json:"stateTxRange"`
	} `json:"chain"`
	Owner              string                      `json:"owner"`
	Inputs             historicalRewardInputs      `json:"inputs"`
	Plan               historicalRewardPlan        `json:"withdrawRewardPlan"`
	DeployedR1         historicalRewardComputation `json:"deployedR1PartNarrowing"`
	FixedJavaSemantics historicalRewardComputation `json:"fixedJavaCompoundAssignment"`
	ExplicitKeyReads   int                         `json:"explicitKeyReads"`
}

type historicalPrecedingTransaction struct {
	Index        int    `json:"index"`
	TxID         string `json:"txId"`
	ContractType string `json:"contractType"`
	Owner        string `json:"owner"`
	To           string `json:"to"`
}

type historicalManifestIdentity struct {
	Generation      uint64 `json:"generation"`
	Path            string `json:"path"`
	SHA256          string `json:"sha256"`
	CatalogChecksum string `json:"catalogChecksum,omitempty"`
	VisibleTxStart  uint64 `json:"visibleTxStart"`
	VisibleTxEnd    uint64 `json:"visibleTxEnd"`
}

type historicalKVReader struct {
	reader *state.PersistentHistoryReader
	ctx    context.Context
	block  uint64
	reads  int
	max    int
}

func (r *historicalKVReader) raw(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if err := r.readerContextError(); err != nil {
		return nil, false, err
	}
	r.reads++
	if r.reads > r.max {
		return nil, false, fmt.Errorf("historical reward export key limit exceeded: %d > %d", r.reads, r.max)
	}
	return r.reader.AccountKVAt(owner, domain, key, r.block)
}

func (r *historicalKVReader) int64(owner tcommon.Address, domain kvdomains.KVDomain, key []byte, missing int64) (historicalRawInt64, error) {
	raw, ok, err := r.raw(owner, domain, key)
	out := historicalRawInt64{KeyHex: hex.EncodeToString(key), Present: ok, Value: missing}
	if err != nil || !ok {
		return out, err
	}
	out.RawHex = hex.EncodeToString(raw)
	if len(raw) != 8 {
		return out, fmt.Errorf("historical key %x has length %d, want 8", key, len(raw))
	}
	out.Value = int64(binary.BigEndian.Uint64(raw))
	return out, nil
}

func (r *historicalKVReader) bigInt(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) (historicalRawBigInt, error) {
	raw, ok, err := r.raw(owner, domain, key)
	out := historicalRawBigInt{KeyHex: hex.EncodeToString(key), Present: ok, Value: "0"}
	if err != nil || !ok {
		return out, err
	}
	out.RawHex = hex.EncodeToString(raw)
	out.Value = new(big.Int).SetBytes(raw).String()
	return out, nil
}

func exportHistoricalWithdrawInputs(opts historicalWithdrawExportOptions) error {
	if opts.Timeout <= 0 || opts.Timeout > historicalWithdrawMaxTimeout {
		return fmt.Errorf("export timeout %s must be in (0,%s]", opts.Timeout, historicalWithdrawMaxTimeout)
	}
	if opts.MaxCycles <= 0 || opts.MaxCycles > historicalWithdrawMaxCycles || opts.MaxKeys <= 0 || opts.MaxKeys > historicalWithdrawMaxKeys || opts.MaxOutputBytes <= 0 || opts.MaxOutputBytes > historicalWithdrawMaxOutputBytes {
		return fmt.Errorf("historical reward export limits exceed hard maxima")
	}
	if opts.DB == nil || opts.ChainDB == nil || opts.ColdHistory == nil || opts.LiveState == nil {
		return fmt.Errorf("historical reward export requires db, chaindb, cold history, and live state")
	}
	if opts.PrestateBlock >= opts.HeadNumber {
		return fmt.Errorf("prestate block %d must be below head %d", opts.PrestateBlock, opts.HeadNumber)
	}
	if err := validateHistoricalOutputPath(opts.DataDir, opts.OutputPath); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()
	manifestStart, err := readHistoricalManifestIdentity(opts.SnapshotDir)
	if err != nil {
		return err
	}

	preBlock, err := readRewardTraceBlock(opts.ChainDB, opts.PrestateBlock)
	if err != nil || preBlock == nil {
		return fmt.Errorf("read prestate block %d: %w", opts.PrestateBlock, err)
	}
	withdrawBlock, err := readRewardTraceBlock(opts.ChainDB, opts.PrestateBlock+1)
	if err != nil || withdrawBlock == nil {
		return fmt.Errorf("read withdraw block %d: %w", opts.PrestateBlock+1, err)
	}
	if withdrawBlock.ParentHash() != preBlock.Hash() {
		return fmt.Errorf("withdraw block parent %x does not match prestate hash %x", withdrawBlock.ParentHash(), preBlock.Hash())
	}
	txIndex, txID, precedingTxs, err := findExpectedWithdraw(withdrawBlock, opts.Owner, opts.WithdrawTxID)
	if err != nil {
		return err
	}
	txRange, err := historicalStateTxRange(opts.DB, opts.ColdHistory, opts.PrestateBlock)
	if err != nil {
		return err
	}
	if txRange.BlockHash != preBlock.Hash() {
		return fmt.Errorf("state tx range block hash %x does not match prestate hash %x", txRange.BlockHash, preBlock.Hash())
	}

	reader := state.NewPersistentHistoryReaderWithColdHistory(opts.DB, opts.LiveState, opts.HeadNumber, opts.ColdHistory)
	reader.SetContext(ctx)
	if err := reader.SetHotHistoryBlockRange(opts.PrestateBlock, opts.HeadNumber); err != nil {
		return err
	}
	account, err := reader.AccountAt(opts.Owner, opts.PrestateBlock)
	if err != nil {
		return fmt.Errorf("read owner account at block %d: %w", opts.PrestateBlock, err)
	}
	if account == nil {
		return fmt.Errorf("owner account missing at block %d", opts.PrestateBlock)
	}
	if account.Balance() != opts.ExpectedBalance {
		return fmt.Errorf("owner balance at block %d = %d, want %d", opts.PrestateBlock, account.Balance(), opts.ExpectedBalance)
	}

	kv := &historicalKVReader{reader: reader, ctx: ctx, block: opts.PrestateBlock, max: opts.MaxKeys}
	inputs, err := readHistoricalRewardInputs(kv, opts.Owner, account)
	if err != nil {
		return err
	}
	plan, err := buildHistoricalRewardPlan(kv, inputs, opts.MaxCycles)
	if err != nil {
		return err
	}
	deployedPending, fixedPending, err := calculateHistoricalReward(&plan, inputs.NewAlgoCycle.Value, inputs.AllowOldRewardOpt.Value != 0)
	if err != nil {
		return err
	}

	var out historicalWithdrawExport
	out.FormatVersion = 1
	out.CreatedUTC = time.Now().UTC().Format(time.RFC3339Nano)
	manifestEnd, err := readHistoricalManifestIdentity(opts.SnapshotDir)
	if err != nil {
		return err
	}
	if manifestStart != manifestEnd {
		return fmt.Errorf("snapshot manifest changed during export: start=%+v end=%+v", manifestStart, manifestEnd)
	}
	out.Manifest = manifestStart
	out.Chain.HeadNumber = opts.HeadNumber
	out.Chain.PrestateBlock = opts.PrestateBlock
	out.Chain.PrestateHash = fmt.Sprintf("%x", preBlock.Hash())
	out.Chain.WithdrawBlock = withdrawBlock.Number()
	out.Chain.WithdrawBlockHash = fmt.Sprintf("%x", withdrawBlock.Hash())
	out.Chain.WithdrawTxIndex = txIndex
	out.Chain.WithdrawTxID = txID
	out.Chain.PrecedingTxs = precedingTxs
	out.Chain.StateTxRange = txRange
	out.Owner = fmt.Sprintf("%x", opts.Owner.Bytes())
	out.Inputs = inputs
	out.Plan = plan
	out.DeployedR1 = historicalRewardComputation{PendingReward: deployedPending, Allowance: account.Allowance(), WithdrawAmount: account.Allowance() + deployedPending}
	out.FixedJavaSemantics = historicalRewardComputation{PendingReward: fixedPending, Allowance: account.Allowance(), WithdrawAmount: account.Allowance() + fixedPending}
	out.ExplicitKeyReads = kv.reads

	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > opts.MaxOutputBytes {
		return fmt.Errorf("historical reward output is %d bytes, limit %d", len(data), opts.MaxOutputBytes)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeHistoricalExportAtomic(opts.OutputPath, data)
}

func findExpectedWithdraw(block *types.Block, owner tcommon.Address, wantTxID string) (int, string, []historicalPrecedingTransaction, error) {
	wantTxID = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(wantTxID)), "0x")
	found := -1
	gotID := ""
	for i, tx := range block.Transactions() {
		if tx.ContractType() != corepb.Transaction_Contract_WithdrawBalanceContract {
			continue
		}
		contract := tx.Contract()
		if contract == nil || contract.Parameter == nil {
			return -1, "", nil, fmt.Errorf("withdraw tx %d has no contract parameter", i)
		}
		decoded := new(contractpb.WithdrawBalanceContract)
		if err := contract.Parameter.UnmarshalTo(decoded); err != nil {
			return -1, "", nil, fmt.Errorf("decode withdraw tx %d: %w", i, err)
		}
		if tcommon.BytesToAddress(decoded.OwnerAddress) != owner {
			continue
		}
		if found >= 0 {
			return -1, "", nil, fmt.Errorf("multiple owner withdrawals in block %d", block.Number())
		}
		found, gotID = i, fmt.Sprintf("%x", tx.Hash())
	}
	if found < 0 {
		return -1, "", nil, fmt.Errorf("owner withdrawal absent from block %d", block.Number())
	}
	if gotID != wantTxID {
		return -1, "", nil, fmt.Errorf("withdraw tx id %s, want %s", gotID, wantTxID)
	}
	preceding := make([]historicalPrecedingTransaction, 0, found)
	for i, tx := range block.Transactions()[:found] {
		// For the incident block the sole predecessor is an unrelated TRC10
		// transfer. Decode it explicitly: parent poststate is a valid reward and
		// owner prestate only after proving this predecessor cannot touch either.
		if tx.ContractType() != corepb.Transaction_Contract_TransferAssetContract {
			return -1, "", nil, fmt.Errorf("preceding tx %d has type %s, want TransferAssetContract", i, tx.ContractType())
		}
		contract := tx.Contract()
		if contract == nil || contract.Parameter == nil {
			return -1, "", nil, fmt.Errorf("preceding tx %d has no contract parameter", i)
		}
		decoded := new(contractpb.TransferAssetContract)
		if err := contract.Parameter.UnmarshalTo(decoded); err != nil {
			return -1, "", nil, fmt.Errorf("decode preceding tx %d: %w", i, err)
		}
		from := tcommon.BytesToAddress(decoded.OwnerAddress)
		to := tcommon.BytesToAddress(decoded.ToAddress)
		if from == owner || to == owner {
			return -1, "", nil, fmt.Errorf("preceding tx %d touches target owner", i)
		}
		preceding = append(preceding, historicalPrecedingTransaction{
			Index: i, TxID: fmt.Sprintf("%x", tx.Hash()), ContractType: tx.ContractType().String(),
			Owner: fmt.Sprintf("%x", decoded.OwnerAddress), To: fmt.Sprintf("%x", decoded.ToAddress),
		})
	}
	return found, gotID, preceding, nil
}

func historicalStateTxRange(db ethdb.KeyValueReader, cold *snapshots.Manager, block uint64) (*rawdb.StateTxRange, error) {
	hot, hotOK, err := rawdb.ReadStateTxRange(db, block)
	if err != nil {
		return nil, err
	}
	coldRow, coldOK, err := cold.StateTxRangeForBlock(block)
	if err != nil {
		return nil, err
	}
	if hotOK && coldOK && (hot.BlockHash != coldRow.BlockHash || hot.BeginTxNum != coldRow.BeginTxNum || hot.EndTxNum != coldRow.EndTxNum) {
		return nil, fmt.Errorf("hot/cold state tx range mismatch at block %d", block)
	}
	if hotOK {
		return hot, nil
	}
	if coldOK {
		return coldRow, nil
	}
	return nil, fmt.Errorf("state tx range missing at block %d", block)
}

func readHistoricalRewardInputs(kv *historicalKVReader, owner tcommon.Address, account *types.Account) (historicalRewardInputs, error) {
	var out historicalRewardInputs
	out.AccountBalance = account.Balance()
	out.AccountAllowance = account.Allowance()
	out.AccountVotes = historicalVotes(account.Votes())
	var err error
	if out.BeginCycle, err = kv.int64(tcommon.SystemAccountAddress, kvdomains.SystemReward, rawdb.BeginCycleStateKey(owner.Bytes()), 0); err != nil {
		return out, err
	}
	if out.EndCycle, err = kv.int64(tcommon.SystemAccountAddress, kvdomains.SystemReward, rawdb.EndCycleStateKey(owner.Bytes()), rawdb.RewardRemark); err != nil {
		return out, err
	}
	if out.CurrentCycle, err = readHistoricalDP(kv, "current_cycle_number"); err != nil {
		return out, err
	}
	if out.NewAlgoCycle, err = readHistoricalDP(kv, "new_reward_algorithm_effective_cycle"); err != nil {
		return out, err
	}
	if out.AllowOldRewardOpt, err = readHistoricalDP(kv, "allow_old_reward_opt"); err != nil {
		return out, err
	}
	if out.ChangeDelegation, err = readHistoricalDP(kv, "change_delegation"); err != nil {
		return out, err
	}

	key := rawdb.CycleAccountVoteStateKey(out.BeginCycle.Value, owner.Bytes())
	raw, ok, err := kv.raw(tcommon.SystemAccountAddress, kvdomains.SystemReward, key)
	if err != nil {
		return out, err
	}
	snapshot := &historicalAccountSnapshot{KeyHex: hex.EncodeToString(key), Present: ok}
	if ok {
		snapshot.RawHex = hex.EncodeToString(raw)
		decoded := new(corepb.Account)
		if err := statecodec.Unmarshal(raw, decoded); err != nil {
			return out, fmt.Errorf("decode begin-cycle account-vote snapshot: %w", err)
		}
		snapshot.Allowance = decoded.GetAllowance()
		snapshot.Votes = historicalVotes(decoded.GetVotes())
	}
	out.BeginSnapshot = snapshot
	return out, nil
}

func readHistoricalDP(kv *historicalKVReader, key string) (historicalRawInt64, error) {
	missing, ok := state.DefaultDPInt64(key)
	if !ok {
		return historicalRawInt64{}, fmt.Errorf("unknown dynamic property %q", key)
	}
	return kv.int64(tcommon.SystemAccountAddress, kvdomains.SystemDynamicProperty, []byte(key), missing)
}

func historicalVotes(votes []*corepb.Vote) []historicalVote {
	out := make([]historicalVote, 0, len(votes))
	for _, vote := range votes {
		out = append(out, historicalVote{Witness: hex.EncodeToString(vote.GetVoteAddress()), Count: vote.GetVoteCount()})
	}
	return out
}

func planHistoricalRewardSegments(inputs historicalRewardInputs) historicalRewardPlan {
	begin, end, current := inputs.BeginCycle.Value, inputs.EndCycle.Value, inputs.CurrentCycle.Value
	plan := historicalRewardPlan{PostBeginCycle: begin, PostEndCycle: end}
	if inputs.ChangeDelegation.Value == 0 {
		plan.EarlyReturnReason = "change_delegation_disabled"
		return plan
	}
	if begin > current {
		plan.EarlyReturnReason = "begin_cycle_after_current"
		return plan
	}
	if begin == current && inputs.BeginSnapshot != nil && inputs.BeginSnapshot.Present {
		plan.EarlyReturnReason = "current_cycle_snapshot_already_present"
		return plan
	}
	if begin+1 == end && begin < current {
		if inputs.BeginSnapshot != nil && len(inputs.BeginSnapshot.Votes) > 0 {
			plan.Segments = append(plan.Segments, historicalRewardSegment{Source: "begin_cycle_account_vote_snapshot", BeginCycle: begin, EndCycle: end, Votes: inputs.BeginSnapshot.Votes})
		}
		begin++
	}
	end = current
	if len(inputs.AccountVotes) == 0 {
		plan.PostBeginCycle = end + 1
		plan.PostEndCycle = inputs.EndCycle.Value
		return plan
	}
	if begin < end {
		plan.Segments = append(plan.Segments, historicalRewardSegment{Source: "prestate_account_votes", BeginCycle: begin, EndCycle: end, Votes: inputs.AccountVotes})
	}
	plan.PostBeginCycle = end
	plan.PostEndCycle = end + 1
	return plan
}

func buildHistoricalRewardPlan(kv *historicalKVReader, inputs historicalRewardInputs, maxCycles int) (historicalRewardPlan, error) {
	plan := planHistoricalRewardSegments(inputs)
	totalCycles := int64(0)
	for _, segment := range plan.Segments {
		totalCycles += segment.EndCycle - segment.BeginCycle
	}
	if totalCycles < 0 || totalCycles > int64(maxCycles) {
		return plan, fmt.Errorf("historical reward plan spans %d cycles, limit %d", totalCycles, maxCycles)
	}
	for si := range plan.Segments {
		segment := &plan.Segments[si]
		for cycle := segment.BeginCycle; cycle < segment.EndCycle; cycle++ {
			if err := kv.readerContextError(); err != nil {
				return plan, err
			}
			row := historicalRewardCycle{Cycle: cycle, Rows: make([]historicalRewardRow, 0, len(segment.Votes))}
			for _, vote := range segment.Votes {
				addrBytes, err := hex.DecodeString(vote.Witness)
				if err != nil {
					return plan, err
				}
				cv, err := kv.int64(tcommon.SystemAccountAddress, kvdomains.SystemReward, rawdb.CycleVoteStateKey(cycle, addrBytes), rawdb.RewardRemark)
				if err != nil {
					return plan, err
				}
				cr, err := kv.int64(tcommon.SystemAccountAddress, kvdomains.SystemReward, rawdb.CycleRewardStateKey(cycle, addrBytes), 0)
				if err != nil {
					return plan, err
				}
				row.Rows = append(row.Rows, historicalRewardRow{Witness: vote.Witness, UserVotes: vote.Count, CycleVote: cv, CycleReward: cr})
			}
			segment.Cycles = append(segment.Cycles, row)
		}
		endpointCycles := historicalVIEndpointCycles(*segment, inputs.NewAlgoCycle.Value)
		for _, endpoint := range endpointCycles {
			for _, vote := range segment.Votes {
				addrBytes, err := hex.DecodeString(vote.Witness)
				if err != nil {
					return plan, err
				}
				vi, err := kv.bigInt(tcommon.SystemAccountAddress, kvdomains.SystemReward, rawdb.WitnessVIStateKey(endpoint, addrBytes))
				if err != nil {
					return plan, err
				}
				segment.VIEndpoints = append(segment.VIEndpoints, historicalVIEndpoint{Cycle: endpoint, Witness: vote.Witness, Value: vi})
			}
		}
	}
	return plan, nil
}

func historicalVIEndpointCycles(segment historicalRewardSegment, newAlgoCycle int64) []int64 {
	endpointSet := map[int64]struct{}{
		segment.BeginCycle - 1: {},
		segment.EndCycle - 1:   {},
	}
	// A hybrid segment's VI path starts at max(begin,newAlgo). Its begin-1
	// endpoint is distinct from the whole segment's begin-1 endpoint.
	newBegin := segment.BeginCycle
	if newBegin < newAlgoCycle {
		newBegin = newAlgoCycle
	}
	if newBegin < segment.EndCycle {
		endpointSet[newBegin-1] = struct{}{}
	}
	endpointCycles := make([]int64, 0, len(endpointSet))
	for endpoint := range endpointSet {
		endpointCycles = append(endpointCycles, endpoint)
	}
	sort.Slice(endpointCycles, func(i, j int) bool { return endpointCycles[i] < endpointCycles[j] })
	return endpointCycles
}

func (r *historicalKVReader) readerContextError() error {
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	default:
		return nil
	}
}

func calculateHistoricalReward(plan *historicalRewardPlan, newAlgoCycle int64, allowOldOpt bool) (int64, int64, error) {
	var deployed, fixed int64
	for i := range plan.Segments {
		segment := &plan.Segments[i]
		var deployedSegment, fixedSegment int64
		oldEnd := segment.EndCycle
		if newAlgoCycle < oldEnd {
			oldEnd = newAlgoCycle
		}
		if segment.BeginCycle < oldEnd {
			if allowOldOpt {
				value := calculateOldOpt(*segment, segment.BeginCycle, oldEnd)
				deployedSegment += value
				fixedSegment += value
			} else {
				d, f := calculateLegacyFloat(*segment, segment.BeginCycle, oldEnd)
				deployedSegment += d
				fixedSegment += f
			}
		}
		newBegin := segment.BeginCycle
		if newBegin < newAlgoCycle {
			newBegin = newAlgoCycle
		}
		if newBegin < segment.EndCycle {
			value, err := calculateVIDifference(*segment, newBegin, segment.EndCycle)
			if err != nil {
				return 0, 0, err
			}
			deployedSegment += value
			fixedSegment += value
		}
		// withdrawReward adds each independently computed segment only when that
		// segment's result is positive.
		if deployedSegment > 0 {
			deployed += deployedSegment
			segment.Computation.DeployedR1Credited = deployedSegment
		}
		if fixedSegment > 0 {
			fixed += fixedSegment
			segment.Computation.FixedJavaCredited = fixedSegment
		}
		segment.Computation.DeployedR1Raw = deployedSegment
		segment.Computation.FixedJavaRaw = fixedSegment
	}
	return deployed, fixed, nil
}

func calculateLegacyFloat(segment historicalRewardSegment, begin, end int64) (int64, int64) {
	var deployed, fixed int64
	for _, cycle := range segment.Cycles {
		if cycle.Cycle < begin || cycle.Cycle >= end {
			continue
		}
		var fixedCycle int64
		for _, row := range cycle.Rows {
			if row.CycleReward.Value <= 0 || row.CycleVote.Value == rawdb.RewardRemark || row.CycleVote.Value == 0 {
				continue
			}
			voteRate := float64(row.UserVotes) / float64(row.CycleVote.Value)
			product := float64(voteRate * float64(row.CycleReward.Value))
			deployed += tcommon.JavaDoubleToInt64(product)
			fixedCycle = tcommon.JavaDoubleToInt64(float64(fixedCycle) + product)
		}
		fixed += fixedCycle
	}
	return deployed, fixed
}

func calculateOldOpt(segment historicalRewardSegment, begin, end int64) int64 {
	decimal := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	var total int64
	for voteIndex, vote := range segment.Votes {
		viSum := new(big.Int)
		for _, cycle := range segment.Cycles {
			if cycle.Cycle < begin || cycle.Cycle >= end {
				continue
			}
			row := cycle.Rows[voteIndex]
			if row.CycleReward.Value == 0 || row.CycleVote.Value == 0 {
				continue
			}
			delta := new(big.Int).Mul(big.NewInt(row.CycleReward.Value), decimal)
			delta.Quo(delta, big.NewInt(row.CycleVote.Value))
			viSum.Add(viSum, delta)
		}
		if viSum.Sign() > 0 {
			share := new(big.Int).Mul(viSum, big.NewInt(vote.Count))
			share.Quo(share, decimal)
			total += share.Int64()
		}
	}
	return total
}

func calculateVIDifference(segment historicalRewardSegment, begin, end int64) (int64, error) {
	decimal := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	lookup := make(map[string]*big.Int, len(segment.VIEndpoints))
	for _, endpoint := range segment.VIEndpoints {
		value, ok := new(big.Int).SetString(endpoint.Value.Value, 10)
		if !ok {
			return 0, fmt.Errorf("invalid VI %q", endpoint.Value.Value)
		}
		lookup[fmt.Sprintf("%d/%s", endpoint.Cycle, endpoint.Witness)] = value
	}
	var total int64
	for _, vote := range segment.Votes {
		b := lookup[fmt.Sprintf("%d/%s", begin-1, vote.Witness)]
		e := lookup[fmt.Sprintf("%d/%s", end-1, vote.Witness)]
		if b == nil || e == nil {
			return 0, fmt.Errorf("missing VI endpoint for %s range [%d,%d)", vote.Witness, begin, end)
		}
		delta := new(big.Int).Sub(e, b)
		if delta.Sign() <= 0 {
			continue
		}
		share := new(big.Int).Mul(delta, big.NewInt(vote.Count))
		share.Quo(share, decimal)
		total += share.Int64()
	}
	return total, nil
}

func readHistoricalManifestIdentity(dir string) (historicalManifestIdentity, error) {
	var out historicalManifestIdentity
	if dir == "" {
		return out, fmt.Errorf("snapshot directory required")
	}
	manifest, err := snapshots.LoadProductionManifest(dir)
	if err != nil {
		return out, fmt.Errorf("load production manifest: %w", err)
	}
	path := snapshots.ManifestFile
	catalogChecksum := ""
	if catalog, err := snapshots.LoadSnapshotCatalog(dir); err == nil {
		path = catalog.ManifestPath
		catalogChecksum = strings.ToLower(catalog.ManifestChecksum)
	} else if !os.IsNotExist(err) {
		return out, fmt.Errorf("load snapshot catalog: %w", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, path))
	if err != nil {
		return out, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	if catalogChecksum != "" && digest != catalogChecksum {
		return out, fmt.Errorf("manifest checksum %s, catalog says %s", digest, catalogChecksum)
	}
	out.Generation = manifest.Generation
	out.Path = path
	out.SHA256 = digest
	out.CatalogChecksum = catalogChecksum
	out.VisibleTxStart = manifest.VisibleTxStart
	out.VisibleTxEnd = manifest.VisibleTxEnd
	return out, nil
}

func validateHistoricalOutputPath(dataDir, outputPath string) error {
	if outputPath == "" {
		return fmt.Errorf("export output path required")
	}
	absOutput, err := filepath.Abs(outputPath)
	if err != nil {
		return err
	}
	outputDir, err := filepath.EvalSymlinks(filepath.Dir(absOutput))
	if err != nil {
		return fmt.Errorf("export output directory must already exist: %w", err)
	}
	absOutput = filepath.Join(outputDir, filepath.Base(absOutput))
	if dataDir != "" {
		absData, err := filepath.Abs(dataDir)
		if err != nil {
			return err
		}
		absData, err = filepath.EvalSymlinks(absData)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(absData, absOutput)
		if err != nil {
			return err
		}
		if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))) {
			return fmt.Errorf("export output %s must be outside datadir %s", absOutput, absData)
		}
	}
	if _, err := os.Lstat(absOutput); err == nil {
		return fmt.Errorf("export output already exists: %s", absOutput)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

func writeHistoricalExportAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("export parent is not a directory: %s", dir)
	}
	tmp, err := os.CreateTemp(dir, ".reward-export-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// Link publishes the completed temporary inode atomically and fails if the
	// destination already exists; unlike Rename it cannot overwrite evidence.
	if err := os.Link(tmpPath, path); err != nil {
		return err
	}
	return nil
}
