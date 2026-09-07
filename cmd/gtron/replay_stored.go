package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/urfave/cli/v2"
	"google.golang.org/protobuf/proto"
)

var (
	syncReplayStoredToFlag = &cli.Uint64Flag{
		Name: "sync.replay-stored-to", Usage: "Offline: apply already-stored canonical bodies through this height, audit, close and exit (use an isolated checkpoint copy)",
	}
	syncReplayAuditFlag = &cli.StringFlag{
		Name: "sync.replay-audit", Usage: "Required new JSONL file for offline replay input boundary, per-block internal roots/history, and completion; never overwrites an existing file",
	}
	syncReplayCPUProfileFlag = &cli.StringFlag{
		Name: "sync.replay-cpuprofile", Usage: "Optional new CPU profile file covering offline replay and its final flush; no HTTP listener is started",
	}
)

func validateStoredReplayOptions(ctx *cli.Context) error {
	enabled := ctx.IsSet(syncReplayStoredToFlag.Name)
	if !enabled {
		if ctx.IsSet(syncReplayAuditFlag.Name) || ctx.IsSet(syncReplayCPUProfileFlag.Name) {
			return errors.New("--sync.replay-audit and --sync.replay-cpuprofile require --sync.replay-stored-to")
		}
		return nil
	}
	for _, name := range []string{"sync.restart-from", "sync.stop-at"} {
		if ctx.IsSet(name) {
			return fmt.Errorf("--sync.replay-stored-to is mutually exclusive with --%s", name)
		}
	}
	for _, name := range []string{"snapshot.bootstrap", "snapshot.reset", "snapshot.serve", "witness"} {
		if ctx.Bool(name) {
			return fmt.Errorf("--sync.replay-stored-to is mutually exclusive with --%s", name)
		}
	}
	if strings.TrimSpace(ctx.String(syncReplayAuditFlag.Name)) == "" {
		return errors.New("--sync.replay-stored-to requires a new --sync.replay-audit file")
	}
	if ctx.IsSet(syncReplayCPUProfileFlag.Name) && strings.TrimSpace(ctx.String(syncReplayCPUProfileFlag.Name)) == "" {
		return errors.New("--sync.replay-cpuprofile must name a new file")
	}
	return nil
}

type storedReplayAuditStart struct {
	Type            string `json:"type"`
	From            uint64 `json:"from"`
	To              uint64 `json:"to"`
	ParentHash      string `json:"parent_hash"`
	ParentStateRoot string `json:"parent_state_root"`
	TargetHash      string `json:"target_hash"`
	HistoryEnabled  bool   `json:"history_enabled"`
}

type storedReplayAuditBlock struct {
	Type             string              `json:"type"`
	Number           uint64              `json:"number"`
	Hash             string              `json:"hash"`
	ParentHash       string              `json:"parent_hash"`
	InternalRoot     string              `json:"internal_root"`
	TransactionCount uint64              `json:"transaction_count"`
	TxRange          *rawdb.StateTxRange `json:"tx_range,omitempty"`
	HistoryRows      uint64              `json:"history_rows,omitempty"`
	HistorySHA256    string              `json:"history_sha256,omitempty"`
	ReceiptsSHA256   string              `json:"receipts_sha256"`
}

type storedReplayAuditComplete struct {
	Type                 string  `json:"type"`
	From                 uint64  `json:"from"`
	To                   uint64  `json:"to"`
	Blocks               uint64  `json:"blocks"`
	Transactions         uint64  `json:"transactions"`
	FinalRoot            string  `json:"final_root"`
	ReplayElapsedSeconds float64 `json:"replay_elapsed_seconds"`
	TotalElapsedSeconds  float64 `json:"total_elapsed_seconds"`
}

// runStoredReplay must run after normal consensus, history and cold-reader
// wiring, but before any node lifecycles start. It never resets the checkpoint,
// fetches blocks, or opens listeners. The audit is produced after settled
// publication, so it reads actual internal roots rather than input headers.
func runStoredReplay(ctx *cli.Context, bc *core.BlockChain, historyEnabled bool, closeStores func() error) (retErr error) {
	storesClosed := false
	defer func() {
		if !storesClosed && closeStores != nil {
			retErr = errors.Join(retErr, closeStores())
		}
	}()
	if err := validateStoredReplayOptions(ctx); err != nil {
		return err
	}
	if bc == nil || bc.CurrentBlock() == nil {
		return errors.New("stored replay: missing current head")
	}
	head := bc.CurrentBlock()
	target := ctx.Uint64(syncReplayStoredToFlag.Name)
	if target <= head.Number() {
		return fmt.Errorf("stored replay: target %d must exceed current head %d", target, head.Number())
	}
	chain := bc.ChainDB()
	parentRoot, ok, err := rawdb.ReadBlockStateRootStrict(chain, head.Hash())
	if err == nil && !ok && head.Number() == 0 {
		parentRoot, ok, err = rawdb.ReadGenesisStateRootStrict(chain)
	}
	if err != nil || !ok || parentRoot == (common.Hash{}) {
		return fmt.Errorf("stored replay: checkpoint block %d has no valid internal root: %w", head.Number(), errors.Join(err, errMissingReplayRoot))
	}
	if err := checkStoredReplayLatestRoot(chain, parentRoot); err != nil {
		return fmt.Errorf("stored replay: checkpoint: %w", err)
	}
	targetBlock, ok, err := rawdb.ReadBlockStrict(chain, target)
	if err != nil {
		return fmt.Errorf("stored replay: read target %d: %w", target, err)
	}
	if !ok {
		return fmt.Errorf("stored replay: canonical block %d not found; bodies must already be present", target)
	}
	var parentEnd uint64
	if historyEnabled {
		parentEnd, err = storedReplayParentTxNum(chain, head)
		if err != nil {
			return fmt.Errorf("stored replay: checkpoint history range: %w", err)
		}
	}
	audit, err := os.OpenFile(ctx.String(syncReplayAuditFlag.Name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("stored replay: create audit: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, audit.Close()) }()
	buffer := bufio.NewWriter(audit)
	defer func() { retErr = errors.Join(retErr, buffer.Flush()) }()
	encoder := json.NewEncoder(buffer)
	if err := encoder.Encode(storedReplayAuditStart{
		Type: "start", From: head.Number() + 1, To: target,
		ParentHash: head.Hash().Hex(), ParentStateRoot: parentRoot.Hex(), TargetHash: targetBlock.Hash().Hex(), HistoryEnabled: historyEnabled,
	}); err != nil {
		return err
	}
	if err := buffer.Flush(); err != nil {
		return err
	}
	if err := audit.Sync(); err != nil {
		return err
	}

	var profile *os.File
	if path := ctx.String(syncReplayCPUProfileFlag.Name); path != "" {
		profile, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("stored replay: create CPU profile: %w", err)
		}
		if err := pprof.StartCPUProfile(profile); err != nil {
			return errors.Join(fmt.Errorf("stored replay: start CPU profile: %w", err), profile.Close())
		}
	}
	started := time.Now()
	replayErr := bc.ReplayStoredBlocksToHeight(target, func(progress core.RestartSyncProgress) {
		log.Info("Offline stored replay", "phase", progress.Phase, "block", progress.Block, "target", progress.Target)
	})
	// The method already drains each staged range and syncs its final buffer.
	// Explicitly join both queues on failure too before the caller closes stores.
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	replayElapsed := time.Since(started)
	if profile != nil {
		pprof.StopCPUProfile()
		retErr = errors.Join(profile.Sync(), profile.Close())
	}
	if replayErr != nil {
		// Match the existing restart failure policy: do not flush a failed
		// materialized image through Close. There is no successful completion
		// record; the operator starts the next attempt from a fresh checkpoint.
		return errors.Join(retErr, replayErr)
	}
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, bc.Close())
		}
	}()
	if retErr != nil {
		return retErr
	}

	parentHash := head.Hash()
	var transactionCount uint64
	var finalRoot common.Hash
	for n := head.Number() + 1; ; n++ {
		block, ok, err := rawdb.ReadBlockStrict(chain, n)
		if err != nil || !ok {
			return fmt.Errorf("stored replay: audit block %d missing or corrupt: %w", n, errors.Join(err, errors.New("canonical block unavailable")))
		}
		if block.ParentHash() != parentHash {
			return fmt.Errorf("stored replay: audit block %d parent mismatch", n)
		}
		root, ok, err := rawdb.ReadBlockStateRootStrict(chain, block.Hash())
		if err != nil || !ok || root == (common.Hash{}) {
			return fmt.Errorf("stored replay: audit block %d: %w", n, errors.Join(err, errMissingReplayRoot))
		}
		row := storedReplayAuditBlock{Type: "block", Number: n, Hash: block.Hash().Hex(), ParentHash: parentHash.Hex(), InternalRoot: root.Hex(), TransactionCount: uint64(len(block.Transactions()))}
		row.ReceiptsSHA256, err = storedReplayReceiptsDigest(chain, block)
		if err != nil {
			return err
		}
		if historyEnabled {
			row.TxRange, ok, err = rawdb.ReadStateTxRange(chain, n)
			if err != nil || !ok {
				return fmt.Errorf("stored replay: audit block %d history range missing or corrupt: %w", n, errors.Join(err, errors.New("history range unavailable")))
			}
			begin, end, err := rawdb.NextStateTxRange(parentEnd, row.TransactionCount)
			if err != nil {
				return err
			}
			if row.TxRange.BlockNum != n || row.TxRange.BlockHash != block.Hash() || row.TxRange.BeginTxNum != begin || row.TxRange.EndTxNum != end {
				return fmt.Errorf("stored replay: audit block %d history range/hash mismatch", n)
			}
			parentEnd = end
			digest := sha256.New()
			historyEncoder := json.NewEncoder(digest)
			if err := rawdb.IterateStateDomainChanges(chain, n, func(change *rawdb.StateDomainChange) (bool, error) {
				if change.BlockNum != n || (change.BlockHash != (common.Hash{}) && change.BlockHash != block.Hash()) || change.TxNum < begin || change.TxNum > end {
					return false, fmt.Errorf("stored replay: block %d history change outside canonical tx range", n)
				}
				// Fresh history stores its hash once in the verified tx-range;
				// block iteration deliberately leaves each row hash empty.
				normalized := *change
				normalized.BlockHash = block.Hash()
				row.HistoryRows++
				return true, historyEncoder.Encode(&normalized)
			}); err != nil {
				return err
			}
			row.HistorySHA256 = hex.EncodeToString(digest.Sum(nil))
		}
		if err := encoder.Encode(row); err != nil {
			return err
		}
		transactionCount += row.TransactionCount
		parentHash, finalRoot = block.Hash(), root
		if n == target {
			break
		}
	}
	final := bc.CurrentBlock()
	if final == nil || final.Number() != target || final.Hash() != targetBlock.Hash() || parentHash != targetBlock.Hash() {
		return errors.New("stored replay: audited endpoint does not match requested canonical target")
	}
	if err := checkStoredReplayLatestRoot(chain, finalRoot); err != nil {
		return err
	}
	if err := bc.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	if closeStores != nil {
		storesClosed = true
		if err := closeStores(); err != nil {
			return err
		}
	}
	if err := buffer.Flush(); err != nil {
		return err
	}
	if err := audit.Sync(); err != nil {
		return err
	}
	// Total excludes common startup and the completion record itself. It
	// includes replay, settled flush, optional profile finalization, per-block
	// audit, audit sync and successful BlockChain/store closes.
	if err := encoder.Encode(storedReplayAuditComplete{
		Type: "complete", From: head.Number() + 1, To: target, Blocks: target - head.Number(), Transactions: transactionCount, FinalRoot: finalRoot.Hex(),
		ReplayElapsedSeconds: replayElapsed.Seconds(), TotalElapsedSeconds: time.Since(started).Seconds(),
	}); err != nil {
		return err
	}
	if err := buffer.Flush(); err != nil {
		return err
	}
	return audit.Sync()
}

var errMissingReplayRoot = errors.New("internal state root missing or invalid")

func checkStoredReplayLatestRoot(chain *rawdb.ChainDB, expected common.Hash) error {
	root, ok, err := rawdb.ReadLatestDomainCommitmentRoot(chain)
	if err != nil {
		return fmt.Errorf("read latest commitment root: %w", err)
	}
	if !ok || root != expected {
		return fmt.Errorf("latest commitment root %x (present=%v) differs from block internal root %x", root, ok, expected)
	}
	return nil
}

// Hash length-framed deterministic protobufs so receipt energy, fees, return
// values and logs are compared independently of immutable input block hashes.
func storedReplayReceiptsDigest(chain *rawdb.ChainDB, block *types.Block) (string, error) {
	// Replay rewrites receipts in hot storage. An ancient-first lookup could
	// return the immutable input receipt and falsely certify new execution.
	hot := rawdb.NewChainDB(chain.KeyValueStore, rawdb.NoopAncient{})
	infos, present, err := rawdb.ReadTransactionInfosByBlockStrict(hot, block.Number())
	if err != nil {
		return "", fmt.Errorf("stored replay: block %d receipts: %w", block.Number(), err)
	}
	txs := block.Transactions()
	if len(infos) != len(txs) || (!present && len(txs) != 0) {
		return "", fmt.Errorf("stored replay: block %d receipt count %d differs from transactions %d (present=%v)", block.Number(), len(infos), len(txs), present)
	}
	// Current compact receipts omit IDs; restore them only after the
	// canonical-position validator has checked all present IDs and counts.
	if err := rawdb.PopulateTransactionInfoIDsForBlock(block.Number(), txs, infos, "stored replay audit"); err != nil {
		return "", err
	}
	digest := sha256.New()
	var length [binary.MaxVarintLen64]byte
	for i, info := range infos {
		data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(info)
		if err != nil {
			return "", fmt.Errorf("stored replay: block %d receipt %d encode: %w", block.Number(), i, err)
		}
		n := binary.PutUvarint(length[:], uint64(len(data)))
		_, _ = digest.Write(length[:n])
		_, _ = digest.Write(data)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// A non-genesis checkpoint used with history must carry the canonical range.
// The normal legacy fallback to blockNum is unsuitable as an A/B credential:
// replay and audit would otherwise agree on the same incorrect starting txNum.
func storedReplayParentTxNum(chain *rawdb.ChainDB, head *types.Block) (uint64, error) {
	row, present, err := rawdb.ReadStateTxRange(chain, head.Number())
	if err != nil {
		return 0, err
	}
	if !present {
		if head.Number() == 0 {
			return 0, nil
		}
		return 0, fmt.Errorf("checkpoint block %d history range missing", head.Number())
	}
	if row.BlockNum != head.Number() || row.BlockHash != head.Hash() || row.EndTxNum < row.BeginTxNum {
		return 0, fmt.Errorf("checkpoint block %d history range/hash mismatch", head.Number())
	}
	if head.Number() > 0 && row.EndTxNum-row.BeginTxNum != uint64(len(head.Transactions())) {
		return 0, fmt.Errorf("checkpoint block %d history range width differs from transaction count", head.Number())
	}
	return row.EndTxNum, nil
}
