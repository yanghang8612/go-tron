package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

var (
	dbCompactAncientTxInfoV2YesFlag = &cli.BoolFlag{
		Name:  "yes",
		Usage: "Confirm the offline V2 tx_infos audit and conditional rewrite",
	}
	dbCompactAncientTxInfoV2MaxSegmentsFlag = &cli.Uint64Flag{
		Name:  "max-segments",
		Usage: "Stop after auditing N pending segments (0 audits every pending segment)",
	}
	dbCompactAncientTxInfoV2ProgressFlag = &cli.DurationFlag{
		Name:  "progress",
		Usage: "Progress reporting interval (0 disables row-level heartbeats)",
		Value: 30 * time.Second,
	}
	dbCompactAncientTxInfoV2JSONFlag = &cli.BoolFlag{
		Name:  "json",
		Usage: "Write the audit/rewrite summary as JSON",
	}
)

type ancientTxInfoV2CompactOutput struct {
	AncientPath         string  `json:"ancient_path"`
	AuditedSegments     uint64  `json:"audited_segments"`
	RewrittenSegments   uint64  `json:"rewritten_segments"`
	Rows                uint64  `json:"rows"`
	DuplicateBytes      uint64  `json:"duplicate_bytes_removed_before_compression"`
	PhysicalBytesBefore uint64  `json:"physical_bytes_before"`
	PhysicalBytesAfter  uint64  `json:"physical_bytes_after"`
	ReclaimedBytes      uint64  `json:"reclaimed_bytes"`
	ElapsedSeconds      float64 `json:"elapsed_seconds"`
}

func dbCompactAncientTxInfoV2Command() *cli.Command {
	return &cli.Command{
		Name:        "compact-ancient-tx-info-v2",
		Usage:       "Audit V2 transaction receipts and rewrite only segments containing duplicate IDs",
		Description: "The node using this datadir must be stopped. ID-free segments only receive a durable manifest marker; segments containing validated duplicate IDs are checksummed and atomically replaced. Interrupted runs resume from the last published manifest.",
		Flags: []cli.Flag{
			dataDirFlag,
			dbCompactAncientTxInfoV2YesFlag,
			dbCompactAncientTxInfoV2MaxSegmentsFlag,
			dbCompactAncientTxInfoV2ProgressFlag,
			dbCompactAncientTxInfoV2JSONFlag,
		},
		Action: dbCompactAncientTxInfoV2Cmd,
	}
}

func dbCompactAncientTxInfoV2Cmd(ctx *cli.Context) error {
	if !ctx.Bool("yes") {
		return fmt.Errorf("refusing to audit/rewrite V2 tx_infos without --yes; stop gtron and rerun with explicit confirmation")
	}
	progressInterval := ctx.Duration("progress")
	if progressInterval < 0 {
		return fmt.Errorf("--progress must be >= 0")
	}
	path := ancientDataDir(ctx.String("datadir"))
	freezer, err := rawdbfreezer.NewFreezer(path, "", false, rawdbfreezer.FreezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return fmt.Errorf("open ancient %q (stop gtron before audit): %w", path, err)
	}
	defer freezer.Close()
	if coverage, indexCoverage := freezer.V2Coverage(), freezer.TransactionIndexCoverage(); indexCoverage < coverage {
		return fmt.Errorf("transaction index coverage %d is behind ancient V2 coverage %d; run migrate-tx-index first", indexCoverage, coverage)
	}
	errWriter := ctx.App.ErrWriter
	if errWriter == nil {
		errWriter = os.Stderr
	}
	result, err := freezer.RewriteV2TransactionInfos(rawdbfreezer.V2TxInfoRewriteOptions{
		MaxSegments:      ctx.Uint64("max-segments"),
		ProgressInterval: progressInterval,
		Transform: func(_ uint64, txInfo, body []byte) ([]byte, uint64, error) {
			compact, _, removed, err := rawdb.CompactTransactionInfoIDsForBlock(txInfo, body)
			if err != nil {
				return txInfo, 0, nil
			}
			return compact, uint64(removed), nil
		},
		Progress: func(progress rawdbfreezer.V2TxInfoRewriteProgress) {
			fmt.Fprintf(errWriter, "%s tx_infos segment=%d range=[%d,%d) rows=%d duplicate=%s elapsed=%s\n",
				progress.Stage, progress.Segment, progress.Start, progress.End, progress.Rows,
				formatIEC(progress.RemovedBytes), progress.Elapsed.Round(time.Millisecond))
		},
	})
	if err != nil {
		return err
	}
	output := ancientTxInfoV2CompactOutput{
		AncientPath: path, AuditedSegments: result.Segments, RewrittenSegments: result.RewrittenSegments,
		Rows: result.Rows, DuplicateBytes: result.RemovedBytes,
		PhysicalBytesBefore: result.PhysicalBytesBefore, PhysicalBytesAfter: result.PhysicalBytesAfter,
		ElapsedSeconds: result.Elapsed.Seconds(),
	}
	if output.PhysicalBytesBefore > output.PhysicalBytesAfter {
		output.ReclaimedBytes = output.PhysicalBytesBefore - output.PhysicalBytesAfter
	}
	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	if ctx.Bool("json") {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(output)
	}
	fmt.Fprintf(writer, "Ancient V2 tx_infos audited: %d segments and %d rows; %d segments rewritten in %s.\n",
		output.AuditedSegments, output.Rows, output.RewrittenSegments,
		time.Duration(output.ElapsedSeconds*float64(time.Second)).Round(time.Millisecond))
	fmt.Fprintf(writer, "Duplicate ID bytes removed: %s. Physical tx_infos: %s before, %s after, %s reclaimed.\n",
		formatIEC(output.DuplicateBytes), formatIEC(output.PhysicalBytesBefore),
		formatIEC(output.PhysicalBytesAfter), formatIEC(output.ReclaimedBytes))
	return nil
}
