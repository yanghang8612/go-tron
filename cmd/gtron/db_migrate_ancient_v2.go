package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

var (
	dbMigrateAncientV2YesFlag = &cli.BoolFlag{
		Name:  "yes",
		Usage: "Confirm the offline format migration and V1 space reclamation",
	}
	dbMigrateAncientV2FrameBlocksFlag = &cli.Uint64Flag{
		Name:  "frame-blocks",
		Usage: "Blocks per independently compressed Zstd frame",
		Value: 64,
	}
	dbMigrateAncientV2SegmentBlocksFlag = &cli.Uint64Flag{
		Name:  "segment-blocks",
		Usage: "Blocks per atomically published V2 segment",
		Value: 65_536,
	}
	dbMigrateAncientV2MaxSegmentsFlag = &cli.Uint64Flag{
		Name:  "max-segments",
		Usage: "Stop after N new segments (0 migrates every complete segment)",
	}
	dbMigrateAncientV2KeepV1Flag = &cli.BoolFlag{
		Name:  "keep-v1",
		Usage: "Publish and verify V2 segments without reclaiming V1 files",
	}
	dbMigrateAncientV2JSONFlag = &cli.BoolFlag{
		Name:  "json",
		Usage: "Write the migration summary as JSON",
	}
)

type ancientV2MigrationOutput struct {
	AncientPath         string  `json:"ancient_path"`
	Start               uint64  `json:"start"`
	End                 uint64  `json:"end"`
	Head                uint64  `json:"head"`
	Segments            uint64  `json:"segments"`
	FrameBlocks         uint32  `json:"frame_blocks"`
	SegmentBlocks       uint64  `json:"segment_blocks"`
	KeptV1              bool    `json:"kept_v1"`
	PhysicalBytesBefore uint64  `json:"physical_bytes_before"`
	PhysicalBytesAfter  uint64  `json:"physical_bytes_after"`
	ReclaimedBytes      uint64  `json:"reclaimed_bytes"`
	ElapsedSeconds      float64 `json:"elapsed_seconds"`
}

func dbMigrateAncientV2Command() *cli.Command {
	return &cli.Command{
		Name:        "migrate-ancient-v2",
		Usage:       "Convert bodies and tx_infos to seekable Zstd segments",
		Description: "The node using this datadir must be stopped. Each segment is fsynced, sampled, checksum-verified and atomically published before its V1 prefix is reclaimed. Interrupted runs resume from the last manifest.",
		Flags: []cli.Flag{
			dataDirFlag,
			dbMigrateAncientV2YesFlag,
			dbMigrateAncientV2FrameBlocksFlag,
			dbMigrateAncientV2SegmentBlocksFlag,
			dbMigrateAncientV2MaxSegmentsFlag,
			dbMigrateAncientV2KeepV1Flag,
			dbMigrateAncientV2JSONFlag,
		},
		Action: dbMigrateAncientV2Cmd,
	}
}

func dbMigrateAncientV2Cmd(ctx *cli.Context) error {
	if !ctx.Bool("yes") {
		return fmt.Errorf("refusing to migrate without --yes; stop gtron and rerun with explicit confirmation")
	}
	frameBlocks := ctx.Uint64("frame-blocks")
	if frameBlocks == 0 || frameBlocks > math.MaxUint32 {
		return fmt.Errorf("--frame-blocks must be between 1 and %d", uint64(math.MaxUint32))
	}
	segmentBlocks := ctx.Uint64("segment-blocks")
	if segmentBlocks == 0 {
		return fmt.Errorf("--segment-blocks must be positive")
	}
	path := ancientDataDir(ctx.String("datadir"))
	freezer, err := rawdbfreezer.NewFreezer(path, "", false, rawdbfreezer.FreezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return fmt.Errorf("open ancient %q (stop gtron before migration): %w", path, err)
	}
	defer freezer.Close()
	errWriter := ctx.App.ErrWriter
	if errWriter == nil {
		errWriter = os.Stderr
	}
	result, err := freezer.MigrateV2(rawdbfreezer.V2MigrationOptions{
		Tables:        []string{"bodies", "tx_infos"},
		SegmentBlocks: segmentBlocks,
		FrameBlocks:   uint32(frameBlocks),
		MaxSegments:   ctx.Uint64("max-segments"),
		KeepV1:        ctx.Bool("keep-v1"),
		Progress: func(progress rawdbfreezer.V2MigrationProgress) {
			if progress.Stage == "writing" {
				fmt.Fprintf(errWriter, "migrating segment=%d table=%s rows=%d/%d range=[%d,%d) elapsed=%s\n",
					progress.Segment, progress.Table, progress.Rows, segmentBlocks,
					progress.Start, progress.End, progress.Elapsed.Round(time.Second))
				return
			}
			fmt.Fprintf(errWriter, "migrated segment=%d range=[%d,%d) head=%d elapsed=%s physical=%s\n",
				progress.Segment, progress.Start, progress.End, progress.Head,
				progress.Elapsed.Round(time.Millisecond), formatIEC(progress.PhysicalBytes))
		},
	})
	if err != nil {
		return err
	}
	output := ancientV2MigrationOutput{
		AncientPath:         path,
		Start:               result.Start,
		End:                 result.End,
		Head:                result.Head,
		Segments:            result.Segments,
		FrameBlocks:         result.FrameBlocks,
		SegmentBlocks:       result.SegmentBlocks,
		KeptV1:              result.KeptV1,
		PhysicalBytesBefore: result.PhysicalBytesBefore,
		PhysicalBytesAfter:  result.PhysicalBytesAfter,
		ElapsedSeconds:      result.Elapsed.Seconds(),
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
	fmt.Fprintf(writer, "Ancient V2 migrated [%d,%d) in %s (%d segments, frame=%d).\n", output.Start, output.End, time.Duration(output.ElapsedSeconds*float64(time.Second)).Round(time.Millisecond), output.Segments, output.FrameBlocks)
	fmt.Fprintf(writer, "Selected-table physical files: %s before, %s after, %s reclaimed.\n", formatIEC(output.PhysicalBytesBefore), formatIEC(output.PhysicalBytesAfter), formatIEC(output.ReclaimedBytes))
	if output.KeptV1 {
		fmt.Fprintln(writer, "V1 source files were retained by --keep-v1.")
	}
	return nil
}
