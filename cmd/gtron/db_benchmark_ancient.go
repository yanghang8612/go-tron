package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

const (
	defaultAncientBenchmarkSamples = uint64(65_536)
	defaultAncientBenchmarkWindows = uint64(8)
	currentFreezerIndexBytes       = uint64(6)
	v2FrameHeaderBytes             = uint64(32)
	v2FileHeaderBytes              = uint64(48)
)

var (
	dbBenchmarkSampleBlocksFlag = &cli.Uint64Flag{
		Name:  "sample-blocks",
		Usage: "Total number of blocks sampled across the ancient range",
		Value: defaultAncientBenchmarkSamples,
	}
	dbBenchmarkWindowsFlag = &cli.Uint64Flag{
		Name:  "windows",
		Usage: "Number of evenly distributed contiguous sample windows",
		Value: defaultAncientBenchmarkWindows,
	}
	dbBenchmarkFramesFlag = &cli.StringFlag{
		Name:  "frames",
		Usage: "Comma-separated Zstd frame sizes in blocks",
		Value: "32,64,128,256",
	}
	dbBenchmarkJSONFlag = &cli.BoolFlag{
		Name:  "json",
		Usage: "Write the complete benchmark report as JSON",
	}
)

type ancientBenchmarkOutput struct {
	AncientPath   string                  `json:"ancient_path"`
	SampleBlocks  uint64                  `json:"sample_blocks"`
	Windows       []ancientBenchmarkRange `json:"windows"`
	FrameBlocks   []uint64                `json:"frame_blocks"`
	ElapsedSecond float64                 `json:"elapsed_seconds"`
	Tables        []ancientBenchmarkTable `json:"tables"`
}

type ancientBenchmarkRange struct {
	Start uint64 `json:"start"`
	Count uint64 `json:"count"`
}

type ancientBenchmarkTable struct {
	Name          string                  `json:"name"`
	Rows          uint64                  `json:"rows"`
	PhysicalBytes uint64                  `json:"physical_bytes"`
	SampledRows   uint64                  `json:"sampled_rows"`
	RawBytes      uint64                  `json:"raw_bytes"`
	Codecs        []ancientBenchmarkCodec `json:"codecs"`
}

type ancientBenchmarkCodec struct {
	Name                  string  `json:"name"`
	FrameBlocks           uint64  `json:"frame_blocks,omitempty"`
	SampleBytes           uint64  `json:"sample_bytes"`
	ProjectedPhysicalByte uint64  `json:"projected_physical_bytes"`
	SavingsPercent        float64 `json:"savings_percent"`
}

type ancientFrameSample struct {
	blocks uint64
	rows   uint64
	bytes  uint64
	buf    []byte
}

func dbBenchmarkAncientCommand() *cli.Command {
	return &cli.Command{
		Name:        "benchmark-ancient",
		Usage:       "Measure candidate compression layouts against offline ancient data",
		Description: "The node using this datadir must be stopped. Samples bodies and tx_infos across the frozen range without modifying any files.",
		Flags: []cli.Flag{
			dataDirFlag,
			dbBenchmarkSampleBlocksFlag,
			dbBenchmarkWindowsFlag,
			dbBenchmarkFramesFlag,
			dbInspectProgressFlag,
			dbBenchmarkJSONFlag,
		},
		Action: dbBenchmarkAncientCmd,
	}
}

func dbBenchmarkAncientCmd(ctx *cli.Context) error {
	if ctx.Uint64("sample-blocks") == 0 {
		return fmt.Errorf("--sample-blocks must be positive")
	}
	if ctx.Uint64("windows") == 0 {
		return fmt.Errorf("--windows must be positive")
	}
	if ctx.Duration("progress") < 0 {
		return fmt.Errorf("--progress must be >= 0")
	}
	frameBlocks, err := parseAncientFrameBlocks(ctx.String("frames"))
	if err != nil {
		return err
	}
	path := ancientDataDir(ctx.String("datadir"))
	freezer, err := rawdbfreezer.NewFreezer(path, "", true, rawdbfreezer.FreezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return fmt.Errorf("open ancient %q (stop gtron before benchmarking): %w", path, err)
	}
	defer freezer.Close()
	if coverage := freezer.V2Coverage(); coverage != 0 {
		return fmt.Errorf("ancient V2 already covers [0,%d); benchmark projections require an unmigrated V1 freezer", coverage)
	}

	rows, err := freezer.AncientCount("bodies")
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("ancient store %q is empty", path)
	}
	ranges := planAncientBenchmarkRanges(rows, ctx.Uint64("sample-blocks"), ctx.Uint64("windows"))
	output := ancientBenchmarkOutput{
		AncientPath:  path,
		Windows:      ranges,
		FrameBlocks:  frameBlocks,
		SampleBlocks: sumAncientBenchmarkRanges(ranges),
	}
	progressWriter := ctx.App.ErrWriter
	if progressWriter == nil {
		progressWriter = os.Stderr
	}
	started := time.Now()
	for _, table := range []string{"bodies", "tx_infos"} {
		stat, err := benchmarkAncientTable(freezer, table, ranges, frameBlocks, ctx.Duration("progress"), progressWriter)
		if err != nil {
			return err
		}
		output.Tables = append(output.Tables, stat)
	}
	output.ElapsedSecond = time.Since(started).Seconds()

	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	if ctx.Bool("json") {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(output)
	}
	writeAncientBenchmarkText(writer, output)
	return nil
}

func parseAncientFrameBlocks(value string) ([]uint64, error) {
	seen := make(map[uint64]struct{})
	var frames []uint64
	for _, field := range strings.Split(value, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		n, err := strconv.ParseUint(field, 10, 64)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("invalid --frames value %q", field)
		}
		if n > 65_536 {
			return nil, fmt.Errorf("--frames value %d exceeds 65536", n)
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		frames = append(frames, n)
	}
	if len(frames) == 0 {
		return nil, fmt.Errorf("--frames must contain at least one positive size")
	}
	sort.Slice(frames, func(i, j int) bool { return frames[i] < frames[j] })
	return frames, nil
}

func planAncientBenchmarkRanges(total, requested, requestedWindows uint64) []ancientBenchmarkRange {
	if requested > total {
		requested = total
	}
	windows := requestedWindows
	if windows > requested {
		windows = requested
	}
	if windows > total {
		windows = total
	}
	if windows == 0 {
		return nil
	}
	base, remainder := requested/windows, requested%windows
	ranges := make([]ancientBenchmarkRange, 0, windows)
	for i := uint64(0); i < windows; i++ {
		count := base
		if i < remainder {
			count++
		}
		binStart := total * i / windows
		binEnd := total * (i + 1) / windows
		if count > binEnd-binStart {
			count = binEnd - binStart
		}
		start := binStart + (binEnd-binStart-count)/2
		ranges = append(ranges, ancientBenchmarkRange{Start: start, Count: count})
	}
	return ranges
}

func sumAncientBenchmarkRanges(ranges []ancientBenchmarkRange) uint64 {
	var total uint64
	for _, sampleRange := range ranges {
		total += sampleRange.Count
	}
	return total
}

func benchmarkAncientTable(freezer *rawdbfreezer.Freezer, table string, ranges []ancientBenchmarkRange, frameBlocks []uint64, progress time.Duration, progressWriter io.Writer) (ancientBenchmarkTable, error) {
	rows, err := freezer.AncientCount(table)
	if err != nil {
		return ancientBenchmarkTable{}, err
	}
	physical, err := freezer.AncientSize(table)
	if err != nil {
		return ancientBenchmarkTable{}, err
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return ancientBenchmarkTable{}, err
	}
	defer enc.Close()

	frames := make([]ancientFrameSample, len(frameBlocks))
	for i, blocks := range frameBlocks {
		frames[i].blocks = blocks
	}
	var sampledRows, rawBytes, snappyBytes, zstdRowBytes uint64
	lastProgress := time.Now()
	flushFrames := func() {
		for i := range frames {
			if frames[i].rows == 0 {
				continue
			}
			frames[i].bytes += uint64(len(enc.EncodeAll(frames[i].buf, nil))) + v2FrameHeaderBytes
			frames[i].rows = 0
			frames[i].buf = frames[i].buf[:0]
		}
	}
	for _, sampleRange := range ranges {
		if sampleRange.Start >= rows {
			continue
		}
		end := sampleRange.Start + sampleRange.Count
		if end > rows {
			end = rows
		}
		for number := sampleRange.Start; number < end; number++ {
			raw, err := freezer.Ancient(table, number)
			if err != nil {
				return ancientBenchmarkTable{}, fmt.Errorf("read ancient %s[%d]: %w", table, number, err)
			}
			sampledRows++
			rawBytes += uint64(len(raw))
			snappyBytes += uint64(len(snappy.Encode(nil, raw))) + currentFreezerIndexBytes
			zstdRowBytes += uint64(len(enc.EncodeAll(raw, nil))) + currentFreezerIndexBytes
			for i := range frames {
				frames[i].buf = binary.AppendUvarint(frames[i].buf, uint64(len(raw)))
				frames[i].buf = append(frames[i].buf, raw...)
				frames[i].rows++
				if frames[i].rows == frames[i].blocks {
					frames[i].bytes += uint64(len(enc.EncodeAll(frames[i].buf, nil))) + v2FrameHeaderBytes
					frames[i].rows = 0
					frames[i].buf = frames[i].buf[:0]
				}
			}
			if progress > 0 && time.Since(lastProgress) >= progress {
				fmt.Fprintf(progressWriter, "benchmarked table=%s rows=%d/%d raw=%s\n", table, sampledRows, sumAncientBenchmarkRanges(ranges), formatIEC(rawBytes))
				lastProgress = time.Now()
			}
		}
		// A frame must never span the unsampled gap between two windows.
		flushFrames()
	}

	stat := ancientBenchmarkTable{Name: table, Rows: rows, PhysicalBytes: physical, SampledRows: sampledRows, RawBytes: rawBytes}
	stat.Codecs = append(stat.Codecs,
		makeAncientBenchmarkCodec("snappy-row-current", 1, snappyBytes, snappyBytes, physical),
		makeAncientBenchmarkCodec("zstd-row", 1, zstdRowBytes, snappyBytes, physical),
	)
	for _, frame := range frames {
		bytes := frame.bytes + v2FileHeaderBytes
		stat.Codecs = append(stat.Codecs, makeAncientBenchmarkCodec("zstd-frame", frame.blocks, bytes, snappyBytes, physical))
	}
	return stat, nil
}

func makeAncientBenchmarkCodec(name string, frameBlocks, sampleBytes, currentSampleBytes, currentPhysicalBytes uint64) ancientBenchmarkCodec {
	codec := ancientBenchmarkCodec{Name: name, FrameBlocks: frameBlocks, SampleBytes: sampleBytes}
	if currentSampleBytes == 0 {
		return codec
	}
	ratio := float64(sampleBytes) / float64(currentSampleBytes)
	codec.ProjectedPhysicalByte = uint64(float64(currentPhysicalBytes) * ratio)
	codec.SavingsPercent = (1 - ratio) * 100
	return codec
}

func writeAncientBenchmarkText(writer io.Writer, output ancientBenchmarkOutput) {
	fmt.Fprintf(writer, "Ancient: %s\n", output.AncientPath)
	fmt.Fprintf(writer, "Sample: %d blocks across %d windows; elapsed %s\n", output.SampleBlocks, len(output.Windows), time.Duration(output.ElapsedSecond*float64(time.Second)).Round(time.Millisecond))
	for _, table := range output.Tables {
		fmt.Fprintf(writer, "\n%s: %d rows, current physical %s, sampled raw %s\n", table.Name, table.Rows, formatIEC(table.PhysicalBytes), formatIEC(table.RawBytes))
		tw := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "CODEC\tFRAME BLOCKS\tSAMPLE\tPROJECTED\tSAVING")
		for _, codec := range table.Codecs {
			fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%.2f%%\n", codec.Name, codec.FrameBlocks, formatIEC(codec.SampleBytes), formatIEC(codec.ProjectedPhysicalByte), codec.SavingsPercent)
		}
		_ = tw.Flush()
	}
	fmt.Fprintln(writer, "\nProjection scales each table's measured physical bytes by the candidate/current sample ratio. Run on a stopped node; the command is read-only.")
}
