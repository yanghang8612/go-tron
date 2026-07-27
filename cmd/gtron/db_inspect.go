package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

var (
	dbInspectJSONFlag = &cli.BoolFlag{
		Name:  "json",
		Usage: "Write the complete inspection report as JSON",
	}
	dbInspectTopFlag = &cli.IntFlag{
		Name:  "top",
		Usage: "Show only the N largest chaindata keyspaces in text output (0 = all)",
		Value: 25,
	}
	dbInspectProgressFlag = &cli.DurationFlag{
		Name:  "progress",
		Usage: "Progress reporting interval (0 disables)",
		Value: 5 * time.Second,
	}
)

type dbInspectionOutput struct {
	ChaindataPath          string                   `json:"chaindata_path"`
	ChaindataPhysicalBytes uint64                   `json:"chaindata_physical_bytes"`
	ScanSeconds            float64                  `json:"scan_seconds"`
	Chaindata              rawdb.DatabaseInspection `json:"chaindata"`
	Ancient                *ancientInspection       `json:"ancient,omitempty"`
}

type ancientInspection struct {
	Path          string             `json:"path"`
	PhysicalBytes uint64             `json:"physical_bytes"`
	Tables        []ancientTableStat `json:"tables"`
}

type ancientTableStat struct {
	Name          string  `json:"name"`
	Rows          uint64  `json:"rows"`
	PhysicalBytes uint64  `json:"physical_bytes"`
	Percent       float64 `json:"percent"`
}

func dbCommand() *cli.Command {
	return &cli.Command{
		Name:  "db",
		Usage: "Inspect and maintain database storage",
		Subcommands: []*cli.Command{
			{
				Name:        "inspect",
				Usage:       "Scan an offline database and report storage by logical keyspace",
				Description: "The node using this datadir must be stopped. Chaindata is opened read-only and every live key is scanned once; ancient files are summarized by table.",
				Flags: []cli.Flag{
					dataDirFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbInspectJSONFlag,
					dbInspectTopFlag,
					dbInspectProgressFlag,
				},
				Action: dbInspectCmd,
			},
			dbBenchmarkAncientCommand(),
			dbPruneTxInfoCommand(),
		},
	}
}

func dbInspectCmd(ctx *cli.Context) error {
	if ctx.Int("top") < 0 {
		return fmt.Errorf("--top must be >= 0")
	}
	if ctx.Duration("progress") < 0 {
		return fmt.Errorf("--progress must be >= 0")
	}
	cache := intFlagOrDefault(ctx, "db.cache", dbCacheFlag.Value)
	if cache <= 0 {
		return fmt.Errorf("--db.cache must be positive")
	}
	handles := intFlagOrDefault(ctx, "db.handles", dbHandlesFlag.Value)
	if handles <= 0 {
		return fmt.Errorf("--db.handles must be positive")
	}

	chaindataPath := chainDataDir(ctx.String("datadir"))
	physicalBytes, err := directoryBytes(chaindataPath)
	if err != nil {
		return fmt.Errorf("inspect chaindata files %q: %w", chaindataPath, err)
	}
	db, err := rawdb.NewPebbleDBReadOnly(chaindataPath, cache, handles)
	if err != nil {
		return fmt.Errorf("open chaindata read-only %q (stop gtron before inspection): %w", chaindataPath, err)
	}
	defer db.Close()

	errWriter := ctx.App.ErrWriter
	if errWriter == nil {
		errWriter = os.Stderr
	}
	started := time.Now()
	inspection, err := rawdb.InspectDatabase(db, rawdb.InspectOptions{
		ProgressInterval: ctx.Duration("progress"),
		Progress: func(progress rawdb.InspectProgress) {
			fmt.Fprintf(errWriter, "scanned rows=%d logical=%s elapsed=%s\n", progress.Rows, formatIEC(progress.LogicalBytes), progress.Elapsed.Round(time.Second))
		},
	})
	if err != nil {
		return err
	}

	output := dbInspectionOutput{
		ChaindataPath:          chaindataPath,
		ChaindataPhysicalBytes: physicalBytes,
		ScanSeconds:            time.Since(started).Seconds(),
		Chaindata:              inspection,
	}
	ancientPath := ancientDataDir(ctx.String("datadir"))
	if info, statErr := os.Stat(ancientPath); statErr == nil && info.IsDir() {
		ancient, inspectErr := inspectAncient(ancientPath)
		if inspectErr != nil {
			return fmt.Errorf("inspect ancient %q (stop gtron before inspection): %w", ancientPath, inspectErr)
		}
		output.Ancient = ancient
	} else if statErr != nil && !os.IsNotExist(statErr) {
		return fmt.Errorf("stat ancient %q: %w", ancientPath, statErr)
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
	writeDBInspectionText(writer, output, ctx.Int("top"))
	return nil
}

func inspectAncient(path string) (*ancientInspection, error) {
	total, err := directoryBytes(path)
	if err != nil {
		return nil, err
	}
	tableConfigs := chainfreezer.FreezerTableSet()
	tableBytes := make(map[string]uint64, len(tableConfigs))
	if err := filepath.WalkDir(path, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		name := entry.Name()
		for table := range tableConfigs {
			if strings.HasPrefix(name, table+".") {
				info, err := entry.Info()
				if err != nil {
					return err
				}
				tableBytes[table] += uint64(info.Size())
				break
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	freezer, err := rawdbfreezer.NewFreezer(path, "", true, rawdbfreezer.FreezerTableSize, tableConfigs)
	if err != nil {
		return nil, err
	}
	defer freezer.Close()

	report := &ancientInspection{Path: path, PhysicalBytes: total}
	for table := range tableConfigs {
		rows, err := freezer.AncientCount(table)
		if err != nil {
			return nil, err
		}
		stat := ancientTableStat{Name: table, Rows: rows, PhysicalBytes: tableBytes[table]}
		if total != 0 {
			stat.Percent = float64(stat.PhysicalBytes) * 100 / float64(total)
		}
		report.Tables = append(report.Tables, stat)
	}
	sort.Slice(report.Tables, func(i, j int) bool {
		if report.Tables[i].PhysicalBytes == report.Tables[j].PhysicalBytes {
			return report.Tables[i].Name < report.Tables[j].Name
		}
		return report.Tables[i].PhysicalBytes > report.Tables[j].PhysicalBytes
	})
	return report, nil
}

func directoryBytes(path string) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(path, func(_ string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += uint64(info.Size())
		return nil
	})
	return total, err
}

func writeDBInspectionText(writer io.Writer, output dbInspectionOutput, top int) {
	fmt.Fprintf(writer, "Chaindata: %s\n", output.ChaindataPath)
	fmt.Fprintf(writer, "Physical files: %s\n", formatIEC(output.ChaindataPhysicalBytes))
	fmt.Fprintf(writer, "Live logical data: %s (%d rows; keys %s, values %s)\n", formatIEC(output.Chaindata.LogicalBytes), output.Chaindata.Rows, formatIEC(output.Chaindata.KeyBytes), formatIEC(output.Chaindata.ValueBytes))
	fmt.Fprintf(writer, "Scan time: %s\n\n", time.Duration(output.ScanSeconds*float64(time.Second)).Round(time.Millisecond))

	spaces := output.Chaindata.Keyspaces
	if top > 0 && len(spaces) > top {
		spaces = spaces[:top]
	}
	tw := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "KEYSPACE\tKEY/PREFIX\tROWS\tKEY BYTES\tVALUE BYTES\tLOGICAL\tSHARE")
	for _, stat := range spaces {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\t%.2f%%\n", stat.Name, stat.KeyPattern, stat.Rows, formatIEC(stat.KeyBytes), formatIEC(stat.ValueBytes), formatIEC(stat.LogicalBytes), stat.Percent)
	}
	_ = tw.Flush()
	if len(spaces) != len(output.Chaindata.Keyspaces) {
		fmt.Fprintf(writer, "... %d keyspaces omitted; use --top 0 or --json for all.\n", len(output.Chaindata.Keyspaces)-len(spaces))
	}

	if output.Ancient != nil {
		fmt.Fprintf(writer, "\nAncient: %s\n", output.Ancient.Path)
		fmt.Fprintf(writer, "Physical files: %s\n", formatIEC(output.Ancient.PhysicalBytes))
		tw = tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "TABLE\tROWS\tPHYSICAL\tSHARE")
		for _, stat := range output.Ancient.Tables {
			fmt.Fprintf(tw, "%s\t%d\t%s\t%.2f%%\n", stat.Name, stat.Rows, formatIEC(stat.PhysicalBytes), stat.Percent)
		}
		_ = tw.Flush()
	}
	fmt.Fprintln(writer, "\nNote: chaindata logical bytes are uncompressed live key/value bytes, not per-keyspace SST disk usage. Ancient bytes are actual table file sizes.")
}

func formatIEC(bytes uint64) string {
	const unit = uint64(1024)
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := unit, 0
	for n := bytes / unit; n >= unit && exp < 5; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
