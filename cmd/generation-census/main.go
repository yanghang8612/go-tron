// generation-census exhaustively scans an immutable offline chaindata copy.
// It has no pruning or database-writing path.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func main() {
	if err := runArgs(os.Args[1:]); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func outsideOutput(db, output string) (string, error) {
	parent, err := filepath.EvalSymlinks(filepath.Dir(output))
	if err != nil {
		return "", err
	}
	parent, err = filepath.Abs(parent)
	if err != nil {
		return "", err
	}
	output = filepath.Join(parent, filepath.Base(output))
	rel, err := filepath.Rel(db, output)
	if err != nil || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("report must be outside source database")
	}
	return output, nil
}

func runArgs(args []string) error {
	if len(args) > 0 && args[0] == "verify-account-pointers" {
		return runPointerArgs(args[1:])
	}
	flags := flag.NewFlagSet("generation-census", flag.ContinueOnError)
	dir := flags.String("db-dir", "", "existing immutable offline chaindata copy; exclusive Pebble lock required")
	out := flags.String("out", "", "new JSON report outside source DB")
	groups := flags.String("groups", "", "new JSONL of all inactive owner/generation/domain groups outside source DB")
	verify := flags.Bool("verify-muhash", true, "recompute all physical source families and compare complete normalized HEAD")
	duration := flags.Duration("max-duration", 2*time.Hour, "maximum scan duration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *out == "" || *groups == "" || *duration <= 0 || flags.NArg() != 0 {
		return errors.New("db-dir, out, groups and positive max-duration required")
	}
	path, err := filepath.EvalSymlinks(*dir)
	if err != nil {
		return err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}
	if _, err = os.Stat(filepath.Join(path, "CURRENT")); err != nil {
		return err
	}
	output, err := outsideOutput(path, *out)
	if err != nil {
		return err
	}
	groupPath, err := outsideOutput(path, *groups)
	if err != nil {
		return err
	}
	if output == groupPath {
		return errors.New("report and groups must be different new files")
	}
	f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	g, err := os.OpenFile(groupPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		f.Close()
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	db, openErr := rawdb.NewPebbleDBReadOnly(path, 64, 128)
	var report rawdb.GenerationCensus
	var pointers accountPointerCensus
	var scanErr, closeErr error
	if openErr == nil {
		last := time.Now()
		enc := json.NewEncoder(g)
		report, scanErr = rawdb.InspectStateKVGenerations(ctx, db, *verify, func(row rawdb.GenerationGroup) error { return enc.Encode(row) }, func(phase string, rows uint64) {
			if time.Since(last) >= 30*time.Second {
				fmt.Fprintf(os.Stderr, "%s phase=%s rows=%d\n", time.Now().UTC().Format(time.RFC3339), phase, rows)
				last = time.Now()
			}
		})
		if scanErr == nil {
			pointers, scanErr = inspectAccountPointers(ctx, db)
			if scanErr == nil && pointers.Accounts != report.SourceRows[0] {
				scanErr = errors.New("account pointer census count differs from physical source")
			}
		}
		closeErr = db.Close()
	}
	resultErr := errors.Join(openErr, scanErr, closeErr, g.Sync(), g.Close())
	if resultErr != nil {
		report.Complete = false
		report.Error = resultErr.Error()
	}
	result := struct {
		Source   string               `json:"source"`
		Note     string               `json:"note"`
		Pointers accountPointerCensus `json:"account_generation_pointers"`
		rawdb.GenerationCensus
	}{path, "Exhaustive visible logical KV census on immutable offline copy. Absent generation=0; obsolete means stored<current; future is an anomaly. Logical bytes are not compressed physical reclaim. No deletion authority or production changes.", pointers, report}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	writeErr := enc.Encode(result)
	return errors.Join(resultErr, writeErr, f.Sync(), f.Close())
}
