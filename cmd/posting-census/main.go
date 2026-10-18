// posting-census scans an offline immutable Pebble copy. It cannot delete data.
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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func main() {
	if err := runArgs(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runArgs(args []string) error {
	flags := flag.NewFlagSet("posting-census", flag.ContinueOnError)
	dir := flags.String("db-dir", "", "existing offline immutable history Pebble copy (takes its exclusive DB lock)")
	cut := flags.String("cutoffs", "", "1..8 positive increasing comma-separated inclusive block boundaries; not deletion authorization")
	out := flags.String("out", "", "new report file outside the DB directory")
	timeout := flags.Duration("max-duration", 2*time.Hour, "cancel an incomplete scan after this duration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *out == "" || *timeout <= 0 || flags.NArg() != 0 {
		return errors.New("db-dir, cutoffs, positive max-duration and new out are required")
	}
	var cutoffs []uint64
	for _, part := range strings.Split(*cut, ",") {
		h, err := strconv.ParseUint(part, 10, 64)
		if err != nil || h == 0 || len(cutoffs) > 0 && h <= cutoffs[len(cutoffs)-1] {
			return errors.New("cutoffs must be positive and strictly increasing")
		}
		cutoffs = append(cutoffs, h)
	}
	if len(cutoffs) > 8 {
		return errors.New("at most eight cutoffs")
	}
	path, err := filepath.EvalSymlinks(*dir)
	if err != nil {
		return err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(path, "CURRENT")); err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(*out))
	if err != nil {
		return err
	}
	parent, err = filepath.Abs(parent)
	if err != nil {
		return err
	}
	output := filepath.Join(parent, filepath.Base(*out))
	if rel, err := filepath.Rel(path, output); err != nil || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("output must be outside source DB")
	}
	f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	db, openErr := rawdb.NewPebbleDBReadOnly(path, 64, 128)
	var report rawdb.PostingCensus
	var scanErr, closeErr error
	if openErr == nil {
		lastProgress := time.Now()
		report, scanErr = rawdb.InspectStateChangePostings(ctx, db, cutoffs, func(phase string, rows, n uint64) {
			if time.Since(lastProgress) >= 30*time.Second {
				fmt.Fprintf(os.Stderr, "%s phase=%s rows=%d logical_bytes=%d\n", time.Now().UTC().Format(time.RFC3339), phase, rows, n)
				lastProgress = time.Now()
			}
		})
		closeErr = db.Close()
	}
	resultErr := errors.Join(openErr, scanErr, closeErr)
	if resultErr != nil {
		report.Complete, report.Error = false, resultErr.Error()
	}
	result := struct {
		Source string `json:"source"`
		Note   string `json:"note"`
		rawdb.PostingCensus
	}{path, "Read-only immutable-copy census. Eligibility is relative to caller cutoffs, not a canonical/cold proof. Directory retained. Logical KV bytes are not compressed physical reclaim; no data deleted.", report}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	writeErr := enc.Encode(result)
	syncErr := f.Sync()
	fileCloseErr := f.Close()
	return errors.Join(resultErr, writeErr, syncErr, fileCloseErr)
}
