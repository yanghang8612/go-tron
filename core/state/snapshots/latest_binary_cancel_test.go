package snapshots

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func TestLatestBinaryBuildChecksumAndCompanionsCancel(t *testing.T) {
	t.Run("segment-checksum", func(t *testing.T) {
		dir := t.TempDir()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reached := false
		ctx = maintenance.WithWorkCheckpoint(ctx, func(uint64) error { reached = true; cancel(); return ctx.Err() })
		ref := SegmentRef{Dataset: SegmentDatasetAccountLatest, Kind: SegmentLatest, FromTxNum: 1, ToTxNum: 1, Path: "latest/test.seg"}
		_, _, _, err := writeLatestBinarySegmentWithCompanionsContext(ctx, dir, ref, func(fn func(LatestEntry) error) error {
			return fn(LatestEntry{Key: AccountSnapshotKey(coldBuilderOwner(1)), Value: []byte("value")})
		}, true)
		if !reached || !errors.Is(err, context.Canceled) {
			t.Fatalf("checksum not cancelled reached=%v err=%v", reached, err)
		}
		files, _ := filepath.Glob(filepath.Join(dir, "latest", "*.seg"))
		if len(files) != 0 {
			t.Fatal("cancelled segment published", files)
		}
	})
	for _, kind := range []string{"accessor", "btree"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			ref, _, _, err := writeLatestBinarySegmentWithCompanionsContext(context.Background(), dir, SegmentRef{Dataset: SegmentDatasetAccountLatest, Kind: SegmentLatest, FromTxNum: 1, ToTxNum: 1, Path: "latest/cancel.seg"}, func(fn func(LatestEntry) error) error {
				return fn(LatestEntry{Key: AccountSnapshotKey(coldBuilderOwner(1)), Value: []byte("value")})
			}, false)
			if err != nil {
				t.Fatal(err)
			}
			checksum, err := latestBinaryChecksumBytes(ref.Checksum)
			if err != nil {
				t.Fatal(err)
			}
			offsets := filepath.Join(t.TempDir(), "offsets")
			if err := os.WriteFile(offsets, make([]byte, 8), 0600); err != nil {
				t.Fatal(err)
			}
			payload := filepath.Join(t.TempDir(), "payload")
			if err := os.WriteFile(payload, []byte{1}, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = maintenance.WithWorkCheckpoint(ctx, func(uint64) error { cancel(); return ctx.Err() })
			if kind == "accessor" {
				_, err = writeLatestBinaryAccessorFromOffsetsFileContext(ctx, dir, ref, checksum, offsets, 1)
			} else {
				_, err = writeLatestBinaryBTreeFromTempFilesContext(ctx, dir, ref, checksum, payload, offsets, 1)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("companion cancellation=%v", err)
			}
		})
	}
}
