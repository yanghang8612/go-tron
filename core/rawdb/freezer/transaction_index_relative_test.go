package freezer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestRelativeTransactionIndexWidthsAndFallback(t *testing.T) {
	for _, plain := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "relative.gtxi")
		start := uint64(90_000_000)
		var hashes [][32]byte
		var locations []uint64
		res, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{StartBlock: start, EndBlock: start + 8192, PrefixBits: 8, Iterate: func(yield func(TransactionIndexEntry) error) error {
			for i := uint64(0); i < 100; i++ {
				h := syntheticTransactionIndexHash(7, i, i, 8)
				loc := transactionLocationMarker | ((start + i*80) << 16) | 65535
				if plain && i == 5 {
					loc = start + 400
				}
				hashes = append(hashes, h)
				locations = append(locations, loc)
				if err := yield(TransactionIndexEntry{h, loc}); err != nil {
					return err
				}
			}
			return nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		r, err := OpenTransactionIndexRun(path)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		_, _, width, err := r.readEncodedBucket(7)
		if err != nil {
			t.Fatal(err)
		}
		wantWidth := 4
		if plain {
			wantWidth = 8
		}
		if width != wantWidth {
			t.Fatal(width, wantWidth)
		}
		if err = r.Verify(); err != nil {
			t.Fatal(err)
		}
		for i, h := range hashes {
			got, err := r.Candidates(h)
			if err != nil || len(got) != 1 || got[0] != locations[i] {
				t.Fatal(got, err)
			}
		}
		if !plain && res.FileBytes != 128+257*8+16+100*(8+4) {
			t.Fatal("unexpected physical size", res.FileBytes)
		}
	}
}

// Reuses immutable V1 runs as the actual baseline. Every fingerprint/location
// is compared after writing V2; reconstructed route hashes are not full tx IDs.
func TestRelativeTransactionIndexPinnedCorpus(t *testing.T) {
	dir, outDir := os.Getenv("GTRON_TX_INDEX_CORPUS"), os.Getenv("GTRON_INDEX_MEASURE_OUT")
	if dir == "" || outDir == "" {
		t.Skip("explicit immutable corpus/new output directory required")
	}
	if err := os.Mkdir(outDir, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest transactionIndexManifest
	if err = json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	type result struct {
		File, SHA256             string
		Rows, Before, After      uint64
		BaselineQueryNS, QueryNS int64
	}
	var results []result
	ctx := context.Background()
	for _, ref := range manifest.Runs {
		path := filepath.Join(dir, "runs", ref.File)
		source, err := OpenTransactionIndexRun(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.New()
		_, err = io.Copy(hash, f)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		var queries [][32]byte
		res, err := BuildTransactionIndexRun(filepath.Join(outDir, ref.File), TransactionIndexBuildOptions{Context: ctx, StartBlock: source.startBlock, EndBlock: source.endBlock, PrefixBits: source.prefixBits, Iterate: func(yield func(TransactionIndexEntry) error) error {
			var tie uint64
			for prefix := uint32(0); uint64(prefix) < uint64(1)<<source.prefixBits; prefix++ {
				fps, locs, err := source.readBucket(prefix)
				if err != nil {
					return err
				}
				for i := 0; i < len(fps)/8; i++ {
					h := syntheticTransactionIndexHash(prefix, binary.BigEndian.Uint64(fps[i*8:]), tie, source.prefixBits)
					tie++
					if len(queries) < 128 && (i == 0 || i == len(fps)/8-1) {
						queries = append(queries, h)
					}
					if err := yield(TransactionIndexEntry{h, binary.BigEndian.Uint64(locs[i*8:])}); err != nil {
						return err
					}
				}
			}
			return nil
		}})
		if err != nil {
			t.Fatal(ref.File, err)
		}
		candidate, err := OpenTransactionIndexRun(res.Path)
		if err != nil {
			t.Fatal(err)
		}
		for prefix := uint32(0); uint64(prefix) < uint64(1)<<source.prefixBits; prefix++ {
			a, b, e := source.readBucket(prefix)
			c, d, f := candidate.readBucket(prefix)
			if e != nil || f != nil || !bytes.Equal(a, c) || !bytes.Equal(b, d) {
				t.Fatal("bucket differs", ref.File, prefix, e, f)
			}
		}
		var baselineNS, candidateNS int64
		for repeat := 0; repeat < 4; repeat++ {
			for _, h := range queries {
				start := time.Now()
				a, e := source.Candidates(h)
				baselineNS += time.Since(start).Nanoseconds()
				start = time.Now()
				b, f := candidate.Candidates(h)
				candidateNS += time.Since(start).Nanoseconds()
				if e != nil || f != nil || !reflect.DeepEqual(a, b) {
					t.Fatal("candidates differ", e, f)
				}
			}
		}
		denom := int64(max(1, len(queries)*4))
		results = append(results, result{ref.File, hex.EncodeToString(hash.Sum(nil)), source.rows, source.fileBytes, res.FileBytes, baselineNS / denom, candidateNS / denom})
		candidate.Close()
		source.Close()
		if len(results)%128 == 0 {
			t.Log("runs", len(results))
		}
	}
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(outDir, "results.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}
