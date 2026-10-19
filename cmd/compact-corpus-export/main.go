// compact-corpus-export must be built with the format matching the immutable
// source database. It authenticates all three source families before sealing
// a normalized account/scalar corpus for successor-codec experiments.
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func framed(h hash.Hash, k, v []byte) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(len(k)))
	h.Write(b[:])
	h.Write(k)
	binary.BigEndian.PutUint64(b[:], uint64(len(v)))
	h.Write(b[:])
	h.Write(v)
}
func run() error {
	dir, baseline, out := flag.String("db-dir", "", "immutable offline database"), flag.String("baseline", "", "authenticated census"), flag.String("out-dir", "", "new exclusive output directory")
	flag.Parse()
	if *dir == "" || *baseline == "" || *out == "" {
		return errors.New("db-dir, baseline and out-dir required")
	}
	input, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	dest, err := filepath.Abs(*out)
	if err != nil {
		return err
	}
	input, err = filepath.EvalSymlinks(input)
	if err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(dest))
	if err != nil {
		return err
	}
	dest = filepath.Join(parent, filepath.Base(dest))
	rel, err := filepath.Rel(input, dest)
	if err != nil || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("output must be outside source")
	}
	var prior rawdb.GenerationCensus
	b, err := os.ReadFile(*baseline)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &prior); err != nil {
		return err
	}
	if !prior.Complete || !prior.MuHashVerified {
		return errors.New("unauthenticated baseline")
	}
	db, err := rawdb.NewPebbleDBReadOnly(input, 64, 128)
	if err != nil {
		return err
	}
	defer db.Close()
	head, ok, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil || !ok || head.BlockNum != prior.BlockNum || head.BlockHash != prior.BlockHash || head.Root != prior.Root || head.TxNum != prior.TxNum {
		return errors.Join(err, errors.New("HEAD mismatch"))
	}
	if err = os.Mkdir(dest, 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dest, "corpus.zst"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	fileHash := sha256.New()
	enc, err := zstd.NewWriter(io.MultiWriter(f, fileHash), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true))
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(enc, 4<<20)
	if _, err = w.WriteString("GTCORP01"); err != nil {
		return err
	}
	hashes := [3]hash.Hash{sha256.New(), sha256.New(), sha256.New()}
	var sourceRows [3]uint64
	var counts [2]uint64
	write := func(family byte, k, v []byte) error {
		var h [9]byte
		h[0] = family
		binary.BigEndian.PutUint32(h[1:], uint32(len(k)))
		binary.BigEndian.PutUint32(h[5:], uint32(len(v)))
		if _, err := w.Write(h[:]); err != nil {
			return err
		}
		if _, err := w.Write(k); err != nil {
			return err
		}
		_, err := w.Write(v)
		return err
	}
	for family, prefix := range rawdb.StateCommitmentSourcePrefixes() {
		it := db.NewIterator(prefix, nil)
		for it.Next() {
			k, v := it.Key(), it.Value()
			if family == 2 && len(k) == 32 {
				continue
			}
			framed(hashes[family], k, v)
			sourceRows[family]++
			if family == 0 {
				err = write(1, k[len(prefix):], v)
				counts[0]++
			} else if family == 2 {
				_, _, domain, _, ok := rawdb.DecodeStateKVLatestKey(k)
				if !ok {
					err = errors.New("invalid KV key")
					break
				}
				if domain >= kvdomains.AccountAsset && domain <= kvdomains.AccountAssetOperationTimeV2 {
					var value []byte
					value, err = rawdb.DecodeStateKVLatestValue(v)
					if err == nil && len(value) != 8 {
						err = errors.New("invalid scalar width")
					}
					if err == nil {
						err = write(2, k[len(prefix):], value)
						counts[1]++
					}
				}
			}
			if err != nil {
				break
			}
		}
		err = errors.Join(err, it.Error())
		it.Release()
		if err != nil {
			return err
		}
	}
	if sourceRows != prior.SourceRows {
		return errors.New("source count mismatch")
	}
	for i, h := range hashes {
		if hex.EncodeToString(h.Sum(nil)) != prior.SourceSHA256[i] {
			return errors.New("source digest mismatch")
		}
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = enc.Close(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		return err
	}
	report := struct {
		Complete     bool
		Block        uint64
		SourceRows   [3]uint64
		SourceSHA256 [3]string
		Rows         [2]uint64
		Bytes        int64
		SHA256       string
	}{true, head.BlockNum, sourceRows, prior.SourceSHA256, counts, st.Size(), hex.EncodeToString(fileHash.Sum(nil))}
	b, err = json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dest, "export.json"), b, 0600)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
