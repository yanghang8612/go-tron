package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type codeReport struct {
	Complete    bool
	Block       uint64
	Started     time.Time
	Finished    time.Time
	Rows        uint64
	KeyBytes    uint64
	CodeBytes   uint64
	MaxCode     uint64
	SourceSHA   string
	Experiments map[string]*tableSeries
	Limitation  string
}

func inspectCode(db rawdbKeyValue, prior rawdb.GenerationCensus, out string) error {
	head, ok, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil || !ok || head.BlockNum != prior.BlockNum || head.BlockHash != prior.BlockHash || head.Root != prior.Root || head.TxNum != prior.TxNum {
		return errors.Join(err, errors.New("code census HEAD differs from baseline"))
	}
	var prefix []byte
	for _, r := range rawdb.DiskSpaceObservationRanges() {
		if r.Name == "state_code" {
			prefix = r.Start
		}
	}
	if len(prefix) == 0 {
		return errors.New("missing code schema range")
	}
	dir := filepath.Join(out, "sst")
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	r := codeReport{Block: head.BlockNum, Started: time.Now().UTC(), Experiments: map[string]*tableSeries{}, Limitation: "all content-addressed code rows, including possibly unreferenced hashes; every code hash checked, but reachability/deletion not assessed; isolated SSTs are not live LSM sizes"}
	for _, compression := range []string{"snappy", "none"} {
		c := sstable.SnappyCompression
		if compression == "none" {
			c = sstable.NoCompression
		}
		for _, model := range []string{"code-baseline", "code-zstd"} {
			name := model + "-" + compression
			r.Experiments[name] = &tableSeries{dir: dir, name: name, compression: c}
		}
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(true))
	if err != nil {
		return err
	}
	defer enc.Close()
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return err
	}
	defer dec.Close()
	h := sha256.New()
	err = rawdb.IterateStateCode(db, func(row rawdb.StateCodeRow) (bool, error) {
		if common.Keccak256(row.Code) != row.Hash {
			return false, errors.New("bytecode content hash mismatch")
		}
		key := append(bytes.Clone(prefix), row.Hash[:]...)
		framed(h, key, row.Code)
		r.Rows++
		r.KeyBytes += uint64(len(key))
		r.CodeBytes += uint64(len(row.Code))
		r.MaxCode = max(r.MaxCode, uint64(len(row.Code)))
		compressed := enc.EncodeAll(row.Code, nil)
		restored, err := dec.DecodeAll(compressed, nil)
		if err != nil || !bytes.Equal(restored, row.Code) {
			return false, errors.Join(err, errors.New("bytecode Zstd round trip differs"))
		}
		candidate := append([]byte{1}, row.Code...)
		if len(compressed) < len(row.Code) {
			candidate = append([]byte{4}, compressed...)
		}
		for _, suffix := range []string{"snappy", "none"} {
			if err = r.Experiments["code-baseline-"+suffix].set(key, row.Code); err != nil {
				return false, err
			}
			if err = r.Experiments["code-zstd-"+suffix].set(key, candidate); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	for _, series := range r.Experiments {
		err = errors.Join(err, series.finish())
	}
	if err != nil {
		return err
	}
	r.Complete = true
	r.SourceSHA = hex.EncodeToString(h.Sum(nil))
	r.Finished = time.Now().UTC()
	f, err := os.OpenFile(filepath.Join(out, "code-distribution.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	e := json.NewEncoder(f)
	e.SetIndent("", "  ")
	return errors.Join(e.Encode(r), f.Sync(), f.Close())
}
