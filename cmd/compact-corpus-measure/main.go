// compact-corpus-measure writes full-corpus baseline/candidate SSTs using the
// actual successor account and latest-value encoders, without opening a DB.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"google.golang.org/protobuf/proto"
)

func run() error {
	dir, out := flag.String("corpus-dir", "", "sealed normalized corpus"), flag.String("out-dir", "", "new exclusive SST/report directory")
	flag.Parse()
	if *dir == "" || *out == "" {
		return errors.New("corpus-dir and out-dir required")
	}
	meta, err := os.ReadFile(filepath.Join(*dir, "export.json"))
	if err != nil {
		return err
	}
	var source struct {
		Complete bool
		Block    uint64
		Rows     [2]uint64
		Bytes    int64
		SHA256   string
	}
	if err = json.Unmarshal(meta, &source); err != nil {
		return err
	}
	if !source.Complete {
		return errors.New("incomplete export")
	}
	f, err := os.Open(filepath.Join(*dir, "corpus.zst"))
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() != source.Bytes {
		return errors.New("corpus size mismatch")
	}
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != source.SHA256 {
		return errors.New("corpus SHA mismatch")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	dec, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return err
	}
	defer dec.Close()
	r := bufio.NewReaderSize(dec, 1<<20)
	var magic [8]byte
	if _, err = io.ReadFull(r, magic[:]); err != nil || string(magic[:]) != "GTCORP01" {
		return errors.New("bad corpus magic")
	}
	if err = os.Mkdir(*out, 0700); err != nil {
		return err
	}
	sstDir := filepath.Join(*out, "sst")
	if err = os.Mkdir(sstDir, 0700); err != nil {
		return err
	}
	series := map[string]*tableSeries{}
	for _, codec := range []string{"snappy", "none"} {
		c := sstable.SnappyCompression
		if codec == "none" {
			c = sstable.NoCompression
		}
		for _, kind := range []string{"account", "uint64"} {
			for _, model := range []string{"baseline", "compact"} {
				name := kind + "-" + model + "-" + codec
				series[name] = &tableSeries{dir: sstDir, name: name, compression: c}
			}
		}
	}
	prefixes := rawdb.StateCommitmentSourcePrefixes()
	var counts [2]uint64
	for {
		var header [9]byte
		_, err := io.ReadFull(r, header[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		keyLen, valueLen := binary.BigEndian.Uint32(header[1:]), binary.BigEndian.Uint32(header[5:])
		if keyLen > 4096 || valueLen > 1<<20 {
			return errors.New("corpus record exceeds bound")
		}
		key, value := make([]byte, keyLen), make([]byte, valueLen)
		if _, err = io.ReadFull(r, key); err != nil {
			return err
		}
		if _, err = io.ReadFull(r, value); err != nil {
			return err
		}
		kind := ""
		var physical, baseline, candidate []byte
		switch header[0] {
		case 1:
			if len(key) != 20 {
				return errors.New("account key width")
			}
			var id common.AccountID
			copy(id[:], key)
			owner := id.Address(common.AddressPrefixMainnet)
			env, err := state.DecodeStateAccountV3(value)
			if err != nil {
				return err
			}
			account, err := types.UnmarshalAccountStorageCoreV4(env.AccountProto, owner)
			if err != nil {
				return err
			}
			before := proto.Clone(account.Proto())
			env.AccountProto, err = account.MarshalStorageCoreV4(owner)
			if err != nil {
				return err
			}
			candidate, err = env.Encode()
			if err != nil {
				return err
			}
			decoded, err := state.DecodeStateAccountV3(candidate)
			if err != nil {
				return err
			}
			after, err := types.UnmarshalAccountStorageCoreV4(decoded.AccountProto, owner)
			if err != nil || !proto.Equal(before, after.Proto()) {
				return errors.New("account round trip changed logical data")
			}
			physical = append(bytes.Clone(prefixes[0]), key...)
			baseline = value
			kind = "account"
			counts[0]++
		case 2:
			if len(key) < 30 || len(value) != 8 {
				return errors.New("scalar key/value width")
			}
			physical = append(bytes.Clone(prefixes[2]), key...)
			baseline = append([]byte{1}, value...)
			candidate = rawdb.EncodeStateKVLatestValue(value)
			restored, err := rawdb.DecodeStateKVLatestValue(candidate)
			if err != nil || !bytes.Equal(restored, value) {
				return errors.New("scalar round trip differs")
			}
			kind = "uint64"
			counts[1]++
		default:
			return errors.New("unknown corpus family")
		}
		for _, codec := range []string{"snappy", "none"} {
			if err = series[kind+"-baseline-"+codec].set(physical, baseline); err != nil {
				return err
			}
			if err = series[kind+"-compact-"+codec].set(physical, candidate); err != nil {
				return err
			}
		}
	}
	if counts != source.Rows {
		return errors.New("corpus row counts differ")
	}
	for _, s := range series {
		if err = s.finish(); err != nil {
			return err
		}
	}
	report := struct {
		Complete    bool
		Source      json.RawMessage
		Rows        [2]uint64
		Experiments map[string]*tableSeries
	}{true, meta, counts, series}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*out, "results.json"), b, 0600)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
