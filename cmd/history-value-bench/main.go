// history-value-bench reads an immutable copied V6 history trio. It never opens
// chaindata, publishes a manifest, creates a temporary file, or starts a node.
// Its experimental codecs are measurement artifacts, not production formats.
package main

import (
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
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
)

const chunkSize = 128 << 10

type record struct {
	header, value []byte // Original V6 frame header, including its length prefix.
	key           uint32
	tx            uint64
}

type valueStats struct {
	Records, Present, EmptyPresent, ValueBytes, MaxValueBytes                uint64
	SameKeyPairs, SameKeyEqual, SameKeyEqualBytes, PrefixSuffixReusableBytes uint64
	NativeRecords                                                            uint64
	Sizes                                                                    map[string]uint64
}

type largest struct {
	Domain      string
	TxNum       uint64
	KeyID       uint32
	ValueBytes  int
	ValueSHA256 string
}

type codecResult struct {
	Name                                       string
	EncodedBytes, ZstdBytes, ContainerBytes    int
	RawRecords, DeltaRecords, ReferenceRecords uint64
	EncodeMS, DecodeMS, ZstdMS                 int64
	DecodedSHA256                              string
	ByteExact                                  bool
}

type report struct {
	FromTxNum, ToTxNum                   uint64
	Files                                []snapshots.SegmentRef
	HistoryPhysicalBytes, CompanionBytes uint64
	RawBytes, Records, Keys              int
	RawSHA256                            string
	InputUnchanged, ReaderMatchesRaw     bool
	CheckpointEvery                      int
	Domains                              map[string]*valueStats
	Largest                              []largest
	AccountEnvelopeVersions              map[string]uint64
	AccountEnvelopeFieldBytes            map[string]uint64
	LegacyAccountProtoFieldBytes         map[string]uint64
	Codecs                               []codecResult
	Notes                                []string
}

func main() {
	dir := flag.String("dir", "", "read-only scratch directory containing copied trio")
	from := flag.Uint64("from-txnum", 0, "inclusive first tx number")
	to := flag.Uint64("to-txnum", 0, "inclusive last tx number")
	path := flag.String("history", "", "relative history path; default history/state-domain-change-FROM-TO.seg")
	limit := flag.Uint64("max-raw-mib", 256, "reject a history logical stream larger than this; memory can be several times this")
	checkpoint := flag.Int("checkpoint", 32, "full value at least every N occurrences of each key")
	flag.Parse()
	if *dir == "" || *to < *from || *limit == 0 || *limit > 2048 || *checkpoint < 1 || *checkpoint > 1024 {
		fmt.Fprintln(os.Stderr, "invalid arguments; --dir and valid bounds are required")
		os.Exit(2)
	}
	r, err := run(*dir, *path, *from, *to, *limit<<20, *checkpoint)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	out := json.NewEncoder(os.Stdout)
	out.SetIndent("", "  ")
	if err := out.Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(dir, path string, from, to, limit uint64, checkpoint int) (*report, error) {
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok {
		return nil, errors.New("state-history domain unavailable")
	}
	if path == "" {
		path = cfg.HistoryPath(from, to)
	}
	if filepath.IsAbs(path) || !filepath.IsLocal(path) {
		return nil, errors.New("history must be a local relative path")
	}
	paths := []string{path, cfg.HistoryIndexPathFor(path), cfg.HistoryAccessorPathFor(path)}
	kinds := []snapshots.SegmentKind{snapshots.SegmentHistory, snapshots.SegmentInverted, snapshots.SegmentAccessor}
	r := &report{FromTxNum: from, ToTxNum: to, CheckpointEvery: checkpoint, Domains: map[string]*valueStats{}, AccountEnvelopeVersions: map[string]uint64{}, AccountEnvelopeFieldBytes: map[string]uint64{}, LegacyAccountProtoFieldBytes: map[string]uint64{}}
	for i, p := range paths {
		size, digest, err := fingerprint(filepath.Join(dir, p), limit*2)
		if err != nil {
			return nil, err
		}
		r.Files = append(r.Files, snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: kinds[i], FromTxNum: from, ToTxNum: to, Path: p, Size: size, Checksum: digest})
		if i == 0 {
			r.HistoryPhysicalBytes = size
		} else {
			r.CompanionBytes += size
		}
	}
	physical, err := os.ReadFile(filepath.Join(dir, path))
	if err != nil {
		return nil, err
	}
	raw, err := unpack(physical, limit)
	if err != nil {
		return nil, err
	}
	prefix, rows, err := parse(raw, from, to)
	if err != nil {
		return nil, err
	}
	r.RawBytes, r.Records, r.RawSHA256 = len(raw), len(rows), digest(raw)
	manifest := snapshots.NewManifest(from, to, r.Files)
	previous := make(map[uint32][]byte)
	i := 0
	err = cfg.IterateHistoryRange(dir, manifest, r.Files[0], from, to, func(c *rawdb.StateDomainChange) (bool, error) {
		if i >= len(rows) {
			return false, errors.New("public reader returned extra record")
		}
		row := rows[i]
		if row.tx != c.TxNum || row.header[16] != boolByte(c.PrevExists) || !bytes.Equal(row.value, c.Prev) {
			return false, fmt.Errorf("public reader/raw mismatch at record %d", i)
		}
		domain := c.FlatDomain.String()
		if c.FlatDomain == rawdb.StateFlatDomainKVLatest {
			domain += "/" + kvdomains.Name(c.Domain)
			if kvdomains.Name(c.Domain) == "" {
				domain += fmt.Sprint(uint16(c.Domain))
			}
		}
		s := r.Domains[domain]
		if s == nil {
			s = &valueStats{Sizes: map[string]uint64{}}
			r.Domains[domain] = s
		}
		s.Records++
		s.ValueBytes += uint64(len(c.Prev))
		s.MaxValueBytes = max(s.MaxValueBytes, uint64(len(c.Prev)))
		if c.PrevExists {
			s.Present++
			if len(c.Prev) == 0 {
				s.EmptyPresent++
			}
		}
		if statecodec.IsNative(c.Prev) {
			s.NativeRecords++
		}
		s.Sizes[sizeBucket(len(c.Prev))]++
		if old, exists := previous[row.key]; exists {
			s.SameKeyPairs++
			p, q := commonEnds(old, row.value)
			s.PrefixSuffixReusableBytes += uint64(p + q)
			if bytes.Equal(old, row.value) {
				s.SameKeyEqual++
				s.SameKeyEqualBytes += uint64(len(row.value))
			}
		}
		previous[row.key] = row.value
		if c.FlatDomain == rawdb.StateFlatDomainAccountLatest && c.PrevExists {
			accountStats(r, c.Prev)
		}
		r.Largest = append(r.Largest, largest{domain, c.TxNum, row.key, len(c.Prev), digest(c.Prev)})
		i++
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if i != len(rows) {
		return nil, fmt.Errorf("public reader returned %d records, expected %d", i, len(rows))
	}
	r.ReaderMatchesRaw, r.Keys = true, len(previous)
	sort.Slice(r.Largest, func(i, j int) bool { return r.Largest[i].ValueBytes > r.Largest[j].ValueBytes })
	if len(r.Largest) > 20 {
		r.Largest = r.Largest[:20]
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(limit*2))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	for _, name := range []string{"current-v6", "same-key-prefix-suffix", "same-key-copy-literal", "segment-exact-dedup"} {
		fmt.Fprintln(os.Stderr, "benchmark", name)
		result := codecResult{Name: name}
		start := time.Now()
		encoded := raw
		if name != "current-v6" {
			encoded = encode(prefix, rows, name, checkpoint, &result)
		}
		result.EncodeMS = time.Since(start).Milliseconds()
		result.EncodedBytes = len(encoded)
		start = time.Now()
		packed := pack(encoded, enc)
		result.ZstdMS = time.Since(start).Milliseconds()
		result.ContainerBytes = len(packed)
		result.ZstdBytes = len(packed) - 48 - 28*((len(encoded)+chunkSize-1)/chunkSize)
		start = time.Now()
		restored, err := unpackWith(packed, uint64(max(len(encoded), len(raw)))+1, dec)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(restored, encoded) {
			return nil, errors.New("compression round-trip mismatch")
		}
		if name != "current-v6" {
			restored, err = decode(restored, len(raw))
			if err != nil {
				return nil, err
			}
		}
		result.DecodeMS = time.Since(start).Milliseconds()
		result.ByteExact = bytes.Equal(restored, raw)
		result.DecodedSHA256 = digest(restored)
		if !result.ByteExact {
			return nil, fmt.Errorf("%s did not restore original bytes", name)
		}
		r.Codecs = append(r.Codecs, result)
	}
	for _, ref := range r.Files {
		size, sum, err := fingerprint(filepath.Join(dir, ref.Path), limit*2)
		if err != nil {
			return nil, err
		}
		if size != ref.Size || sum != ref.Checksum {
			return nil, fmt.Errorf("input changed: %s", ref.Path)
		}
	}
	r.InputUnchanged = true
	r.Notes = []string{
		"All codecs restore the complete original V6 logical stream byte-for-byte, including tx-range table, key IDs, tx numbers, existence flags and record order.",
		"ContainerBytes measures a fully encoded experimental history payload with 48-byte header, 28-byte page table entries and actual zstd SpeedDefault 128-KiB pages; current-v6 is the same recompression baseline.",
		"Existing companion byte counts are reported separately. Their offsets must be rebuilt for a deployed candidate; simply replacing .seg is invalid. No production-compatible index or point-query benchmark is claimed.",
		"Delta anchors occur at most every checkpoint occurrences per key; a real query needs an anchor locator and at most checkpoint-1 predecessor versions. Exact dedup references earlier full values and needs a value locator.",
		"Same-key statistics cover only this trio. Values before or after it and cross-trio opportunities are not inferred. Absent and empty-present values remain distinct.",
		"Account envelopes are inspected generically so legacy production payloads are counted rather than rejected by the fresh-genesis-only runtime decoder.",
	}
	return r, nil
}

func fingerprint(path string, limit uint64) (uint64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, "", err
	}
	if !st.Mode().IsRegular() || st.Size() < 0 || uint64(st.Size()) > limit {
		return 0, "", fmt.Errorf("input size/type rejected: %s", path)
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, int64(limit)+1))
	if err != nil || n != st.Size() {
		return 0, "", fmt.Errorf("input read failed or changed: %s: %v", path, err)
	}
	return uint64(n), "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func parse(raw []byte, from, to uint64) ([]byte, []record, error) {
	if len(raw) < 76 || string(raw[:8]) != "gtsdcseg" || binary.BigEndian.Uint32(raw[8:12]) != 6 {
		return nil, nil, errors.New("only V6 history record streams are supported")
	}
	if binary.BigEndian.Uint64(raw[12:20]) != from || binary.BigEndian.Uint64(raw[20:28]) != to {
		return nil, nil, errors.New("history bounds mismatch")
	}
	n, txranges := binary.BigEndian.Uint64(raw[28:36]), binary.BigEndian.Uint64(raw[68:76])
	if txranges > uint64((len(raw)-76)/56) || n > uint64(len(raw)/21) {
		return nil, nil, errors.New("invalid history counts")
	}
	off := 76 + int(txranges)*56
	prefix := raw[:off]
	rows := make([]record, 0, int(n))
	for range n {
		if len(raw)-off < 21 {
			return nil, nil, io.ErrUnexpectedEOF
		}
		h := raw[off : off+21]
		size := uint64(binary.BigEndian.Uint32(h[17:21]))
		if size > uint64(len(raw)-off-21) || uint64(binary.BigEndian.Uint32(h[:4])) != 17+size || h[16] > 1 {
			return nil, nil, errors.New("invalid V6 frame")
		}
		rows = append(rows, record{h, raw[off+21 : off+21+int(size)], binary.BigEndian.Uint32(h[4:8]), binary.BigEndian.Uint64(h[8:16])})
		off += 21 + int(size)
	}
	if off != len(raw) {
		return nil, nil, errors.New("trailing history bytes")
	}
	return prefix, rows, nil
}

func unpack(data []byte, limit uint64) ([]byte, error) {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(limit*2))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	return unpackWith(data, limit, dec)
}

func unpackWith(data []byte, limit uint64, dec *zstd.Decoder) ([]byte, error) {
	if len(data) >= 8 && string(data[:8]) == "gtsdcseg" {
		if uint64(len(data)) > limit {
			return nil, errors.New("raw limit exceeded")
		}
		return data, nil
	}
	if len(data) < 48 || string(data[:8]) != "gtcblk01" || binary.BigEndian.Uint32(data[8:12]) != 1 {
		return nil, errors.New("invalid compressed container")
	}
	count, total, start := binary.BigEndian.Uint64(data[24:32]), binary.BigEndian.Uint64(data[32:40]), binary.BigEndian.Uint64(data[40:48])
	if total > limit || count > uint64((len(data)-48)/28) || start != 48+count*28 || start > uint64(len(data)) {
		return nil, errors.New("invalid compressed bounds or raw limit exceeded")
	}
	out := make([]byte, 0, int(total))
	var consumed uint64
	for i := uint64(0); i < count; i++ {
		e := data[48+i*28 : 48+(i+1)*28]
		u, c, n := binary.BigEndian.Uint64(e[:8]), binary.BigEndian.Uint64(e[8:16]), binary.BigEndian.Uint64(e[16:24])
		if u != uint64(len(out)) || c != consumed || n > uint64(len(data))-start-consumed {
			return nil, errors.New("invalid compressed page")
		}
		page, err := dec.DecodeAll(data[start+c:start+c+n], nil)
		if err != nil {
			return nil, err
		}
		if uint64(len(page)) > total-uint64(len(out)) {
			return nil, errors.New("decoded size exceeded")
		}
		out = append(out, page...)
		consumed += n
	}
	if uint64(len(out)) != total || start+consumed != uint64(len(data)) {
		return nil, errors.New("compressed size mismatch or trailing data")
	}
	return out, nil
}

func pack(raw []byte, enc *zstd.Encoder) []byte {
	count := (len(raw) + chunkSize - 1) / chunkSize
	start := 48 + count*28
	out := make([]byte, start)
	copy(out, "gtcblk01")
	binary.BigEndian.PutUint32(out[8:12], 1)
	binary.BigEndian.PutUint32(out[12:16], chunkSize)
	binary.BigEndian.PutUint64(out[24:32], uint64(count))
	binary.BigEndian.PutUint64(out[32:40], uint64(len(raw)))
	binary.BigEndian.PutUint64(out[40:48], uint64(start))
	for i := 0; i < count; i++ {
		u := i * chunkSize
		zipped := enc.EncodeAll(raw[u:min(u+chunkSize, len(raw))], nil)
		e := out[48+i*28 : 48+(i+1)*28]
		binary.BigEndian.PutUint64(e[:8], uint64(u))
		binary.BigEndian.PutUint64(e[8:16], uint64(len(out)-start))
		binary.BigEndian.PutUint64(e[16:24], uint64(len(zipped)))
		out = append(out, zipped...)
	}
	return out
}

func commonEnds(a, b []byte) (int, int) {
	p := 0
	for p < min(len(a), len(b)) && a[p] == b[p] {
		p++
	}
	q := 0
	for q < min(len(a), len(b))-p && a[len(a)-1-q] == b[len(b)-1-q] {
		q++
	}
	return p, q
}

func digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
func sizeBucket(n int) string {
	for _, v := range []int{0, 32, 128, 512, 2048, 8192, 32768, 131072, 1048576} {
		if n <= v {
			return fmt.Sprintf("<=%d", v)
		}
	}
	return ">1048576"
}

func accountStats(r *report, value []byte) {
	var fields []rlp.RawValue
	if rlp.DecodeBytes(value, &fields) != nil || len(fields) != 5 {
		r.AccountEnvelopeVersions["unrecognized"]++
		return
	}
	var version uint64
	if rlp.DecodeBytes(fields[0], &version) != nil {
		r.AccountEnvelopeVersions["unrecognized"]++
		return
	}
	r.AccountEnvelopeVersions[fmt.Sprint(version)]++
	for i, name := range []string{"version", "core", "kv-root", "generation", "code-hash"} {
		r.AccountEnvelopeFieldBytes[name] += uint64(len(fields[i]))
	}
	var core []byte
	if rlp.DecodeBytes(fields[1], &core) != nil || version >= 4 {
		return
	}
	counts := map[string]uint64{}
	original := core
	descriptor := (&corepb.Account{}).ProtoReflect().Descriptor().Fields()
	for len(core) > 0 {
		num, typ, n := protowire.ConsumeTag(core)
		if n < 0 {
			return
		}
		m := protowire.ConsumeFieldValue(num, typ, core[n:])
		if m < 0 {
			return
		}
		name := fmt.Sprint(num)
		if field := descriptor.ByNumber(num); field != nil {
			name = string(field.Name())
		}
		counts[name] += uint64(n + m)
		core = core[n+m:]
	}
	if len(original) > 0 {
		for name, n := range counts {
			r.LegacyAccountProtoFieldBytes[name] += n
		}
	}
}
