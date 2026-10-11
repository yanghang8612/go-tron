// Bounded, read-only V6/V7 accessor dictionary sampling and zstd diagnostics.
// Build or run ./cmd/history-dictionary-benchmark using the pinned zstd module.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
)

const storedRaw = uint32(1 << 31)
const maxDecodedBlock = uint64(128 * (math.MaxUint16 + 2*binary.MaxVarintLen64 + 2*binary.MaxVarintLen32))

type header struct {
	Version               uint32 `json:"version"`
	FileBytes             uint64 `json:"file_bytes"`
	HeaderBytes           uint64 `json:"header_bytes"`
	FromTx, ToTx          uint64
	Records, Keys, Blocks uint64
	DirectoryBytes        uint64 `json:"directory_bytes"`
	KeyDataBytes          uint64 `json:"key_data_bytes"`
	PostingBytes          uint64 `json:"posting_bytes"`
}
type sample struct {
	Index       uint64 `json:"index"`
	StoredBytes uint32 `json:"stored_bytes"`
	StoredRaw   bool   `json:"stored_raw"`
	KeyCount    uint32 `json:"key_count"`
	Raw         []byte `json:"raw"`
}
type captureFile struct {
	Path                 string `json:"path"`
	Header               header `json:"header"`
	ReadCalls, ReadBytes uint64
	Samples              []sample `json:"samples"`
}
type capture struct {
	Format string        `json:"format"`
	Files  []captureFile `json:"files"`
}
type limitedReader struct {
	r            io.ReaderAt
	remaining    *uint64
	calls, bytes uint64
}

func (r *limitedReader) read(off, n uint64) ([]byte, error) {
	if n > *r.remaining || off > math.MaxInt64 || n > uint64(math.MaxInt) {
		return nil, errors.New("sampling read budget exceeded")
	}
	out := make([]byte, int(n))
	read, err := r.r.ReadAt(out, int64(off))
	r.calls++
	r.bytes += uint64(read)
	*r.remaining -= uint64(read)
	if err != nil {
		return nil, err
	}
	if read != len(out) {
		return nil, io.ErrUnexpectedEOF
	}
	return out, nil
}
func readHeader(r *limitedReader, size uint64) (header, error) {
	var h header
	first, err := r.read(0, 12)
	if err != nil {
		return h, err
	}
	if string(first[:8]) != "gtsdckv1" {
		return h, errors.New("not a key-oriented accessor")
	}
	h.Version = binary.BigEndian.Uint32(first[8:12])
	h.FileBytes = size
	switch h.Version {
	case 6:
		h.HeaderBytes = 112
	case 7:
		h.HeaderBytes = 120
	default:
		return h, fmt.Errorf("unsupported accessor version %d", h.Version)
	}
	raw, err := r.read(0, h.HeaderBytes)
	if err != nil {
		return h, err
	}
	crcOffset := 104
	if h.Version == 7 {
		crcOffset = 112
	}
	if binary.BigEndian.Uint32(raw[crcOffset:]) != crc32.ChecksumIEEE(raw[:crcOffset]) || !bytes.Equal(raw[crcOffset+4:], []byte{0, 0, 0, 0}) || binary.BigEndian.Uint32(raw[36:40]) != 128 {
		return h, errors.New("invalid accessor header checksum/shape")
	}
	h.FromTx = binary.BigEndian.Uint64(raw[12:20])
	h.ToTx = binary.BigEndian.Uint64(raw[20:28])
	h.Records = binary.BigEndian.Uint64(raw[28:36])
	h.Keys = binary.BigEndian.Uint64(raw[40:48])
	h.Blocks = binary.BigEndian.Uint64(raw[48:56])
	h.DirectoryBytes = binary.BigEndian.Uint64(raw[56:64])
	h.KeyDataBytes = binary.BigEndian.Uint64(raw[64:72])
	h.PostingBytes = h.Records * 18
	if h.Version == 7 {
		h.PostingBytes = binary.BigEndian.Uint64(raw[104:112])
	}
	if h.Records > math.MaxUint32 || h.Keys > math.MaxUint32 || h.Blocks != (h.Keys+127)/128 || h.DirectoryBytes < h.Blocks*8 {
		return h, errors.New("invalid accessor counts")
	}
	remaining := size
	for _, n := range []uint64{h.HeaderBytes, h.DirectoryBytes, h.KeyDataBytes, h.PostingBytes} {
		if n > remaining {
			return h, errors.New("accessor section outside file")
		}
		remaining -= n
	}
	if remaining != 0 {
		return h, errors.New("accessor section lengths do not cover file")
	}
	return h, nil
}
func sampleIndices(count uint64, n int) []uint64 {
	if uint64(n) > count {
		n = int(count)
	}
	out := make([]uint64, n)
	if n == 1 {
		out[0] = count / 2
		return out
	}
	for i := range out {
		out[i] = uint64(i) * (count - 1) / uint64(n-1)
	}
	return out
}
func readSample(r *limitedReader, h header, index uint64, dec *zstd.Decoder, maxRawRemaining *uint64) (sample, error) {
	var s sample
	s.Index = index
	length := uint64(8)
	if index+1 < h.Blocks {
		length = 16
	}
	offsets, err := r.read(h.HeaderBytes+index*8, length)
	if err != nil {
		return s, err
	}
	off := binary.BigEndian.Uint64(offsets[:8])
	next := h.HeaderBytes + h.DirectoryBytes
	if len(offsets) == 16 {
		next = binary.BigEndian.Uint64(offsets[8:16])
	}
	if off < h.HeaderBytes+h.Blocks*8 || off >= next || next > h.HeaderBytes+h.DirectoryBytes || next-off < 30 || next-off > math.MaxUint16+30 {
		return s, errors.New("invalid directory entry offsets")
	}
	entry, err := r.read(off, next-off)
	if err != nil {
		return s, err
	}
	keyLen := uint64(binary.BigEndian.Uint16(entry[:2]))
	if keyLen == 0 || keyLen+30 != uint64(len(entry)) {
		return s, errors.New("invalid directory first key length")
	}
	key, tail := entry[2:2+keyLen], entry[2+keyLen:]
	crc := crc32.NewIEEE()
	crc.Write(key)
	crc.Write(tail[:24])
	if crc.Sum32() != binary.BigEndian.Uint32(tail[24:28]) {
		return s, errors.New("directory checksum mismatch")
	}
	dataOff := binary.BigEndian.Uint64(tail[:8])
	dataLen := binary.BigEndian.Uint32(tail[8:12])
	rawLen := uint64(binary.BigEndian.Uint32(tail[12:16]))
	rawCRC := binary.BigEndian.Uint32(tail[16:20])
	s.KeyCount = binary.BigEndian.Uint32(tail[20:24])
	s.StoredRaw = dataLen&storedRaw != 0
	s.StoredBytes = dataLen &^ storedRaw
	keyStart := h.HeaderBytes + h.DirectoryBytes
	keyEnd := keyStart + h.KeyDataBytes
	if s.KeyCount == 0 || s.KeyCount > 128 || dataOff < keyStart || dataOff > keyEnd || uint64(s.StoredBytes) > keyEnd-dataOff || rawLen > h.KeyDataBytes || rawLen > uint64(s.KeyCount)*(math.MaxUint16+2*binary.MaxVarintLen64+2*binary.MaxVarintLen32) || rawLen > *maxRawRemaining {
		return s, errors.New("key block exceeds reader or sample bounds")
	}
	stored, err := r.read(dataOff, uint64(s.StoredBytes))
	if err != nil {
		return s, err
	}
	s.Raw = stored
	if !s.StoredRaw {
		s.Raw, err = dec.DecodeAll(stored, make([]byte, 0, int(rawLen)))
		if err != nil {
			return s, err
		}
	}
	if uint64(len(s.Raw)) != rawLen || crc32.ChecksumIEEE(s.Raw) != rawCRC {
		return s, errors.New("key block decoded checksum mismatch")
	}
	*maxRawRemaining -= rawLen
	return s, nil
}
func captureSources(paths []string, n int, maxRead, maxRaw uint64) (capture, error) {
	out := capture{Format: "gtron-dictionary-zstd-sample-v1"}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderMaxMemory(16<<20))
	if err != nil {
		return out, err
	}
	defer dec.Close()
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return out, err
		}
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return out, err
		}
		r := &limitedReader{r: file, remaining: &maxRead}
		h, err := readHeader(r, uint64(info.Size()))
		if err != nil {
			file.Close()
			return out, fmt.Errorf("%s: %w", path, err)
		}
		cf := captureFile{Path: path, Header: h}
		for _, index := range sampleIndices(h.Blocks, n) {
			s, err := readSample(r, h, index, dec, &maxRaw)
			if err != nil {
				file.Close()
				return out, fmt.Errorf("%s block %d: %w", path, index, err)
			}
			cf.Samples = append(cf.Samples, s)
		}
		cf.ReadCalls, cf.ReadBytes = r.calls, r.bytes
		if err := file.Close(); err != nil {
			return out, err
		}
		out.Files = append(out.Files, cf)
	}
	return out, nil
}
func cpuTime() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	return time.Duration(usage.Utime.Sec+usage.Stime.Sec)*time.Second + time.Duration(usage.Utime.Usec+usage.Stime.Usec)*time.Microsecond
}

type timing struct {
	CPUSeconds      float64 `json:"cpu_seconds"`
	WallSeconds     float64 `json:"wall_seconds"`
	MiBPerCPUSecond float64 `json:"mib_per_cpu_second"`
	CPUUsPerBlock   float64 `json:"cpu_us_per_block"`
}

func measure(rawBytes, operations uint64, fn func() error) (timing, error) {
	cpu, start := cpuTime(), time.Now()
	if err := fn(); err != nil {
		return timing{}, err
	}
	elapsed := cpuTime() - cpu
	out := timing{CPUSeconds: elapsed.Seconds(), WallSeconds: time.Since(start).Seconds()}
	if elapsed > 0 {
		out.MiBPerCPUSecond = float64(rawBytes) / (1 << 20) / elapsed.Seconds()
		out.CPUUsPerBlock = float64(elapsed.Microseconds()) / float64(operations)
	}
	return out, nil
}

type levelResult struct {
	KeepLargestSampleBlockRawBytes               uint64   `json:"keep_largest_sample_block_raw_bytes"`
	KeepLargestSampleBlockRawRatio               float64  `json:"keep_largest_sample_block_raw_ratio"`
	FullySampledUnsafeCompressedFiles            []string `json:"fully_sampled_unsafe_compressed_files"`
	Level                                        string   `json:"level"`
	RawBytes, ZstdBytes, KeepOnlyBeneficialBytes uint64
	ZstdRatio                                    float64 `json:"zstd_ratio"`
	KeepBeneficialRatio                          float64 `json:"keep_beneficial_ratio"`
	Encode, Decode                               timing
}
type report struct {
	GoVersion, GOOS, GOARCH string
	Repeat                  int
	Files                   []captureFile `json:"files"`
	Results                 []levelResult `json:"results"`
	Warnings                []string      `json:"warnings"`
}

func benchmark(c capture, n int, levelNames string) (report, error) {
	out := report{GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Repeat: n, Files: c.Files, Warnings: []string{"Samples are stratified dictionary blocks, not a whole-store compression estimate.", "CPU includes process user+system time; timed loops exclude file I/O, CRC and key parsing. Encode reuses destination capacity; decode allocates its destination like the reader.", "Existing reader rejects any rawLen > total stored keyDataLen. Unconditional compression is unsafe for one-block/small files.", "Keeping each file's largest decoded block raw guarantees that reader length bound; the largest block must be identified during the existing complete writer pass, not from this sample."}}
	var samples []sample
	var rawTotal uint64
	for _, file := range c.Files {
		for _, s := range file.Samples {
			if len(s.Raw) == 0 || uint64(len(s.Raw)) > maxDecodedBlock {
				return out, errors.New("invalid sample raw block")
			}
			samples = append(samples, s)
			rawTotal += uint64(len(s.Raw))
		}
	}
	if len(samples) == 0 {
		return out, errors.New("no dictionary blocks sampled")
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderMaxMemory(16<<20))
	if err != nil {
		return out, err
	}
	defer dec.Close()
	for _, name := range strings.Split(levelNames, ",") {
		var level zstd.EncoderLevel
		switch strings.TrimSpace(name) {
		case "fastest":
			level = zstd.SpeedFastest
		case "default":
			level = zstd.SpeedDefault
		case "better":
			level = zstd.SpeedBetterCompression
		default:
			return out, fmt.Errorf("unsupported zstd level %q", name)
		}
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level), zstd.WithEncoderConcurrency(1))
		if err != nil {
			return out, err
		}
		result := levelResult{Level: name, RawBytes: rawTotal}
		encoded := make([][]byte, len(samples))
		for i, s := range samples {
			encoded[i] = enc.EncodeAll(s.Raw, nil)
			decoded, err := dec.DecodeAll(encoded[i], make([]byte, 0, len(s.Raw)))
			if err != nil || !bytes.Equal(decoded, s.Raw) {
				enc.Close()
				return out, errors.New("zstd round trip failed")
			}
			result.ZstdBytes += uint64(len(encoded[i]))
			result.KeepOnlyBeneficialBytes += uint64(min(len(encoded[i]), len(s.Raw)))
		}
		position := 0
		for _, file := range c.Files {
			var storedTotal, maxRaw, largestSaving uint64
			for _, s := range file.Samples {
				beneficial := uint64(min(len(encoded[position]), len(s.Raw)))
				storedTotal += beneficial
				if uint64(len(s.Raw)) > maxRaw {
					maxRaw = uint64(len(s.Raw))
					largestSaving = maxRaw - beneficial
				}
				position++
			}
			result.KeepLargestSampleBlockRawBytes += storedTotal + largestSaving
			if uint64(len(file.Samples)) == file.Header.Blocks && storedTotal < maxRaw {
				result.FullySampledUnsafeCompressedFiles = append(result.FullySampledUnsafeCompressedFiles, file.Path)
			}
		}
		result.KeepLargestSampleBlockRawRatio = float64(result.KeepLargestSampleBlockRawBytes) / float64(rawTotal)
		result.ZstdRatio = float64(result.ZstdBytes) / float64(rawTotal)
		result.KeepBeneficialRatio = float64(result.KeepOnlyBeneficialBytes) / float64(rawTotal)
		result.Encode, err = measure(rawTotal*uint64(n), uint64(n*len(samples)), func() error {
			for repeat := 0; repeat < n; repeat++ {
				for i, s := range samples {
					encoded[i] = enc.EncodeAll(s.Raw, encoded[i][:0])
				}
			}
			return nil
		})
		if err != nil {
			enc.Close()
			return out, err
		}
		result.Decode, err = measure(rawTotal*uint64(n), uint64(n*len(samples)), func() error {
			for repeat := 0; repeat < n; repeat++ {
				for i, s := range samples {
					if _, err := dec.DecodeAll(encoded[i], make([]byte, 0, len(s.Raw))); err != nil {
						return err
					}
				}
			}
			return nil
		})
		enc.Close()
		if err != nil {
			return out, err
		}
		out.Results = append(out.Results, result)
	}
	// Metadata only in reports; -extract is the explicit path for sample bytes.
	for i := range out.Files {
		for j := range out.Files[i].Samples {
			out.Files[i].Samples[j].Raw = nil
		}
	}
	return out, nil
}
func main() {
	blocks := flag.Int("blocks", 32, "stratified dictionary blocks per file (1..1024)")
	repeat := flag.Int("repeat", 200, "CPU benchmark repetitions (1..10000)")
	levels := flag.String("levels", "fastest,default,better", "comma-separated fastest/default/better")
	extract := flag.Bool("extract", false, "emit a bounded sample JSON bundle; do not benchmark")
	input := flag.String("sample", "", "read an extracted sample JSON bundle instead of accessor files")
	maxRead := flag.Uint64("max-read-bytes", 32<<20, "total bytes read from all accessor files")
	maxRaw := flag.Uint64("max-raw-bytes", 64<<20, "total decoded sample bytes")
	flag.Parse()
	if *blocks < 1 || *blocks > 1024 || *repeat < 1 || *repeat > 10000 {
		fatal(errors.New("blocks/repeat outside bounds"))
	}
	var c capture
	var err error
	if *input != "" {
		if flag.NArg() != 0 {
			fatal(errors.New("-sample and accessor paths are mutually exclusive"))
		}
		file, e := os.Open(*input)
		if e != nil {
			fatal(e)
		}
		defer file.Close()
		err = json.NewDecoder(io.LimitReader(file, 128<<20)).Decode(&c)
		if err == nil && c.Format != "gtron-dictionary-zstd-sample-v1" {
			err = errors.New("unsupported sample format")
		}
		var rawBytes uint64
		for _, f := range c.Files {
			for _, s := range f.Samples {
				rawBytes += uint64(len(s.Raw))
			}
		}
		if rawBytes > *maxRaw {
			err = errors.New("sample exceeds decoded byte budget")
		}
	} else {
		if flag.NArg() == 0 || flag.NArg() > 16 {
			fatal(errors.New("pass 1..16 accessor .kv paths, or -sample bundle.json"))
		}
		c, err = captureSources(flag.Args(), *blocks, *maxRead, *maxRaw)
	}
	if err != nil {
		fatal(err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if *extract {
		err = encoder.Encode(c)
	} else {
		var r report
		r, err = benchmark(c, *repeat, *levels)
		if err == nil {
			err = encoder.Encode(r)
		}
	}
	if err != nil {
		fatal(err)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
