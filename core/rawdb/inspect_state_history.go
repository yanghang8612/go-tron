package rawdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

const (
	HistoryPrevMaxSamples     = 4096
	historyPrevMaxGroups      = 4096
	historyPrevTopCount       = 20
	historyPrevLargeThreshold = 16 << 10
	historyPrevGateKeyBytes   = 1 << 20
)

// ErrHistoryPrevInspectionPartial also covers cooperative resource limits. The
// accompanying report remains useful, but must never be labelled complete.
var ErrHistoryPrevInspectionPartial = errors.New("state history Prev inspection incomplete")

// InspectStateHistoryPackEncoding identifies a self-contained diagnostic file
// and validates its decoded-size header without allocating the decoded pack.
// It does not validate the RLP rows; callers must still run a complete decoder.
// Shared v3 references are deliberately rejected: they are not standalone files.
func InspectStateHistoryPackEncoding(encoded []byte) (codec string, decodedBytes uint64, err error) {
	_, length, compressed, err := stateDomainChangeBlockCompressionPayload(encoded)
	if err != nil {
		return "", 0, err
	}
	codec = "raw"
	if !compressed {
		length = len(encoded)
	} else if encoded[len(stateDomainChangeBlockEnvelopeMagic)] == stateDomainChangeBlockChunksVersion {
		codec = "chunks2"
	} else {
		codec = "snappy1"
	}
	if length <= 0 || length > stateDomainChangeBlockMaxDecodedBytes {
		return "", 0, errors.New("history diagnostic pack decoded length exceeds limits")
	}
	return codec, uint64(length), nil
}

type HistoryPrevInspectOptions struct {
	FromBlock         uint64        `json:"from_block"`
	ToBlock           uint64        `json:"to_block"`
	Seed              uint64        `json:"seed"`
	Samples           int           `json:"samples"`
	MaxEncodedBytes   uint64        `json:"max_encoded_bytes"`
	MaxChunkReadBytes uint64        `json:"max_chunk_read_bytes"`
	MaxExportBytes    uint64        `json:"max_export_bytes"`
	MaxDecodedBytes   uint64        `json:"max_decoded_bytes"`
	MaxRows           uint64        `json:"max_rows"`
	MaxDuration       time.Duration `json:"max_duration_ns"`
	// OnCompletePack is called synchronously only after a full successful decode.
	// The encoded slice must not be retained or mutated. It enables explicit
	// diagnostic export. Shared v3 packs are materialized to self-contained raw
	// RLP; legacy packs keep their original bytes. ExportCodec/ExportBytes describe
	// this argument, while Codec/EncodedBytes describe the physical source pack.
	OnCompletePack func(HistoryPrevPackSample, []byte) error `json:"-"`
}

// DefaultHistoryPrevInspectOptions does not infer a height range from chain
// state. Callers must select that range explicitly.
func DefaultHistoryPrevInspectOptions() HistoryPrevInspectOptions {
	return HistoryPrevInspectOptions{Seed: 20260913, Samples: 256, MaxEncodedBytes: 256 << 20, MaxChunkReadBytes: 256 << 20, MaxExportBytes: 1 << 30,
		MaxDecodedBytes: 1 << 30, MaxRows: 2_000_000, MaxDuration: time.Minute}
}

func (o HistoryPrevInspectOptions) Validate() error {
	if o.FromBlock > o.ToBlock || o.ToBlock-o.FromBlock == ^uint64(0) {
		return errors.New("invalid inclusive height range (the full uint64 range is unsupported)")
	}
	if o.Samples < 1 || o.Samples > HistoryPrevMaxSamples {
		return fmt.Errorf("samples must be in [1,%d]", HistoryPrevMaxSamples)
	}
	if o.MaxEncodedBytes == 0 || o.MaxEncodedBytes > 1<<30 || o.MaxDecodedBytes == 0 || o.MaxDecodedBytes > 4<<30 {
		return errors.New("encoded budget must be in [1,1GiB], decoded budget in [1,4GiB]")
	}
	if o.MaxChunkReadBytes > 1<<30 || o.MaxExportBytes > 4<<30 {
		return errors.New("chunk read budget must be at most 1GiB, export budget at most 4GiB")
	}
	if o.MaxRows == 0 || o.MaxRows > 10_000_000 || o.MaxDuration <= 0 || o.MaxDuration > 5*time.Minute {
		return errors.New("rows budget must be in [1,10000000], duration in (0,5m]")
	}
	return nil
}

type HistoryPrevHistogram struct {
	Labels [10]string `json:"labels"`
	Rows   [10]uint64 `json:"rows"`
	Bytes  [10]uint64 `json:"bytes"`
}

func newHistoryPrevHistogram() HistoryPrevHistogram {
	return HistoryPrevHistogram{Labels: [10]string{"0", "1..32", "33..256", "257..1024", "1025..4096", "4097..16384", "16385..65536", "65537..262144", "262145..1048576", ">1048576"}}
}

func (h *HistoryPrevHistogram) add(n uint64) {
	i := 0
	for _, upper := range [...]uint64{0, 32, 256, 1024, 4096, 16384, 65536, 262144, 1048576} {
		if n <= upper {
			break
		}
		i++
	}
	h.Rows[i]++
	h.Bytes[i] += n
}

type HistoryPrevDomainStat struct {
	FlatDomain   StateFlatDomain      `json:"flat_domain"`
	FlatName     string               `json:"flat_name"`
	KVDomain     kvdomains.KVDomain   `json:"kv_domain"`
	KVName       string               `json:"kv_name"`
	Rows         uint64               `json:"rows"`
	PrevExists   uint64               `json:"prev_exists"`
	PrevBytes    uint64               `json:"prev_bytes"`
	KeyBytes     uint64               `json:"key_bytes"`
	MaxPrevBytes uint64               `json:"max_prev_bytes"`
	Histogram    HistoryPrevHistogram `json:"histogram"`
}

// Only identities are retained, never Prev values. Key hashes cover the entire
// logical key; the bounded prefix is a diagnostic aid, not an identity shortcut.
type HistoryPrevKeyIdentity struct {
	FlatDomain   StateFlatDomain    `json:"flat_domain"`
	Owner        string             `json:"owner"`
	Generation   uint64             `json:"generation"`
	KVDomain     kvdomains.KVDomain `json:"kv_domain"`
	KeyBytes     uint64             `json:"key_bytes"`
	KeySHA256    string             `json:"key_sha256"`
	KeyPrefixHex string             `json:"key_prefix_hex"`
}

type HistoryPrevLargestRow struct {
	HistoryPrevKeyIdentity
	Block      uint64 `json:"block"`
	Seq        uint64 `json:"seq"`
	TxNum      uint64 `json:"tx_num"`
	PrevExists bool   `json:"prev_exists"`
	PrevBytes  uint64 `json:"prev_bytes"`
}

type HistoryPrevLargeKey struct {
	HistoryPrevKeyIdentity
	LargeVersions  uint64 `json:"large_versions"`
	LargePrevBytes uint64 `json:"large_prev_bytes"`
	MaxPrevBytes   uint64 `json:"max_prev_bytes"`
	FirstBlock     uint64 `json:"first_block"`
	LastBlock      uint64 `json:"last_block"`
}

type HistoryPrevPackSample struct {
	Block                         uint64 `json:"block"`
	StratumFrom                   uint64 `json:"stratum_from"`
	StratumTo                     uint64 `json:"stratum_to"`
	Status                        string `json:"status"`
	Error                         string `json:"error,omitempty"`
	Codec                         string `json:"codec"`
	EncodedBytes                  uint64 `json:"encoded_bytes"`
	ChunkReadBytes                uint64 `json:"chunk_read_bytes"`
	ChunkReads                    uint64 `json:"chunk_reads"`
	ExportCodec                   string `json:"export_codec,omitempty"`
	ExportBytes                   uint64 `json:"export_bytes"`
	DecodedBytes                  uint64 `json:"decoded_bytes"`
	Rows                          uint64 `json:"rows"`
	PrevExists                    uint64 `json:"prev_exists"`
	PrevBytes                     uint64 `json:"prev_bytes"`
	KeyBytes                      uint64 `json:"key_bytes"`
	MaxPrevBytes                  uint64 `json:"max_prev_bytes"`
	ChunkGate                     string `json:"chunk_gate"`
	LargeIdentityMaxVersions      uint64 `json:"large_identity_max_versions"`
	LargeIdentityTrackingComplete bool   `json:"large_identity_tracking_complete"`
}

type HistoryPrevInspection struct {
	Options               HistoryPrevInspectOptions `json:"options"`
	Scope                 string                    `json:"scope"`
	StartedAt             time.Time                 `json:"started_at"`
	ElapsedSeconds        float64                   `json:"elapsed_seconds"`
	Complete              bool                      `json:"complete"`
	StopReason            string                    `json:"stop_reason"`
	Error                 string                    `json:"error,omitempty"`
	Samples               []HistoryPrevPackSample   `json:"samples"`
	PacksComplete         uint64                    `json:"packs_complete"`
	MissingPacks          uint64                    `json:"missing_packs"`
	EncodedBytesRead      uint64                    `json:"encoded_bytes_read"`
	EncodedBytesAccepted  uint64                    `json:"encoded_bytes_accepted"`
	ChunkReadBytes        uint64                    `json:"chunk_read_bytes"`
	ChunkReads            uint64                    `json:"chunk_reads"`
	ExportBytesAttempted  uint64                    `json:"export_bytes_attempted"`
	ExportBytesAccepted   uint64                    `json:"export_bytes_accepted"`
	DecodedBytesReserved  uint64                    `json:"decoded_bytes_reserved"`
	Rows                  uint64                    `json:"rows"`
	PrevBytes             uint64                    `json:"prev_bytes"`
	KeyBytes              uint64                    `json:"key_bytes"`
	MaxPrevBytes          uint64                    `json:"max_prev_bytes"`
	Histogram             HistoryPrevHistogram      `json:"histogram"`
	Domains               []HistoryPrevDomainStat   `json:"domains"`
	LargestRows           []HistoryPrevLargestRow   `json:"largest_rows"`
	LargeKeys             []HistoryPrevLargeKey     `json:"large_keys"`
	LargeKeysTracked      int                       `json:"large_keys_tracked"`
	LargeKeyOverflowRows  uint64                    `json:"large_key_overflow_rows"`
	LargeKeyOverflowBytes uint64                    `json:"large_key_overflow_bytes"`
	Limitations           []string                  `json:"limitations"`
}

// InspectStateHistoryPrev performs bounded exact point reads of modern seq=0
// packs and their referenced chunks. It never opens an iterator or touches
// legacy seq>0 rows or ancient. A snapshot is acquired when supported; readers
// without snapshots support only self-contained packs. Resource checks are
// cooperative: an individual Get/decompression/row cannot be interrupted.
func InspectStateHistoryPrev(ctx context.Context, db ethdb.KeyValueReader, opts HistoryPrevInspectOptions) (report HistoryPrevInspection, resultErr error) {
	defaults := DefaultHistoryPrevInspectOptions()
	if opts.MaxChunkReadBytes == 0 {
		opts.MaxChunkReadBytes = defaults.MaxChunkReadBytes
	}
	if opts.MaxExportBytes == 0 {
		opts.MaxExportBytes = defaults.MaxExportBytes
	}
	if err := opts.Validate(); err != nil {
		return report, err
	}
	if ctx == nil || db == nil {
		return report, errors.New("nil inspection context or reader")
	}
	started := time.Now()
	report = HistoryPrevInspection{Options: opts, StartedAt: started.UTC(), Scope: "sampled physical state-changeset-v2 seq=0 packs and referenced shared chunks; exports are self-contained diagnostic representations",
		Histogram: newHistoryPrevHistogram(), Samples: historyPrevSamples(opts),
		Limitations: []string{
			"Empty selections are retained without replacement; results are not the full hot retention, cold history, or physical disk usage.",
			"Legacy seq>0 rows and tx-range/canonical metadata are not read; missing means missing seq=0 pack, not missing block/history.",
			"Budgets are cooperative between Get/decode/rows. Get must materialize one encoded value before its size is known; encoded_bytes_read includes a rejected value.",
			"decoded_bytes_reserved includes attempted decodes, including failures; row/domain totals may contain a valid prefix of an incomplete pack.",
			"encoded_bytes_* count physical pack values only. chunk_read_bytes counts returned stored chunk values, including repeated references and a rejected value; chunk_reads counts resolution attempts, not physical disk I/O. One lookup may include backend presence verification.",
			"Shared3 exports contain materialized raw RLP. export_bytes_attempted includes failed callbacks; export_bytes_accepted includes successful callbacks only. Export sizes are not original pack/chunk disk occupation or compression savings.",
			"largest_rows is top20 processed rows; large_keys is top20 among the first4096 identities with Prev>=16KiB, counting only such large rows. Overflow is explicit; it is not a guaranteed global top.",
			"Large-key grouping uses full identity plus SHA256(key); no value is retained or emitted. No account payload semantics are inferred.",
			"Chunk gate checks only size and repeated large full identity; it does not prove runtime enablement or the additional codec saving threshold. Capped identity tracking reports unknown when inconclusive.",
			fmt.Sprintf("large_identity_max_versions counts PrevExists rows with Prev>=%dKiB for one full identity inside a pack; measured only when raw>=%dKiB. It is a lower bound unless large_identity_tracking_complete is true.", historychunk.MaxSize>>10, stateChangeBlockChunkMinRawBytes>>10),
		}}
	domains := make(map[[2]uint16]*HistoryPrevDomainStat)
	large := make(map[historyPrevIdentity]*HistoryPrevLargeKey)
	defer func() {
		report.ElapsedSeconds = time.Since(started).Seconds()
		for _, d := range domains {
			report.Domains = append(report.Domains, *d)
		}
		sort.Slice(report.Domains, func(i, j int) bool {
			a, b := report.Domains[i], report.Domains[j]
			if a.FlatDomain != b.FlatDomain {
				return a.FlatDomain < b.FlatDomain
			}
			return a.KVDomain < b.KVDomain
		})
		report.LargeKeysTracked = len(large)
		for _, k := range large {
			report.LargeKeys = append(report.LargeKeys, *k)
		}
		sort.Slice(report.LargeKeys, func(i, j int) bool {
			a, b := report.LargeKeys[i], report.LargeKeys[j]
			if a.LargePrevBytes != b.LargePrevBytes {
				return a.LargePrevBytes > b.LargePrevBytes
			}
			return historyPrevIdentityLess(a.HistoryPrevKeyIdentity, b.HistoryPrevKeyIdentity)
		})
		if len(report.LargeKeys) > historyPrevTopCount {
			report.LargeKeys = report.LargeKeys[:historyPrevTopCount]
		}
	}()
	stop := func(reason string, err error) (HistoryPrevInspection, error) {
		report.StopReason = reason
		if err == nil {
			err = ErrHistoryPrevInspectionPartial
		}
		report.Error = err.Error()
		return report, fmt.Errorf("%w: %s: %w", ErrHistoryPrevInspectionPartial, reason, err)
	}
	check := func() (string, error) {
		if err := ctx.Err(); err != nil {
			return "context", err
		}
		if time.Since(started) >= opts.MaxDuration {
			return "duration_budget", nil
		}
		return "", nil
	}
	if reason, err := check(); reason != "" {
		return stop(reason, err)
	}
	view, releaseView, err := AcquireStateHistoryReadView(db)
	if err != nil {
		return stop("snapshot_error", err)
	}
	defer func() {
		if err := releaseView(); err != nil {
			report.Complete = false
			report.StopReason = "snapshot_close_error"
			report.Error = err.Error()
			resultErr = errors.Join(resultErr, ErrHistoryPrevInspectionPartial, err)
		}
	}()
	db = view
	for i := range report.Samples {
		sample := &report.Samples[i]
		if reason, err := check(); reason != "" {
			return stop(reason, err)
		}
		if report.Rows >= opts.MaxRows {
			return stop("rows_budget", nil)
		}
		if report.EncodedBytesAccepted >= opts.MaxEncodedBytes {
			return stop("encoded_budget", nil)
		}
		if report.DecodedBytesReserved >= opts.MaxDecodedBytes {
			return stop("decoded_budget", nil)
		}
		data, exists, err := readPresentValue(db, stateChangeSetKey(sample.Block, 0), fmt.Sprintf("history Prev pack %d", sample.Block))
		if err != nil {
			sample.Status = "read_error"
			sample.Error = err.Error()
			return stop("read_error", err)
		}
		sample.Status = "read"
		sample.EncodedBytes = uint64(len(data))
		report.EncodedBytesRead += sample.EncodedBytes
		if reason, err := check(); reason != "" {
			return stop(reason, err)
		}
		if !exists {
			sample.Status = "missing"
			report.MissingPacks++
			continue
		}
		if sample.EncodedBytes > opts.MaxEncodedBytes-report.EncodedBytesAccepted {
			sample.Status = "encoded_budget"
			return stop("encoded_budget", nil)
		}
		report.EncodedBytesAccepted += sample.EncodedBytes
		shared := isStateHistorySharedPack(data)
		decodedLen, compressed := 0, false
		if shared {
			// Validate the complete reference header and allocation budget before
			// issuing any chunk reads. The physical key binds the expected block.
			_, decodedLen, _, _, err = sharedStateHistoryPackHeader(data, sample.Block)
		} else {
			_, decodedLen, compressed, err = stateDomainChangeBlockCompressionPayload(data)
			if err == nil && !compressed {
				decodedLen = len(data)
			}
		}
		if err == nil && (decodedLen <= 0 || decodedLen > stateDomainChangeBlockMaxDecodedBytes) {
			err = fmt.Errorf("decoded pack size %d outside [1,%d]", decodedLen, stateDomainChangeBlockMaxDecodedBytes)
		}
		if err != nil {
			sample.Status = "decode_error"
			sample.Error = err.Error()
			return stop("decode_error", err)
		}
		sample.DecodedBytes = uint64(decodedLen)
		sample.Codec = "raw"
		if shared {
			sample.Codec = "shared3"
		} else if compressed {
			sample.Codec = fmt.Sprintf("snappy%d", data[len(stateDomainChangeBlockEnvelopeMagic)])
			if data[len(stateDomainChangeBlockEnvelopeMagic)] == stateDomainChangeBlockChunksVersion {
				sample.Codec = "chunks2"
			}
		}
		if sample.DecodedBytes > opts.MaxDecodedBytes-report.DecodedBytesReserved {
			sample.Status = "decoded_budget"
			return stop("decoded_budget", nil)
		}
		report.DecodedBytesReserved += sample.DecodedBytes
		if shared {
			reader := &historyPrevChunkReader{view: view, check: check, report: &report, sample: sample, limit: opts.MaxChunkReadBytes}
			data, err = materializeStateHistorySharedPack(data, sample.Block, []ethdb.KeyValueReader{reader})
			if err != nil {
				reason := reader.stopReason
				if reason == "" {
					reason = "decode_error"
				}
				sample.Status, sample.Error = reason, err.Error()
				return stop(reason, err)
			}
		}
		gate := historyPrevChunkGate{seen: make(map[historyPrevGateIdentity]uint64)}
		rowStop := ""
		cont, err := iteratePersistedStateDomainChangeBlockBorrowed(data, sample.Block, func(c *StateDomainChange) (bool, error) {
			if reason, err := check(); reason != "" {
				rowStop = reason
				return false, err
			}
			if report.Rows >= opts.MaxRows {
				rowStop = "rows_budget"
				return false, nil
			}
			domainKey := [2]uint16{uint16(c.FlatDomain), uint16(c.Domain)}
			d := domains[domainKey]
			if d == nil {
				if len(domains) >= historyPrevMaxGroups {
					rowStop = "domain_groups_budget"
					return false, nil
				}
				d = &HistoryPrevDomainStat{FlatDomain: c.FlatDomain, FlatName: c.FlatDomain.String(), KVDomain: c.Domain, KVName: kvdomains.Name(c.Domain), Histogram: newHistoryPrevHistogram()}
				domains[domainKey] = d
			}
			n, k := uint64(len(c.Prev)), uint64(len(c.Key))
			report.Rows++
			report.PrevBytes += n
			report.KeyBytes += k
			report.MaxPrevBytes = max(report.MaxPrevBytes, n)
			report.Histogram.add(n)
			sample.Rows++
			sample.PrevBytes += n
			sample.KeyBytes += k
			sample.MaxPrevBytes = max(sample.MaxPrevBytes, n)
			d.Rows++
			d.PrevBytes += n
			d.KeyBytes += k
			d.MaxPrevBytes = max(d.MaxPrevBytes, n)
			d.Histogram.add(n)
			if c.PrevExists {
				sample.PrevExists++
				d.PrevExists++
			}
			if len(report.LargestRows) < historyPrevTopCount || n > report.LargestRows[len(report.LargestRows)-1].PrevBytes {
				report.LargestRows = append(report.LargestRows, HistoryPrevLargestRow{HistoryPrevKeyIdentity: historyPrevPublicIdentity(c), Block: c.BlockNum, Seq: c.Seq, TxNum: c.TxNum, PrevExists: c.PrevExists, PrevBytes: n})
				sort.SliceStable(report.LargestRows, func(i, j int) bool { return report.LargestRows[i].PrevBytes > report.LargestRows[j].PrevBytes })
				if len(report.LargestRows) > historyPrevTopCount {
					report.LargestRows = report.LargestRows[:historyPrevTopCount]
				}
			}
			if n >= historyPrevLargeThreshold {
				id := historyPrevIdentityOf(c)
				entry := large[id]
				if entry == nil && len(large) < historyPrevMaxGroups {
					entry = &HistoryPrevLargeKey{HistoryPrevKeyIdentity: historyPrevPublicIdentity(c), FirstBlock: c.BlockNum}
					large[id] = entry
				}
				if entry == nil {
					report.LargeKeyOverflowRows++
					report.LargeKeyOverflowBytes += n
				} else {
					entry.LargeVersions++
					entry.LargePrevBytes += n
					entry.MaxPrevBytes = max(entry.MaxPrevBytes, n)
					entry.LastBlock = c.BlockNum
				}
			}
			if decodedLen >= stateChangeBlockChunkMinRawBytes {
				gate.observe(c)
			}
			return true, nil
		})
		sample.ChunkGate = gate.result(decodedLen, cont && err == nil)
		sample.LargeIdentityMaxVersions = gate.maxVersions
		sample.LargeIdentityTrackingComplete = decodedLen >= stateChangeBlockChunkMinRawBytes && cont && err == nil && !gate.overflow
		if rowStop != "" {
			sample.Status = rowStop
			return stop(rowStop, err)
		}
		if err != nil || !cont {
			if err == nil {
				err = errors.New("borrowed pack decoder stopped unexpectedly")
			}
			sample.Status = "decode_error"
			sample.Error = err.Error()
			return stop("decode_error", err)
		}
		sample.Status = "complete"
		report.PacksComplete++
		if reason, err := check(); reason != "" {
			return stop(reason, err)
		}
		if opts.OnCompletePack != nil {
			if uint64(len(data)) > opts.MaxExportBytes-report.ExportBytesAttempted {
				sample.Status = "export_budget"
				return stop("export_budget", nil)
			}
			sample.ExportCodec = sample.Codec
			if shared {
				sample.ExportCodec = "raw"
			}
			sample.ExportBytes = uint64(len(data))
			report.ExportBytesAttempted += sample.ExportBytes
			if err := opts.OnCompletePack(*sample, data); err != nil {
				sample.Status = "export_error"
				sample.Error = err.Error()
				return stop("export_error", err)
			}
			report.ExportBytesAccepted += sample.ExportBytes
			if reason, err := check(); reason != "" {
				return stop(reason, err)
			}
		}
	}
	report.Complete = true
	report.StopReason = "complete"
	return report, nil
}

// This adapter keeps the already acquired view and measures each chunk lookup
// before/after the underlying call. It never creates another snapshot or caches
// values. As with pack Get, one stored value must be materialized before its
// actual encoded length is known; that rejected value is still charged.
type historyPrevChunkReader struct {
	view       StateHistoryReadView
	check      func() (string, error)
	report     *HistoryPrevInspection
	sample     *HistoryPrevPackSample
	limit      uint64
	stopReason string
}

func (r *historyPrevChunkReader) IsPinnedKeyValueView() bool { return r.view.IsPinnedKeyValueView() }

func (r *historyPrevChunkReader) Has(key []byte) (bool, error) {
	_, exists, err := r.GetWithPresence(key)
	return exists, err
}

func (r *historyPrevChunkReader) Get(key []byte) ([]byte, error) {
	value, exists, err := r.GetWithPresence(key)
	if err == nil && !exists {
		err = errors.New("history inspection: missing shared chunk")
	}
	return value, err
}

func (r *historyPrevChunkReader) GetWithPresence(key []byte) ([]byte, bool, error) {
	if reason, err := r.check(); reason != "" {
		r.stopReason = reason
		if err == nil {
			err = ErrHistoryPrevInspectionPartial
		}
		return nil, false, err
	}
	if r.report.ChunkReadBytes >= r.limit {
		r.stopReason = "chunk_read_budget"
		return nil, false, ErrHistoryPrevInspectionPartial
	}
	r.report.ChunkReads++
	r.sample.ChunkReads++
	value, exists, err := readPresentValue(r.view, key, "inspection shared chunk")
	r.report.ChunkReadBytes += uint64(len(value))
	r.sample.ChunkReadBytes += uint64(len(value))
	if reason, checkErr := r.check(); reason != "" {
		r.stopReason = reason
		if checkErr == nil {
			checkErr = ErrHistoryPrevInspectionPartial
		}
		return nil, false, errors.Join(err, checkErr)
	}
	if r.report.ChunkReadBytes > r.limit {
		r.stopReason = "chunk_read_budget"
		return nil, false, errors.Join(err, ErrHistoryPrevInspectionPartial)
	}
	return value, exists, err
}

func historyPrevSamples(o HistoryPrevInspectOptions) []HistoryPrevPackSample {
	span := o.ToBlock - o.FromBlock + 1
	n := uint64(o.Samples)
	if span < n {
		n = span
	}
	width, extra := span/n, span%n
	rng := rand.New(rand.NewPCG(o.Seed, 0x686973746f727970))
	out := make([]HistoryPrevPackSample, 0, n)
	start := o.FromBlock
	for i := uint64(0); i < n; i++ {
		w := width
		if i < extra {
			w++
		}
		end := start + w - 1
		out = append(out, HistoryPrevPackSample{Block: start + rng.Uint64N(w), StratumFrom: start, StratumTo: end, Status: "not_visited", Codec: "unknown", ChunkGate: "not_evaluated"})
		if i+1 < n {
			start = end + 1
		}
	}
	return out
}

type historyPrevIdentity struct {
	flat       StateFlatDomain
	owner      common.Address
	generation uint64
	domain     kvdomains.KVDomain
	keyBytes   int
	keyHash    [32]byte
}

func historyPrevIdentityOf(c *StateDomainChange) historyPrevIdentity {
	return historyPrevIdentity{c.FlatDomain, c.Owner, c.Generation, c.Domain, len(c.Key), sha256.Sum256(c.Key)}
}

func historyPrevPublicIdentity(c *StateDomainChange) HistoryPrevKeyIdentity {
	digest := sha256.Sum256(c.Key)
	return HistoryPrevKeyIdentity{FlatDomain: c.FlatDomain, Owner: c.Owner.Hex(), Generation: c.Generation, KVDomain: c.Domain, KeyBytes: uint64(len(c.Key)), KeySHA256: hex.EncodeToString(digest[:]), KeyPrefixHex: hex.EncodeToString(c.Key[:min(len(c.Key), 64)])}
}

func historyPrevIdentityLess(a, b HistoryPrevKeyIdentity) bool {
	if a.FlatDomain != b.FlatDomain {
		return a.FlatDomain < b.FlatDomain
	}
	if a.Owner != b.Owner {
		return a.Owner < b.Owner
	}
	if a.Generation != b.Generation {
		return a.Generation < b.Generation
	}
	if a.KVDomain != b.KVDomain {
		return a.KVDomain < b.KVDomain
	}
	return a.KeySHA256 < b.KeySHA256
}

// Gate tracking uses exact full key bytes, with independent entry/byte limits.
// A limit does not invalidate the domain measurements, only a negative gate
// conclusion. A later repeat of an already admitted identity can still prove it.
type historyPrevGateIdentity struct {
	flat       StateFlatDomain
	owner      common.Address
	generation uint64
	domain     kvdomains.KVDomain
	key        string
}
type historyPrevChunkGate struct {
	seen        map[historyPrevGateIdentity]uint64
	keyBytes    int
	repeated    bool
	overflow    bool
	maxVersions uint64
}

func (g *historyPrevChunkGate) observe(c *StateDomainChange) {
	if !c.PrevExists || len(c.Prev) < historychunk.MaxSize {
		return
	}
	// Reject oversized identities before copying the borrowed key.
	if len(c.Key) > historyPrevGateKeyBytes {
		g.overflow = true
		return
	}
	id := historyPrevGateIdentity{c.FlatDomain, c.Owner, c.Generation, c.Domain, string(c.Key)}
	if count, exists := g.seen[id]; exists {
		g.seen[id] = count + 1
		g.maxVersions = max(g.maxVersions, count+1)
		g.repeated = true
		return
	}
	if len(g.seen) >= historyPrevMaxGroups || len(c.Key) > historyPrevGateKeyBytes-g.keyBytes {
		g.overflow = true
		return
	}
	g.seen[id] = 1
	g.maxVersions = max(g.maxVersions, 1)
	g.keyBytes += len(c.Key)
}
func (g *historyPrevChunkGate) result(decodedLen int, complete bool) string {
	if !complete {
		return "unknown_incomplete_pack"
	}
	if decodedLen < stateChangeBlockChunkMinRawBytes {
		return fmt.Sprintf("ineligible_raw_below_%dKiB", stateChangeBlockChunkMinRawBytes>>10)
	}
	if g.repeated {
		return "eligible_size_and_repeated_large_identity"
	}
	if g.overflow {
		return "unknown_identity_budget"
	}
	return "ineligible_no_repeated_identity_with_Prev_ge_128KiB"
}
