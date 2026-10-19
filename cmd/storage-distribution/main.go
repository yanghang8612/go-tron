// storage-distribution scans an authenticated immutable chaindata image.
// All writes are diagnostic artifacts under a new directory outside the image.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type metric struct {
	Rows    uint64 `json:"rows"`
	Key     uint64 `json:"key_bytes"`
	Value   uint64 `json:"wrapped_value_bytes"`
	Payload uint64 `json:"payload_bytes"`
}

func (m *metric) add(k, v, payload []byte) {
	m.Rows++
	m.Key += uint64(len(k))
	m.Value += uint64(len(v))
	m.Payload += uint64(len(payload))
}
func add(m map[string]*metric, s string, k, v, payload []byte) {
	if m[s] == nil {
		m[s] = new(metric)
	}
	m[s].add(k, v, payload)
}
func framed(h hash.Hash, k, v []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(k)))
	h.Write(n[:])
	h.Write(k)
	binary.BigEndian.PutUint64(n[:], uint64(len(v)))
	h.Write(n[:])
	h.Write(v)
}
func zeros(v []byte) int {
	n := 0
	for n < len(v) && v[n] == 0 {
		n++
	}
	return n
}

type incarnation struct {
	Owner      common.Address
	Generation uint64
}
type ownerMetric struct {
	Owner      string `json:"owner"`
	Generation uint64 `json:"generation"`
	Class      string `json:"class"`
	metric
	Prefix                string `json:"slot_prefix_16_hex"`
	Prefixes              uint64 `json:"distinct_slot_prefixes"`
	ZeroRows              uint64 `json:"zero_rows"`
	TrimmedBytes          uint64 `json:"trimmed_payload_bytes"`
	SmallTailRows         uint64 `json:"slot_tail_fits_u64_rows"`
	Name                  string `json:"contract_name,omitempty"`
	Version               int32  `json:"contract_version"`
	MetadataPresent       bool   `json:"metadata_present"`
	PrefixMatchesMetadata bool   `json:"prefix_matches_metadata"`
}
type rootRun struct {
	Owner      string `json:"owner"`
	Generation uint64 `json:"generation"`
	Start      uint64 `json:"start"`
	End        uint64 `json:"end"`
	Root       string `json:"root"`
}
type report struct {
	Complete          bool                    `json:"complete"`
	Error             string                  `json:"error,omitempty"`
	Started           time.Time               `json:"started_utc"`
	Finished          time.Time               `json:"finished_utc"`
	Block             uint64                  `json:"block_num"`
	BlockHash         string                  `json:"block_hash"`
	TxNum             uint64                  `json:"tx_num"`
	Root              string                  `json:"muhash_root"`
	SourceRows        [3]uint64               `json:"source_rows"`
	SourceSHA         [3]string               `json:"source_sha256"`
	Authenticated     bool                    `json:"matches_authenticated_baseline"`
	Storage           map[string]*metric      `json:"storage_classes"`
	Shielded          map[string]*metric      `json:"shielded_subkeys"`
	ValueLeadingZeros map[string]*metric      `json:"storage_value_leading_zero_bytes"`
	ValueLength       map[string]*metric      `json:"storage_payload_lengths"`
	ValueKinds        map[string]*metric      `json:"storage_value_kinds"`
	StorageOwners     uint64                  `json:"storage_owners"`
	Incarnations      uint64                  `json:"storage_incarnations"`
	PrefixVariants    uint64                  `json:"incarnations_with_multiple_slot_prefixes"`
	TrimmedBytes      uint64                  `json:"storage_trimmed_payload_bytes"`
	RootRows          uint64                  `json:"block_root_rows"`
	DistinctRoots     uint64                  `json:"distinct_block_roots"`
	Runs              uint64                  `json:"block_root_runs"`
	MinHeight         uint64                  `json:"min_indexed_height"`
	MaxHeight         uint64                  `json:"max_indexed_height"`
	Gaps              uint64                  `json:"missing_heights_between_index_rows"`
	LargestRun        uint64                  `json:"largest_root_run_blocks"`
	RootFrequencies   map[string]uint64       `json:"block_root_frequencies"`
	Experiments       map[string]*tableSeries `json:"sst_experiments,omitempty"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string) error {
	f := flag.NewFlagSet("storage-distribution", flag.ContinueOnError)
	dbdir := f.String("db-dir", "", "immutable offline chaindata")
	baseline := f.String("baseline", "", "complete authenticated generation census")
	out := f.String("out-dir", "", "new directory outside DB")
	experiments := f.Bool("sst-experiments", false, "write fresh isolated comparison SSTs, never alter source")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *dbdir == "" || *baseline == "" || *out == "" || f.NArg() != 0 {
		return errors.New("db-dir, baseline and out-dir required")
	}
	path, err := filepath.EvalSymlinks(*dbdir)
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
	parent, err := filepath.EvalSymlinks(filepath.Dir(*out))
	if err != nil {
		return err
	}
	parent, err = filepath.Abs(parent)
	if err != nil {
		return err
	}
	dest := filepath.Join(parent, filepath.Base(*out))
	rel, err := filepath.Rel(path, dest)
	if err != nil || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("out-dir must be outside DB")
	}
	data, err := os.ReadFile(*baseline)
	if err != nil {
		return err
	}
	var prior rawdb.GenerationCensus
	if err = json.Unmarshal(data, &prior); err != nil {
		return err
	}
	if !prior.Complete || !prior.MuHashVerified {
		return errors.New("baseline must be complete and full MuHash verified")
	}
	if err = os.Mkdir(dest, 0700); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()
	db, err := rawdb.NewPebbleDBReadOnly(path, 64, 128)
	var r report
	if err == nil {
		r, err = inspect(ctx, db, prior, dest, *experiments)
		err = errors.Join(err, db.Close())
	}
	if err != nil {
		r.Complete = false
		r.Error = err.Error()
	}
	r.Finished = time.Now().UTC()
	file, e := os.OpenFile(filepath.Join(dest, "distribution.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return errors.Join(err, e)
	}
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	return errors.Join(err, enc.Encode(r), file.Sync(), file.Close())
}

func inspect(ctx context.Context, db interface {
	ethdb.KeyValueReader
	ethdb.Iteratee
}, prior rawdb.GenerationCensus, out string, experiments bool) (r report, retErr error) {
	defer func() {
		if retErr != nil {
			r.Complete = false
			r.Error = retErr.Error()
		}
	}()
	r.Started = time.Now().UTC()
	r.Storage = map[string]*metric{}
	r.Shielded = map[string]*metric{}
	r.ValueLeadingZeros = map[string]*metric{}
	r.ValueLength = map[string]*metric{}
	r.ValueKinds = map[string]*metric{}
	r.RootFrequencies = map[string]uint64{}
	head, ok, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil {
		return r, err
	}
	if !ok || head.BlockNum != prior.BlockNum || head.BlockHash != prior.BlockHash || head.TxNum != prior.TxNum || head.Root != prior.Root {
		return r, errors.New("baseline HEAD differs")
	}
	if pending, err := rawdb.StateCommitmentRestoreInProgress(db); err != nil || pending {
		return r, errors.Join(err, errors.New("restore marker"))
	}
	if pending, err := rawdb.StateCommitmentUnwindCleanupPending(db); err != nil || pending {
		return r, errors.Join(err, errors.New("unwind marker"))
	}
	r.Block = head.BlockNum
	r.BlockHash = hex.EncodeToString(head.BlockHash[:])
	r.TxNum = head.TxNum
	r.Root = hex.EncodeToString(head.Root[:])
	prefixes := rawdb.StateCommitmentSourcePrefixes()
	hashes := [3]hash.Hash{sha256.New(), sha256.New(), sha256.New()}
	gens := map[common.Address]uint64{}
	for family := 0; family < 2; family++ {
		it := db.NewIterator(prefixes[family], nil)
		for it.Next() {
			if err = ctx.Err(); err != nil {
				break
			}
			k, v := it.Key(), it.Value()
			framed(hashes[family], k, v)
			r.SourceRows[family]++
			if family == 1 {
				owner, valid := rawdb.DecodeStateKVGenerationKey(k)
				if !valid {
					err = errors.New("bad generation key")
					break
				}
				g, e := rawdb.DecodeStateKVGenerationValue(v)
				if e != nil {
					err = e
					break
				}
				gens[owner] = g
			}
		}
		err = errors.Join(err, it.Error())
		it.Release()
		err = errors.Join(err, it.Error())
		if err != nil {
			return r, err
		}
	}
	owners := map[incarnation]*ownerMetric{}
	ownerSet := map[common.Address]bool{}
	lastPrefix := map[incarnation][16]byte{}
	values, err := os.OpenFile(filepath.Join(out, "storage-values.hex"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return r, err
	}
	vw := bufio.NewWriterSize(values, 1<<20)
	defer func() { retErr = errors.Join(retErr, vw.Flush(), values.Sync(), values.Close()) }()
	runs, err := os.OpenFile(filepath.Join(out, "shielded-runs.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return r, err
	}
	rw := bufio.NewWriterSize(runs, 1<<16)
	defer func() { retErr = errors.Join(retErr, rw.Flush(), runs.Sync(), runs.Close()) }()
	runEncoder := json.NewEncoder(rw)
	if experiments {
		r.Experiments, err = newExperiments(out)
		if err != nil {
			return r, err
		}
		defer func() {
			for _, s := range r.Experiments {
				retErr = errors.Join(retErr, s.finish())
			}
		}()
	}
	var run rootRun
	haveRun := false
	var prev incarnation
	var prevHeight uint64
	var haveHeight bool
	flushRun := func() error {
		if !haveRun {
			return nil
		}
		r.Runs++
		r.LargestRun = max(r.LargestRun, run.End-run.Start+1)
		if e := runEncoder.Encode(run); e != nil {
			return e
		}
		if experiments {
			root, e := hex.DecodeString(run.Root)
			if e != nil {
				return e
			}
			ownerBytes, e := hex.DecodeString(run.Owner)
			if e != nil {
				return e
			}
			key := append([]byte(nil), ownerBytes[1:]...)
			key = binary.BigEndian.AppendUint64(key, run.Generation)
			key = binary.BigEndian.AppendUint64(key, run.Start)
			val := binary.BigEndian.AppendUint64(nil, run.End)
			val = append(val, root...)
			for _, compression := range []string{"snappy", "none"} {
				if e = r.Experiments["shielded-runs-"+compression].set(key, val); e != nil {
					return e
				}
			}
		}
		return nil
	}
	it := db.NewIterator(prefixes[2], nil)
	defer func() {
		retErr = errors.Join(retErr, it.Error())
		it.Release()
		retErr = errors.Join(retErr, it.Error())
	}()
	var seen uint64
	last := time.Now()
	for it.Next() {
		if err = ctx.Err(); err != nil {
			return r, err
		}
		key, raw := it.Key(), it.Value()
		// A raw trie hash cannot be a latest-KV row.
		if len(key) == common.HashLength {
			continue
		}
		framed(hashes[2], key, raw)
		r.SourceRows[2]++
		owner, generation, domain, logical, valid := rawdb.DecodeStateKVLatestKey(key)
		if !valid {
			return r, fmt.Errorf("bad KV key %x", key)
		}
		value, e := rawdb.DecodeStateKVLatestValue(raw)
		if e != nil {
			return r, e
		}
		class := "current"
		if generation < gens[owner] {
			class = "obsolete"
		} else if generation > gens[owner] {
			class = "future"
		}
		inc := incarnation{owner, generation}
		if domain == kvdomains.ContractStorage {
			add(r.Storage, class, key, raw, value)
			add(r.ValueLength, strconv.Itoa(len(value)), key, raw, value)
			z := zeros(value)
			add(r.ValueLeadingZeros, strconv.Itoa(z), key, raw, value)
			trim := len(value) - z
			r.TrimmedBytes += uint64(trim)
			kind := "wide"
			if trim == 0 {
				kind = "zero"
			} else if trim == 1 && value[z] == 1 {
				kind = "one"
			} else if trim <= 1 {
				kind = "u8-other"
			} else if trim <= 4 {
				kind = "u32"
			} else if trim <= 8 {
				kind = "u64"
			} else if trim <= 20 {
				kind = "u160"
			}
			add(r.ValueKinds, kind, key, raw, value)
			o := owners[inc]
			if o == nil {
				o = &ownerMetric{Owner: hex.EncodeToString(owner[:]), Generation: generation, Class: class}
				owners[inc] = o
			}
			o.add(key, raw, value)
			o.TrimmedBytes += uint64(trim)
			if trim == 0 {
				o.ZeroRows++
			}
			ownerSet[owner] = true
			if len(logical) != 32 || len(value) != 32 {
				return r, fmt.Errorf("non-32 storage row key/value: %d/%d", len(logical), len(value))
			}
			var prefix [16]byte
			copy(prefix[:], logical[:16])
			if o.Prefixes == 0 {
				o.Prefix = hex.EncodeToString(prefix[:])
				o.Prefixes = 1
				lastPrefix[inc] = prefix
			} else if lastPrefix[inc] != prefix {
				o.Prefixes++
				lastPrefix[inc] = prefix
			}
			if zeros(logical[16:24]) == 8 {
				o.SmallTailRows++
			}
			var line [65]byte
			hex.Encode(line[:64], value)
			line[64] = '\n'
			if _, e = vw.Write(line[:]); e != nil {
				return r, e
			}
			if experiments {
				if o.Prefixes != 1 {
					return r, errors.New("compact suffix experiment requires one prefix per incarnation")
				}
				id := owner.AccountID()
				compact := append([]byte(nil), id[:]...)
				compact = binary.BigEndian.AppendUint64(compact, generation)
				if o.Rows == 1 {
					for _, compression := range []string{"snappy", "none"} {
						if e = r.Experiments["storage-prefix-metadata-"+compression].set(compact, prefix[:]); e != nil {
							return r, e
						}
					}
				}
				compact = append(compact, logical[16:]...)
				if !bytes.Equal(value, append(make([]byte, z), value[z:]...)) {
					return r, errors.New("trim round-trip")
				}
				for _, compression := range []string{"snappy", "none"} {
					for _, model := range []string{"storage-baseline", "storage-compact-key", "storage-compact-key-trim"} {
						k, v := key, raw
						if model != "storage-baseline" {
							k = compact
						}
						if model == "storage-compact-key-trim" {
							v = value[z:]
						}
						if e = r.Experiments[model+"-"+compression].set(k, v); e != nil {
							return r, e
						}
					}
				}
			}
		} else if domain == kvdomains.SystemShielded {
			sub := shieldedKind(logical)
			add(r.Shielded, sub+"/"+class, key, raw, value)
			if sub == "block-root-index" {
				if len(value) != 32 {
					return r, errors.New("block-root value length")
				}
				height := binary.BigEndian.Uint64(logical[len(logical)-8:])
				root := hex.EncodeToString(value)
				r.RootFrequencies[root]++
				r.RootRows++
				if !haveHeight {
					r.MinHeight = height
				} else if inc == prev {
					if height <= prevHeight {
						return r, errors.New("unsorted block root heights")
					}
					r.Gaps += height - prevHeight - 1
				}
				r.MaxHeight = max(r.MaxHeight, height)
				if !haveRun || inc != prev || height != prevHeight+1 || run.Root != root {
					if e = flushRun(); e != nil {
						return r, e
					}
					run = rootRun{hex.EncodeToString(owner[:]), generation, height, height, root}
					haveRun = true
				} else {
					run.End = height
				}
				prev = inc
				prevHeight = height
				haveHeight = true
				if experiments {
					for _, compression := range []string{"snappy", "none"} {
						if e = r.Experiments["shielded-baseline-"+compression].set(key, raw); e != nil {
							return r, e
						}
					}
				}
			}
		}
		seen++
		if seen%1000000 == 0 && time.Since(last) > 20*time.Second {
			fmt.Fprintf(os.Stderr, "%s rows=%d storage_owners=%d\n", time.Now().UTC().Format(time.RFC3339), seen, len(ownerSet))
			last = time.Now()
		}
	}
	if err = it.Error(); err != nil {
		return r, err
	}
	if err = flushRun(); err != nil {
		return r, err
	}
	r.DistinctRoots = uint64(len(r.RootFrequencies))
	r.StorageOwners = uint64(len(ownerSet))
	r.Incarnations = uint64(len(owners))
	list := make([]*ownerMetric, 0, len(owners))
	for inc, o := range owners {
		data, present, e := rawdb.ReadStateKVLatest(db, inc.Owner, inc.Generation, kvdomains.ContractMetadata, []byte("meta"))
		if e != nil {
			return r, e
		}
		o.MetadataPresent = present
		var metadata contractpb.SmartContract
		if present {
			if e = statecodec.Unmarshal(data, &metadata); e != nil {
				return r, e
			}
			o.Name = metadata.Name
			o.Version = metadata.Version
		}
		seed := append([]byte(nil), inc.Owner[:]...)
		if zeros(metadata.TrxHash) != len(metadata.TrxHash) {
			seed = append(seed, metadata.TrxHash...)
		}
		expected := common.Keccak256(seed)
		o.PrefixMatchesMetadata = o.Prefixes == 1 && o.Prefix == hex.EncodeToString(expected[:16])
		list = append(list, o)
		if o.Prefixes > 1 {
			r.PrefixVariants++
		}
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Rows != list[j].Rows {
			return list[i].Rows > list[j].Rows
		}
		if list[i].Owner != list[j].Owner {
			return list[i].Owner < list[j].Owner
		}
		return list[i].Generation < list[j].Generation
	})
	f, err := os.OpenFile(filepath.Join(out, "storage-owners.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return r, err
	}
	enc := json.NewEncoder(f)
	for _, o := range list {
		if err = enc.Encode(o); err != nil {
			break
		}
	}
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return r, err
	}
	for i, h := range hashes {
		r.SourceSHA[i] = hex.EncodeToString(h.Sum(nil))
		if r.SourceRows[i] != prior.SourceRows[i] || r.SourceSHA[i] != prior.SourceSHA256[i] {
			return r, fmt.Errorf("family %d differs from authenticated baseline", i)
		}
	}
	after, ok, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil || !ok || after != head {
		return r, errors.Join(err, errors.New("HEAD changed"))
	}
	r.Authenticated = true
	r.Complete = true
	return r, nil
}

func shieldedKind(k []byte) string {
	switch {
	case len(k) == len(rawdb.ShieldedRootChangeStateKey(1)) && bytes.HasPrefix(k, rawdb.ShieldedRootChangePrefix()):
		return "root-change-point"
	case len(k) == len(rawdb.MerkleTreeIndexStateKey(0)) && bytes.Equal(k[:len(k)-8], rawdb.MerkleTreeIndexStateKey(0)[:len(k)-8]):
		return "block-root-index"
	case bytes.Equal(k, rawdb.IncrMerkleLastTreeStateKey()):
		return "last-tree"
	case bytes.Equal(k, rawdb.IncrMerkleCurrentTreeStateKey()):
		return "current-tree"
	case len(k) == len(rawdb.IncrMerkleTreeStateKey(make([]byte, 32))) && bytes.Equal(k[:len(k)-32], rawdb.IncrMerkleTreeStateKey(make([]byte, 32))[:len(k)-32]):
		return "root-tree"
	case bytes.Equal(k, rawdb.NoteCommitmentCountStateKey()):
		return "note-count"
	case len(k) == len(rawdb.NoteCommitmentStateKey(0)) && bytes.Equal(k[:len(k)-8], rawdb.NoteCommitmentStateKey(0)[:len(k)-8]):
		return "note-commitment"
	case len(k) == len(rawdb.NullifierStateKey(make([]byte, 32))) && bytes.Equal(k[:len(k)-32], rawdb.NullifierStateKey(make([]byte, 32))[:len(k)-32]):
		return "nullifier"
	case len(k) == len(rawdb.ZKProofStateKey(make([]byte, 32))) && bytes.Equal(k[:len(k)-32], rawdb.ZKProofStateKey(make([]byte, 32))[:len(k)-32]):
		return "proof-cache"
	default:
		return "unknown:" + hex.EncodeToString(k[:min(8, len(k))])
	}
}
