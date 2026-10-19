// other-storage-distribution is a read-only census of an authenticated offline
// chaindata image. Candidate SSTs are diagnostic models, not production codecs.
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
	"hash"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	"github.com/tronprotocol/go-tron/core/types"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

type rawdbKeyValue interface{ ethdb.KeyValueStore }

type metric struct {
	Rows    uint64 `json:"rows"`
	Key     uint64 `json:"key_bytes"`
	Wrapped uint64 `json:"wrapped_bytes"`
	Payload uint64 `json:"payload_bytes"`
}

func (m *metric) add(k, v, p []byte) {
	m.Rows++
	m.Key += uint64(len(k))
	m.Wrapped += uint64(len(v))
	m.Payload += uint64(len(p))
}

type numbers struct {
	metric
	Width           map[int]uint64 `json:"minimal_be_widths"`
	VarintBytes     uint64         `json:"unsigned_varint_bytes"`
	Negative        uint64         `json:"negative_rows"`
	Zero            uint64         `json:"zero_rows"`
	LogicalKeyWidth map[int]uint64 `json:"logical_key_widths"`
}
type pairing struct {
	Legacy             uint64
	V2                 uint64
	Equal              uint64
	Different          uint64
	MissingV2          uint64
	V2Only             uint64
	MissingNameMapping uint64
	MappingCollision   uint64
	MaxLegacyPerOwner  uint64
}
type accountStats struct {
	metric
	CoreBytes           uint64
	EnvelopeBytes       uint64
	AddressMatchesOwner uint64
	AddressAbsent       uint64
	AddressDiffers      uint64
	AddressElisionBytes uint64
	V5Rows              uint64
}
type nativeStats struct {
	metric
	RawZstdBytes      uint64
	SelectedZstdBytes uint64
}
type report struct {
	Complete               bool   `json:"complete"`
	Error                  string `json:"error,omitempty"`
	Started                time.Time
	Finished               time.Time
	Block                  uint64
	BlockHash              string
	TxNum                  uint64
	Root                   string
	SourceRows             [3]uint64 `json:"source_rows"`
	SourceSHA              [3]string `json:"source_sha256"`
	Authenticated          bool      `json:"matches_authenticated_baseline"`
	Domains                map[string]*metric
	Numbers                map[string]*numbers
	Assets                 pairing
	Accounts               accountStats
	Native                 map[string]*nativeStats
	MetadataABIRows        uint64
	MetadataABIBytes       uint64
	SeparateABIRows        uint64
	SeparateABIBytes       uint64
	SameABIRows            uint64
	DifferentABIRows       uint64
	MissingSeparateABIRows uint64
	Experiments            map[string]*tableSeries `json:"sst_experiments,omitempty"`
}

func framed(h hash.Hash, k, v []byte) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(len(k)))
	h.Write(b[:])
	h.Write(k)
	binary.BigEndian.PutUint64(b[:], uint64(len(v)))
	h.Write(b[:])
	h.Write(v)
}
func leading(v []byte) int {
	i := 0
	for i < len(v) && v[i] == 0 {
		i++
	}
	return i
}
func uintSize(n uint64) int { var b [10]byte; return binary.PutUvarint(b[:], n) }
func isNumeric(d kvdomains.KVDomain) bool {
	return d >= kvdomains.AccountAsset && d <= kvdomains.AccountAssetOperationTimeV2
}

// intModel retains exact fixed-eight-byte logical semantics; even zero is a
// present row. It models a new tag for minimal BE words with >=1 byte saved.
func intModel(v []byte) []byte {
	z := leading(v)
	if z > 0 {
		return append([]byte{3}, v[z:]...)
	}
	return append([]byte{1}, v...)
}
func postV2Key(k []byte, prefixLen int) []byte { return append([]byte("kv3-"), k[prefixLen:]...) }
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	dir := flag.String("db-dir", "", "immutable offline chaindata")
	baseline := flag.String("baseline", "", "authenticated generation census")
	out := flag.String("out-dir", "", "exclusive new output directory")
	codeOnly := flag.Bool("code-only", false, "inspect content-addressed bytecode separately from rooted rows")
	flag.Parse()
	if *dir == "" || *baseline == "" || *out == "" || flag.NArg() != 0 {
		return errors.New("db-dir, baseline, out-dir required")
	}
	dbpath, err := filepath.EvalSymlinks(*dir)
	if err != nil {
		return err
	}
	dbpath, err = filepath.Abs(dbpath)
	if err != nil {
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
	rel, err := filepath.Rel(dbpath, dest)
	if err != nil || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("output inside source")
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
	if err = os.Mkdir(dest, 0700); err != nil {
		return err
	}
	db, err := rawdb.NewPebbleDBReadOnly(dbpath, 64, 128)
	if err == nil && *codeOnly {
		return errors.Join(inspectCode(db, prior, dest), db.Close())
	}
	var r report
	if err == nil {
		r, err = inspect(db, prior, dest)
		err = errors.Join(err, db.Close())
	}
	if err != nil {
		r.Complete = false
		r.Error = err.Error()
	}
	r.Finished = time.Now().UTC()
	f, e := os.OpenFile(filepath.Join(dest, "distribution.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return errors.Join(err, e)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return errors.Join(err, enc.Encode(r), f.Sync(), f.Close())
}
func inspect(db rawdbKeyValue, prior rawdb.GenerationCensus, out string) (r report, retErr error) {
	r.Started = time.Now().UTC()
	r.Domains = map[string]*metric{}
	r.Numbers = map[string]*numbers{}
	r.Native = map[string]*nativeStats{}
	head, ok, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil || !ok {
		return r, errors.Join(err, errors.New("missing HEAD"))
	}
	if head.BlockNum != prior.BlockNum || head.Root != prior.Root || head.BlockHash != prior.BlockHash || head.TxNum != prior.TxNum {
		return r, errors.New("HEAD mismatch")
	}
	r.Block = head.BlockNum
	r.BlockHash = hex.EncodeToString(head.BlockHash[:])
	r.Root = hex.EncodeToString(head.Root[:])
	r.TxNum = head.TxNum
	s, err := state.New(head.Root, state.NewDatabase(rawdb.WrapKeyValueStore(db)))
	if err != nil {
		return r, err
	}
	r.Experiments, err = newExperiments(out)
	if err != nil {
		return r, err
	}
	defer func() {
		for _, v := range r.Experiments {
			retErr = errors.Join(retErr, v.finish())
		}
	}()
	zstdEnc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(true))
	if err != nil {
		return r, err
	}
	defer zstdEnc.Close()
	prefixes := rawdb.StateCommitmentSourcePrefixes()
	hs := [3]hash.Hash{sha256.New(), sha256.New(), sha256.New()}
	gens := map[common.Address]uint64{}
	setModels := func(a, b string, k, v, w []byte) error {
		for _, c := range []string{"snappy", "none"} {
			if e := r.Experiments[a+"-"+c].set(k, v); e != nil {
				return e
			}
			if e := r.Experiments[b+"-"+c].set(k, w); e != nil {
				return e
			}
		}
		return nil
	}
	for fam := 0; fam < 2; fam++ {
		it := db.NewIterator(prefixes[fam], nil)
		for it.Next() {
			k, v := it.Key(), it.Value()
			framed(hs[fam], k, v)
			r.SourceRows[fam]++
			if fam == 1 {
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
			} else {
				env, e := state.DecodeStateAccountV3(v)
				if e != nil {
					err = e
					break
				}
				acc, e := types.UnmarshalAccountStorageCoreV4(env.AccountProto)
				if e != nil {
					err = e
					break
				}
				r.Accounts.add(k, v, env.AccountProto)
				r.Accounts.CoreBytes += uint64(len(env.AccountProto))
				r.Accounts.EnvelopeBytes += uint64(len(v) - len(env.AccountProto))
				if env.Version == 5 {
					r.Accounts.V5Rows++
				}
				candidate := v
				address := acc.Proto().Address
				if len(address) == 0 {
					r.Accounts.AddressAbsent++
				} else if len(address) == common.AddressLength && address[0] == common.AddressPrefixMainnet && bytes.Equal(address[1:], k[len(prefixes[0]):]) {
					r.Accounts.AddressMatchesOwner++
					acc.Proto().Address = nil
					core, e := acc.MarshalStorageCoreV4()
					if e != nil {
						err = e
						break
					}
					env.AccountProto = core
					candidate, e = env.Encode()
					if e != nil {
						err = e
						break
					}
					r.Accounts.AddressElisionBytes += uint64(len(v) - len(candidate))
				} else {
					r.Accounts.AddressDiffers++
				}
				if e := setModels("account-baseline", "account-address-elided", k, v, candidate); e != nil {
					err = e
					break
				}
			}
		}
		err = errors.Join(err, it.Error())
		it.Release()
		if err != nil {
			return r, err
		}
	}
	type nameResult struct {
		id    int64
		found bool
	}
	names := map[string]nameResult{}
	type leg struct {
		value   uint64
		matched bool
	}
	legacy := map[int64]*leg{}
	var ownerPrev common.Address
	var genPrev uint64
	haveOwner := false
	flush := func() {
		for _, v := range legacy {
			if !v.matched {
				r.Assets.MissingV2++
			}
		}
		r.Assets.MaxLegacyPerOwner = max(r.Assets.MaxLegacyPerOwner, uint64(len(legacy)))
		clear(legacy)
	}
	type abiKey struct {
		owner common.Address
		gen   uint64
	}
	metadata := map[abiKey]*contractpb.SmartContract_ABI{}
	separate := map[abiKey]*contractpb.SmartContract_ABI{}
	it := db.NewIterator(prefixes[2], nil)
	defer func() { retErr = errors.Join(retErr, it.Error()); it.Release() }()
	last := time.Now()
	for it.Next() {
		k, raw := it.Key(), it.Value()
		if len(k) == common.HashLength {
			continue
		}
		framed(hs[2], k, raw)
		r.SourceRows[2]++
		owner, gen, domain, key, valid := rawdb.DecodeStateKVLatestKey(k)
		if !valid {
			return r, errors.New("bad KV key")
		}
		v, e := rawdb.DecodeStateKVLatestValue(raw)
		if e != nil {
			return r, e
		}
		class := "current"
		if gen < gens[owner] {
			class = "obsolete"
		} else if gen > gens[owner] {
			return r, errors.New("future generation")
		}
		name := kvdomains.Name(domain) + "/" + class
		if r.Domains[name] == nil {
			r.Domains[name] = new(metric)
		}
		r.Domains[name].add(k, raw, v)
		if !haveOwner || owner != ownerPrev || gen != genPrev {
			flush()
			ownerPrev = owner
			genPrev = gen
			haveOwner = true
		}
		if isNumeric(domain) {
			if len(v) != 8 {
				return r, fmt.Errorf("numeric width %s: %d", name, len(v))
			}
			n := binary.BigEndian.Uint64(v)
			m := r.Numbers[name]
			if m == nil {
				m = &numbers{Width: map[int]uint64{}, LogicalKeyWidth: map[int]uint64{}}
				r.Numbers[name] = m
			}
			m.add(k, raw, v)
			m.Width[8-leading(v)]++
			m.VarintBytes += uint64(uintSize(n))
			m.LogicalKeyWidth[len(key)]++
			if n == 0 {
				m.Zero++
			}
			if int64(n) < 0 {
				m.Negative++
			}
			modelKey := postV2Key(k, len(prefixes[2]))
			if e = setModels("int64-post-v2", "int64-trim", modelKey, append([]byte{1}, v...), intModel(v)); e != nil {
				return r, e
			}
			if class == "current" && domain == kvdomains.AccountAsset {
				r.Assets.Legacy++
				nr, seen := names[string(key)]
				if !seen {
					id, found, e := s.ReadAssetIssueIDByName(key)
					if e != nil {
						return r, e
					}
					nr = nameResult{id, found}
					names[string(key)] = nr
				}
				if !nr.found {
					r.Assets.MissingNameMapping++
				} else if _, dup := legacy[nr.id]; dup {
					r.Assets.MappingCollision++
				} else {
					legacy[nr.id] = &leg{value: n}
				}
			} else if class == "current" && domain == kvdomains.AccountAssetV2 {
				r.Assets.V2++
				id, e := strconv.ParseInt(string(key), 10, 64)
				if e != nil {
					return r, e
				}
				if l := legacy[id]; l != nil {
					l.matched = true
					if l.value == n {
						r.Assets.Equal++
					} else {
						r.Assets.Different++
					}
				} else {
					r.Assets.V2Only++
				}
			}
		}
		if statecodec.IsNative(v) {
			m := r.Native[name]
			if m == nil {
				m = new(nativeStats)
				r.Native[name] = m
			}
			m.add(k, raw, v)
			compressed := zstdEnc.EncodeAll(v, nil)
			m.RawZstdBytes += uint64(len(compressed))
			cv := append([]byte{1}, v...)
			if len(compressed) < len(v) {
				cv = append([]byte{4}, compressed...)
			}
			m.SelectedZstdBytes += uint64(len(cv))
			modelKey := postV2Key(k, len(prefixes[2]))
			if e = setModels("native-baseline", "native-zstd", modelKey, append([]byte{1}, v...), cv); e != nil {
				return r, e
			}
		}
		if domain == kvdomains.ContractMetadata {
			c := new(contractpb.SmartContract)
			if e = statecodec.Unmarshal(v, c); e != nil {
				return r, e
			}
			if c.Abi != nil {
				r.MetadataABIRows++
				b, e := statecodec.Marshal(c.Abi)
				if e != nil {
					return r, e
				}
				r.MetadataABIBytes += uint64(len(b))
				metadata[abiKey{owner, gen}] = c.Abi
			}
		}
		if domain == kvdomains.ContractABI {
			a := new(contractpb.SmartContract_ABI)
			if e = statecodec.Unmarshal(v, a); e != nil {
				return r, e
			}
			r.SeparateABIRows++
			r.SeparateABIBytes += uint64(len(v))
			separate[abiKey{owner, gen}] = a
		}
		if time.Since(last) > 30*time.Second {
			fmt.Fprintf(os.Stderr, "rows %d elapsed %s\n", r.SourceRows[2], time.Since(r.Started))
			last = time.Now()
		}
	}
	flush()
	if e := it.Error(); e != nil {
		return r, e
	}
	for k, a := range metadata {
		if b, ok := separate[k]; !ok {
			r.MissingSeparateABIRows++
		} else if proto.Equal(a, b) {
			r.SameABIRows++
		} else {
			r.DifferentABIRows++
		}
	}
	if r.SourceRows != prior.SourceRows {
		return r, errors.New("source row mismatch")
	}
	for i := range hs {
		r.SourceSHA[i] = hex.EncodeToString(hs[i].Sum(nil))
		if r.SourceSHA[i] != prior.SourceSHA256[i] {
			return r, fmt.Errorf("source SHA mismatch family %d", i)
		}
	}
	r.Authenticated = true
	r.Complete = true
	return r, nil
}
