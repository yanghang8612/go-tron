package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// GenerationBytes describes visible logical rows, not compressed SST ownership.
type GenerationBytes struct {
	Rows         uint64 `json:"rows"`
	KeyBytes     uint64 `json:"physical_key_bytes"`
	ValueBytes   uint64 `json:"wrapped_value_bytes"`
	PayloadBytes uint64 `json:"payload_bytes"`
	LogicalBytes uint64 `json:"logical_bytes"`
	MaxPayload   uint64 `json:"max_payload_bytes"`
}

func (s *GenerationBytes) add(key, raw []byte) {
	s.Rows++
	s.KeyBytes += uint64(len(key))
	s.ValueBytes += uint64(len(raw))
	s.PayloadBytes += uint64(len(raw) - 1)
	s.LogicalBytes += uint64(len(key) + len(raw))
	s.MaxPayload = max(s.MaxPayload, uint64(len(raw)-1))
}

type GenerationOwner struct {
	Owner          common.Address  `json:"owner"`
	Current        uint64          `json:"current_generation"`
	Explicit       bool            `json:"explicit_generation"`
	AccountPresent bool            `json:"account_present"`
	Obsolete       GenerationBytes `json:"obsolete"`
	Future         GenerationBytes `json:"future"`
}

// GenerationGroup is one contiguous (owner, generation, domain) group. The
// optional sink can stream every inactive group without retaining all owners.
type GenerationGroup struct {
	Owner   common.Address     `json:"owner"`
	Current uint64             `json:"current_generation"`
	Stored  uint64             `json:"stored_generation"`
	Domain  kvdomains.KVDomain `json:"domain"`
	Class   string             `json:"class"`
	GenerationBytes
}

type GenerationCensus struct {
	Complete                     bool                                   `json:"complete"`
	Error                        string                                 `json:"error,omitempty"`
	Started                      time.Time                              `json:"started_utc"`
	Finished                     time.Time                              `json:"finished_utc"`
	BlockNum                     uint64                                 `json:"block_num"`
	BlockHash                    common.Hash                            `json:"block_hash"`
	TxNum                        uint64                                 `json:"tx_num"`
	Root                         common.Hash                            `json:"muhash_root"`
	MuHashVerified               bool                                   `json:"muhash_verified"`
	SourceRows                   [3]uint64                              `json:"source_rows"`
	SourceLogicalBytes           [3]uint64                              `json:"source_logical_bytes"`
	SourceSHA256                 [3]string                              `json:"source_sha256"`
	ClassSHA256                  map[string]string                      `json:"class_sha256"`
	Total                        GenerationBytes                        `json:"total_kv"`
	Classes                      map[string]*GenerationBytes            `json:"classes"`
	Domains                      map[string]map[string]*GenerationBytes `json:"domains"`
	PayloadSizes                 map[string]map[string]*GenerationBytes `json:"payload_sizes"`
	StoredGenerations            map[string]map[string]*GenerationBytes `json:"stored_generation_buckets"`
	Owners                       uint64                                 `json:"owners_with_kv"`
	OwnersWithObsolete           uint64                                 `json:"owners_with_obsolete"`
	OwnersWithFuture             uint64                                 `json:"owners_with_future"`
	OwnersWithoutGeneration      uint64                                 `json:"owners_without_explicit_generation"`
	InactiveOwnersWithoutAccount uint64                                 `json:"inactive_owners_without_account"`
	TopObsoleteOwners            []GenerationOwner                      `json:"top_obsolete_owners"`
}

func generationBucket(n uint64) string {
	switch {
	case n == 0:
		return "0"
	case n == 1:
		return "1"
	case n < 4:
		return "2..3"
	case n < 16:
		return "4..15"
	case n < 256:
		return "16..255"
	default:
		return "256+"
	}
}
func payloadBucket(n int) string {
	switch {
	case n == 0:
		return "0"
	case n <= 32:
		return "1..32"
	case n <= 128:
		return "33..128"
	case n <= 1024:
		return "129..1024"
	case n <= 4096:
		return "1025..4096"
	case n <= 65536:
		return "4097..65536"
	case n <= 1048576:
		return "65537..1048576"
	default:
		return "1048577+"
	}
}
func generationMetric(m map[string]*GenerationBytes, k string) *GenerationBytes {
	if m[k] == nil {
		m[k] = new(GenerationBytes)
	}
	return m[k]
}
func generationDimension(m map[string]map[string]*GenerationBytes, dimension, class string, key, value []byte) {
	if m[dimension] == nil {
		m[dimension] = make(map[string]*GenerationBytes)
	}
	generationMetric(m[dimension], class).add(key, value)
}
func hashCensusRow(h hash.Hash, key, value []byte) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(len(key)))
	h.Write(b[:])
	h.Write(key)
	binary.BigEndian.PutUint64(b[:], uint64(len(value)))
	h.Write(b[:])
	h.Write(value)
}

// InspectStateKVGenerations requires an immutable/offline view supplied by the
// caller. Its API has no writes. A streamed merge joins KV owners to the full
// generation table with O(1) cursor state. Absent generation means zero;
// stored<current is obsolete, stored>current is an anomaly, never GC authority.
// All three physical source families are exhaustively hashed and optionally
// recomputed into the exact normalized MuHash HEAD. No sampling is performed.
func InspectStateKVGenerations(ctx context.Context, db interface {
	ethdb.KeyValueReader
	ethdb.Iteratee
}, verify bool, sink func(GenerationGroup) error, progress func(string, uint64)) (r GenerationCensus, retErr error) {
	r.Started = time.Now().UTC()
	r.Classes = make(map[string]*GenerationBytes)
	r.Domains = make(map[string]map[string]*GenerationBytes)
	r.PayloadSizes = make(map[string]map[string]*GenerationBytes)
	r.StoredGenerations = make(map[string]map[string]*GenerationBytes)
	for _, k := range []string{"current", "obsolete", "future"} {
		r.Classes[k] = new(GenerationBytes)
	}
	defer func() {
		r.Finished = time.Now().UTC()
		r.Complete = retErr == nil
		if retErr != nil {
			r.Error = retErr.Error()
		}
	}()
	if ctx == nil || db == nil {
		return r, errors.New("generation census requires context and immutable reader")
	}
	if err := ctx.Err(); err != nil {
		return r, err
	}
	if pending, err := StateCommitmentRestoreInProgress(db); err != nil {
		return r, err
	} else if pending {
		return r, errors.New("generation census: unpublished restore")
	}
	if pending, err := StateCommitmentUnwindCleanupPending(db); err != nil {
		return r, err
	} else if pending {
		return r, errors.New("generation census: unfinished unwind")
	}
	head, present, err := ReadStateCommitmentHead(db)
	if err != nil {
		return r, err
	}
	if !present {
		return r, ErrStateCommitmentHead
	}
	r.BlockNum, r.BlockHash, r.TxNum, r.Root = head.BlockNum, head.BlockHash, head.TxNum, head.Root
	root, ok, err := ReadLatestDomainCommitmentRoot(db)
	if err != nil {
		return r, err
	}
	if !ok || root != head.Root {
		return r, errors.New("generation census: HEAD/root mismatch")
	}
	var acc *muhash3072.Accumulator
	if verify {
		acc = muhash3072.NewAccumulator()
	}
	hashes := [3]hash.Hash{sha256.New(), sha256.New(), sha256.New()}
	classHashes := map[string]hash.Hash{"current": sha256.New(), "obsolete": sha256.New(), "future": sha256.New()}
	observe := func(family int, key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.SourceRows[family]++
		r.SourceLogicalBytes[family] += uint64(len(key) + len(value))
		hashCensusRow(hashes[family], key, value)
		if acc != nil {
			e, err := EncodeStateCommitmentElement(key, value)
			if err != nil {
				return err
			}
			if err = acc.Insert(e); err != nil {
				return err
			}
		}
		if progress != nil && r.SourceRows[family]%100000 == 0 {
			progress(string(latestDomainCommitmentSources[family].name), r.SourceRows[family])
		}
		return nil
	}
	// Accounts are a separate sequential pass; they participate in the root.
	ai := db.NewIterator(stateAccountLatestPrefix, nil)
	for ai.Next() {
		if _, err = StateCommitmentElementFamily(ai.Key()); err == nil {
			err = observe(0, ai.Key(), ai.Value())
		}
		if err != nil {
			break
		}
	}
	err = errors.Join(err, ai.Error())
	ai.Release()
	err = errors.Join(err, ai.Error())
	if err != nil {
		return r, err
	}
	gi := db.NewIterator(stateKVGenerationPrefix, nil)
	ki := db.NewIterator(stateKVLatestPrefix, nil)
	defer func() {
		retErr = errors.Join(retErr, gi.Error(), ki.Error())
		gi.Release()
		ki.Release()
		retErr = errors.Join(retErr, gi.Error(), ki.Error())
	}()
	var genOwner common.Address
	var gen uint64
	genOK := false
	advanceGen := func() error {
		genOK = gi.Next()
		if !genOK {
			return gi.Error()
		}
		var valid bool
		genOwner, valid = DecodeStateKVGenerationKey(gi.Key())
		if !valid {
			return fmt.Errorf("generation census: malformed generation key %x", gi.Key())
		}
		gen, err = DecodeStateKVGenerationValue(gi.Value())
		if err != nil {
			return err
		}
		return observe(1, gi.Key(), gi.Value())
	}
	if err = advanceGen(); err != nil {
		return r, err
	}
	var owner GenerationOwner
	haveOwner := false
	var group GenerationGroup
	haveGroup := false
	flushGroup := func() error {
		if haveGroup && group.Class != "current" && sink != nil {
			return sink(group)
		}
		return nil
	}
	flushOwner := func() error {
		if !haveOwner {
			return nil
		}
		if !owner.Explicit {
			r.OwnersWithoutGeneration++
		}
		if owner.Obsolete.Rows == 0 && owner.Future.Rows == 0 {
			return nil
		}
		_, owner.AccountPresent, err = ReadStateAccountLatest(db, owner.Owner)
		if err != nil {
			return err
		}
		if !owner.AccountPresent {
			r.InactiveOwnersWithoutAccount++
		}
		if owner.Future.Rows > 0 {
			r.OwnersWithFuture++
		}
		if owner.Obsolete.Rows > 0 {
			r.OwnersWithObsolete++
			r.TopObsoleteOwners = append(r.TopObsoleteOwners, owner)
			sort.Slice(r.TopObsoleteOwners, func(i, j int) bool {
				a, b := r.TopObsoleteOwners[i], r.TopObsoleteOwners[j]
				if a.Obsolete.LogicalBytes != b.Obsolete.LogicalBytes {
					return a.Obsolete.LogicalBytes > b.Obsolete.LogicalBytes
				}
				return bytes.Compare(a.Owner[:], b.Owner[:]) < 0
			})
			if len(r.TopObsoleteOwners) > 32 {
				r.TopObsoleteOwners = r.TopObsoleteOwners[:32]
			}
		}
		return nil
	}
	for ki.Next() {
		key, raw := ki.Key(), ki.Value()
		kvOwner, stored, domain, _, valid := DecodeStateKVLatestKey(key)
		if !valid {
			return r, fmt.Errorf("generation census: malformed/unregistered KV key %x", key)
		}
		if _, err = DecodeStateKVLatestValue(raw); err != nil {
			return r, err
		}
		if !haveOwner || owner.Owner != kvOwner {
			if haveOwner && bytes.Compare(kvOwner[:], owner.Owner[:]) <= 0 {
				return r, errors.New("generation census: unsorted owners")
			}
			if err = flushOwner(); err != nil {
				return r, err
			}
			for genOK && bytes.Compare(genOwner[:], kvOwner[:]) < 0 {
				if err = advanceGen(); err != nil {
					return r, err
				}
			}
			owner = GenerationOwner{Owner: kvOwner, Explicit: genOK && genOwner == kvOwner}
			if owner.Explicit {
				owner.Current = gen
			}
			r.Owners++
			haveOwner = true
		}
		class := "current"
		if stored < owner.Current {
			class = "obsolete"
			owner.Obsolete.add(key, raw)
		} else if stored > owner.Current {
			class = "future"
			owner.Future.add(key, raw)
		}
		if !haveGroup || group.Owner != kvOwner || group.Stored != stored || group.Domain != domain {
			if err = flushGroup(); err != nil {
				return r, err
			}
			group = GenerationGroup{Owner: kvOwner, Current: owner.Current, Stored: stored, Domain: domain, Class: class}
			haveGroup = true
		}
		group.add(key, raw)
		r.Total.add(key, raw)
		r.Classes[class].add(key, raw)
		hashCensusRow(classHashes[class], key, raw)
		generationDimension(r.Domains, kvdomains.Name(domain), class, key, raw)
		generationDimension(r.PayloadSizes, payloadBucket(len(raw)-1), class, key, raw)
		generationDimension(r.StoredGenerations, generationBucket(stored), class, key, raw)
		if err = observe(2, key, raw); err != nil {
			return r, err
		}
	}
	if err = errors.Join(ki.Error(), flushGroup(), flushOwner()); err != nil {
		return r, err
	}
	for genOK {
		if err = advanceGen(); err != nil {
			return r, err
		}
	}
	for i, h := range hashes {
		r.SourceSHA256[i] = hex.EncodeToString(h.Sum(nil))
	}
	r.ClassSHA256 = make(map[string]string)
	for k, h := range classHashes {
		r.ClassSHA256[k] = hex.EncodeToString(h.Sum(nil))
	}
	if acc != nil {
		final, err := acc.Finalize()
		if err != nil {
			return r, err
		}
		if final.State != head.State || common.Hash(final.Digest) != head.Root {
			return r, errors.New("generation census: full physical source differs from MuHash HEAD")
		}
		r.MuHashVerified = true
	}
	after, present, err := ReadStateCommitmentHead(db)
	if err != nil {
		return r, err
	}
	if !present || after != head {
		return r, errors.New("generation census: HEAD changed during scan")
	}
	return r, nil
}
