package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const LatestSegmentVersion = 1

type LatestEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type LatestSegment struct {
	Version   uint32             `json:"version"`
	Dataset   SegmentDataset     `json:"dataset,omitempty"`
	Domain    kvdomains.KVDomain `json:"domain,omitempty"`
	FromTxNum uint64             `json:"fromTxNum"`
	ToTxNum   uint64             `json:"toTxNum"`
	Entries   []LatestEntry      `json:"entries"`
}

type Manager struct {
	dir      string
	manifest *Manifest
	cache    map[string]*LatestSegment
}

func AccountSnapshotKey(owner common.Address) []byte {
	id := owner.AccountID()
	return append([]byte(nil), id[:]...)
}

func AccountKVSnapshotKey(owner common.Address, generation uint64, logicalKey []byte) []byte {
	out := make([]byte, 0, common.AccountIDLength+8+len(logicalKey))
	out = append(out, AccountSnapshotKey(owner)...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], generation)
	out = append(out, buf[:]...)
	return append(out, logicalKey...)
}

func KVGenerationSnapshotKey(owner common.Address) []byte {
	return AccountSnapshotKey(owner)
}

func CommitmentSnapshotKey(logicalKey []byte) []byte {
	return append([]byte(nil), logicalKey...)
}

func CodeSnapshotKey(hash common.Hash) []byte {
	return append([]byte(nil), hash.Bytes()...)
}

func BuildAccountLatestSegmentFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildAccountLatestSegmentFromStore(newRawDBLatestHotBuildStore(db), dir, fromTxNum, toTxNum, relPath)
}

func buildAccountLatestSegmentFromStore(store latestHotStore, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	return writeLatestSegmentFromIterator(dir, SegmentRef{
		Dataset:   SegmentDatasetAccountLatest,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return store.IterateAccountLatest(func(owner common.Address, value []byte) (bool, error) {
			if err := yield(LatestEntry{Key: AccountSnapshotKey(owner), Value: value}); err != nil {
				return false, err
			}
			return true, nil
		})
	})
}

func BuildAccountLatestSegmentFilesFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildAccountLatestSegmentFilesFromStore(newRawDBLatestHotBuildStore(db), dir, fromTxNum, toTxNum, relPath)
}

func buildAccountLatestSegmentFilesFromStore(store latestHotStore, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	return writeLatestBinarySegmentAndAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetAccountLatest,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return store.IterateAccountLatest(func(owner common.Address, value []byte) (bool, error) {
			if err := yield(LatestEntry{Key: AccountSnapshotKey(owner), Value: value}); err != nil {
				return false, err
			}
			return true, nil
		})
	})
}

func BuildLatestDomainSegmentFromDB(db ethdb.Iteratee, dir string, domain kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildLatestDomainSegmentFromStore(newRawDBLatestHotBuildStore(db), dir, domain, fromTxNum, toTxNum, relPath)
}

func buildLatestDomainSegmentFromStore(store latestHotStore, dir string, domain kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	return writeLatestSegmentFromIterator(dir, SegmentRef{
		Dataset:   SegmentDatasetKVLatest,
		Domain:    domain,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return store.IterateKVLatestDomain(domain, func(owner common.Address, generation uint64, key, value []byte) (bool, error) {
			if err := yield(LatestEntry{Key: AccountKVSnapshotKey(owner, generation, key), Value: value}); err != nil {
				return false, err
			}
			return true, nil
		})
	})
}

func BuildLatestDomainSegmentFilesFromDB(db ethdb.Iteratee, dir string, domain kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildLatestDomainSegmentFilesFromStore(newRawDBLatestHotBuildStore(db), dir, domain, fromTxNum, toTxNum, relPath)
}

func buildLatestDomainSegmentFilesFromStore(store latestHotStore, dir string, domain kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	return writeLatestBinarySegmentAndAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetKVLatest,
		Domain:    domain,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return store.IterateKVLatestDomain(domain, func(owner common.Address, generation uint64, key, value []byte) (bool, error) {
			if err := yield(LatestEntry{Key: AccountKVSnapshotKey(owner, generation, key), Value: value}); err != nil {
				return false, err
			}
			return true, nil
		})
	})
}

func BuildKVGenerationSegmentFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildKVGenerationSegmentFromStore(newRawDBLatestHotBuildStore(db), dir, fromTxNum, toTxNum, relPath)
}

func buildKVGenerationSegmentFromStore(store latestHotStore, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	return writeLatestSegmentFromIterator(dir, SegmentRef{
		Dataset:   SegmentDatasetKVGeneration,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return store.IterateKVGeneration(func(owner common.Address, generation uint64) (bool, error) {
			if err := yield(LatestEntry{Key: KVGenerationSnapshotKey(owner), Value: rawdb.EncodeStateKVGenerationValue(generation)}); err != nil {
				return false, err
			}
			return true, nil
		})
	})
}

func BuildKVGenerationSegmentFilesFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildKVGenerationSegmentFilesFromStore(newRawDBLatestHotBuildStore(db), dir, fromTxNum, toTxNum, relPath)
}

func buildKVGenerationSegmentFilesFromStore(store latestHotStore, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	return writeLatestBinarySegmentAndAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetKVGeneration,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return store.IterateKVGeneration(func(owner common.Address, generation uint64) (bool, error) {
			if err := yield(LatestEntry{Key: KVGenerationSnapshotKey(owner), Value: rawdb.EncodeStateKVGenerationValue(generation)}); err != nil {
				return false, err
			}
			return true, nil
		})
	})
}

func BuildCodeSegmentFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildCodeSegmentFromStore(newRawDBLatestHotBuildStore(db), dir, fromTxNum, toTxNum, relPath)
}

func buildCodeSegmentFromStore(store latestHotStore, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	return writeLatestSegmentFromIterator(dir, SegmentRef{
		Dataset:   SegmentDatasetCode,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return store.IterateCode(func(hash common.Hash, code []byte) (bool, error) {
			if err := yield(LatestEntry{Key: CodeSnapshotKey(hash), Value: code}); err != nil {
				return false, err
			}
			return true, nil
		})
	})
}

func BuildCodeSegmentFilesFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildCodeSegmentFilesFromStore(newRawDBLatestHotBuildStore(db), dir, fromTxNum, toTxNum, relPath)
}

func buildCodeSegmentFilesFromStore(store latestHotStore, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	return writeLatestBinarySegmentAndAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetCode,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return store.IterateCode(func(hash common.Hash, code []byte) (bool, error) {
			if err := yield(LatestEntry{Key: CodeSnapshotKey(hash), Value: code}); err != nil {
				return false, err
			}
			return true, nil
		})
	})
}

func BuildCommitmentRootSegmentFromDB(db ethdb.KeyValueReader, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildCommitmentRootSegmentFromStore(newRawDBLatestHotReadStore(db), dir, fromTxNum, toTxNum, relPath)
}

func buildCommitmentRootSegmentFromStore(store latestHotStore, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	root, ok, err := store.ReadCommitmentRoot()
	if err != nil {
		return SegmentRef{}, err
	}
	if !ok {
		return SegmentRef{}, errors.New("snapshots: missing latest commitment root")
	}
	return writeLatestSegmentFromIterator(dir, SegmentRef{
		Dataset:   SegmentDatasetCommitmentRoot,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return yield(LatestEntry{Key: rawdb.LatestDomainCommitmentRootLogicalKey(), Value: root.Bytes()})
	})
}

func BuildCommitmentRootSegmentFilesFromDB(db ethdb.KeyValueReader, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildCommitmentRootSegmentFilesFromStore(newRawDBLatestHotReadStore(db), dir, fromTxNum, toTxNum, relPath)
}

func buildCommitmentRootSegmentFilesFromStore(store latestHotStore, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	root, ok, err := store.ReadCommitmentRoot()
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if !ok {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: missing latest commitment root")
	}
	return writeLatestBinarySegmentAndAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetCommitmentRoot,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return yield(LatestEntry{Key: rawdb.LatestDomainCommitmentRootLogicalKey(), Value: root.Bytes()})
	})
}

func BuildCommitmentNodeSegmentFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildCommitmentDomainSegmentFromStore(newRawDBLatestHotBuildStore(db), SegmentDatasetCommitmentNode, rawdb.LatestDomainCommitmentNodeLogicalPrefix(), dir, fromTxNum, toTxNum, relPath)
}

func buildCommitmentDomainSegmentFromStore(store latestHotStore, dataset SegmentDataset, logicalPrefix []byte, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	return writeLatestSegmentFromIterator(dir, SegmentRef{
		Dataset:   dataset,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return store.IterateCommitmentDomain(logicalPrefix, func(logicalKey, value []byte) (bool, error) {
			if err := yield(LatestEntry{Key: CommitmentSnapshotKey(logicalKey), Value: value}); err != nil {
				return false, err
			}
			return true, nil
		})
	})
}

func BuildCommitmentNodeSegmentFilesFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildCommitmentDomainSegmentFilesFromStore(newRawDBLatestHotBuildStore(db), SegmentDatasetCommitmentNode, rawdb.LatestDomainCommitmentNodeLogicalPrefix(), dir, fromTxNum, toTxNum, relPath)
}

func buildCommitmentDomainSegmentFilesFromStore(store latestHotStore, dataset SegmentDataset, logicalPrefix []byte, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	return writeLatestBinarySegmentAndAccessor(dir, SegmentRef{
		Dataset:   dataset,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		return store.IterateCommitmentDomain(logicalPrefix, func(logicalKey, value []byte) (bool, error) {
			if err := yield(LatestEntry{Key: CommitmentSnapshotKey(logicalKey), Value: value}); err != nil {
				return false, err
			}
			return true, nil
		})
	})
}

func BuildCommitmentCheckpointSegmentFilesFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil database")
	}
	return buildCommitmentCheckpointSegmentFilesFromStore(newRawDBLatestHotBuildStore(db), dir, fromTxNum, toTxNum, relPath)
}

func buildCommitmentCheckpointSegmentFilesFromStore(store latestHotStore, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, SegmentRef, SegmentRef, error) {
	if store == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil latest hot store")
	}
	return writeLatestBinarySegmentAndAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetCommitmentCheckpoint,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}, func(yield func(LatestEntry) error) error {
		var latestBlock uint64
		var latestValue []byte
		err := store.IterateCommitmentDomain(rawdb.StateCommitmentCheckpointLogicalPrefix(), func(logicalKey, value []byte) (bool, error) {
			checkpoint, err := rawdb.DecodeStateCommitmentCheckpointValue(value)
			if err != nil {
				return false, fmt.Errorf("snapshots: decode commitment checkpoint %x: %w", logicalKey, err)
			}
			if latestValue == nil || checkpoint.BlockNum > latestBlock {
				latestBlock = checkpoint.BlockNum
				latestValue = append([]byte(nil), value...)
			}
			if err := yield(LatestEntry{Key: CommitmentSnapshotKey(logicalKey), Value: value}); err != nil {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		if latestValue == nil {
			return nil
		}
		return yield(LatestEntry{Key: rawdb.LatestStateCommitmentCheckpointLogicalKey(), Value: latestValue})
	})
}

func WriteLatestSegment(dir string, ref SegmentRef, entries []LatestEntry) (SegmentRef, error) {
	if ref.Kind == "" {
		ref.Kind = SegmentLatest
	}
	if ref.Dataset == "" && ref.Kind == SegmentLatest {
		ref.Dataset = SegmentDatasetKVLatest
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return SegmentRef{}, err
	}
	seg := &LatestSegment{
		Version:   LatestSegmentVersion,
		Dataset:   ref.normalizedDataset(),
		Domain:    ref.Domain,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Entries:   normalizeLatestEntries(entries),
	}
	if err := seg.Validate(); err != nil {
		return SegmentRef{}, err
	}
	if isLatestBinarySegmentPath(ref.Path) {
		abs := filepath.Join(dir, ref.Path)
		abs, size, checksum, err := writeLatestBinarySegment(abs, seg)
		if err != nil {
			return SegmentRef{}, err
		}
		rel, err := filepath.Rel(dir, abs)
		if err != nil {
			return SegmentRef{}, err
		}
		ref.Path = filepath.ToSlash(rel)
		ref.Size = size
		ref.Checksum = checksum
		return ref, nil
	}
	data, err := json.Marshal(seg)
	if err != nil {
		return SegmentRef{}, err
	}
	sum := sha256.Sum256(data)
	ref.Size = uint64(len(data))
	ref.Checksum = "sha256:" + hex.EncodeToString(sum[:])

	abs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, err
	}
	if err := os.Rename(tmpName, abs); err != nil {
		return SegmentRef{}, err
	}
	return ref, nil
}

func writeLatestSegmentFromIterator(dir string, ref SegmentRef, iter latestEntryIterator) (SegmentRef, error) {
	if iter == nil {
		return SegmentRef{}, errors.New("snapshots: nil latest entry iterator")
	}
	if ref.Kind == "" {
		ref.Kind = SegmentLatest
	}
	if ref.Dataset == "" && ref.Kind == SegmentLatest {
		ref.Dataset = SegmentDatasetKVLatest
	}
	if isLatestBinarySegmentPath(ref.Path) {
		segRef, _, _, err := writeLatestBinarySegmentAndAccessor(dir, ref, iter)
		return segRef, err
	}
	if err := validateLatestStreamRef(ref); err != nil {
		return SegmentRef{}, err
	}
	return writeLatestJSONSegmentFromIterator(dir, ref, iter)
}

func validateLatestStreamRef(ref SegmentRef) error {
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return err
	}
	dataset := ref.normalizedDataset()
	switch dataset {
	case SegmentDatasetKVLatest:
		if !kvdomains.IsRegistered(ref.Domain) {
			return fmt.Errorf("snapshots: unregistered latest segment domain %#04x", uint16(ref.Domain))
		}
	case SegmentDatasetAccountLatest, SegmentDatasetKVGeneration, SegmentDatasetCode, SegmentDatasetCommitmentRoot, SegmentDatasetCommitmentNode, SegmentDatasetCommitmentCheckpoint:
		if ref.Domain != 0 {
			return fmt.Errorf("snapshots: %s latest segment must not set kv domain %#04x", dataset, uint16(ref.Domain))
		}
	default:
		return fmt.Errorf("snapshots: unknown latest dataset %q", ref.Dataset)
	}
	if ref.ToTxNum < ref.FromTxNum {
		return fmt.Errorf("snapshots: latest segment range [%d,%d] is inverted", ref.FromTxNum, ref.ToTxNum)
	}
	return nil
}

func writeLatestJSONSegmentFromIterator(dir string, ref SegmentRef, iter latestEntryIterator) (SegmentRef, error) {
	abs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if err := writeLatestJSONHeader(tmp, ref); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	var count uint64
	var prev []byte
	err = iter(func(entry LatestEntry) error {
		entry = LatestEntry{
			Key:   append([]byte(nil), entry.Key...),
			Value: append([]byte(nil), entry.Value...),
		}
		if len(entry.Key) == 0 {
			return errors.New("snapshots: latest segment contains empty key")
		}
		if len(prev) > 0 && bytes.Compare(prev, entry.Key) >= 0 {
			return errors.New("snapshots: latest stream entries are not strictly sorted")
		}
		if err := validateLatestEntry(ref.normalizedDataset(), entry); err != nil {
			return err
		}
		if count > 0 {
			if _, err := io.WriteString(tmp, ","); err != nil {
				return err
			}
		}
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if _, err := tmp.Write(data); err != nil {
			return err
		}
		prev = entry.Key
		count++
		return nil
	})
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if ref.normalizedDataset() == SegmentDatasetCommitmentRoot && count != 1 {
		_ = tmp.Close()
		return SegmentRef{}, fmt.Errorf("snapshots: commitment root segment entries = %d, want 1", count)
	}
	if _, err := io.WriteString(tmp, "]}"); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, err
	}
	size, checksum, _, err := latestBinaryFileMetadata(tmpName)
	if err != nil {
		return SegmentRef{}, err
	}
	ref.Size = size
	ref.Checksum = checksum
	if err := os.Rename(tmpName, abs); err != nil {
		return SegmentRef{}, err
	}
	return ref, nil
}

func writeLatestJSONHeader(w io.Writer, ref SegmentRef) error {
	if _, err := fmt.Fprintf(w, `{"version":%d`, LatestSegmentVersion); err != nil {
		return err
	}
	if dataset := ref.normalizedDataset(); dataset != "" {
		data, err := json.Marshal(dataset)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, `,"dataset":%s`, data); err != nil {
			return err
		}
	}
	if ref.Domain != 0 {
		if _, err := fmt.Fprintf(w, `,"domain":%d`, uint16(ref.Domain)); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, `,"fromTxNum":%d,"toTxNum":%d,"entries":[`, ref.FromTxNum, ref.ToTxNum)
	return err
}

func OpenLatestSegment(dir string, ref SegmentRef) (*LatestSegment, error) {
	if ref.Kind != SegmentLatest {
		return nil, fmt.Errorf("snapshots: segment %q is %q, want latest", ref.Path, ref.Kind)
	}
	if ref.Dataset == "" {
		ref.Dataset = ref.normalizedDataset()
	}
	if isLatestBinarySegmentPath(ref.Path) {
		return readLatestBinarySegment(filepath.Join(dir, ref.Path), ref)
	}
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	if ref.Size != 0 && uint64(len(data)) != ref.Size {
		return nil, fmt.Errorf("snapshots: segment %q size %d, want %d", ref.Path, len(data), ref.Size)
	}
	if ref.Checksum != "" {
		sum := sha256.Sum256(data)
		want := "sha256:" + hex.EncodeToString(sum[:])
		if !strings.EqualFold(ref.Checksum, want) {
			return nil, fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, want, ref.Checksum)
		}
	}
	var seg LatestSegment
	if err := json.Unmarshal(data, &seg); err != nil {
		return nil, err
	}
	if seg.Dataset == "" {
		seg.Dataset = SegmentDatasetKVLatest
	}
	if err := seg.Validate(); err != nil {
		return nil, err
	}
	if seg.normalizedDataset() != ref.normalizedDataset() || seg.Domain != ref.Domain || seg.FromTxNum != ref.FromTxNum || seg.ToTxNum != ref.ToTxNum {
		return nil, fmt.Errorf("snapshots: segment %q metadata does not match manifest", ref.Path)
	}
	return &seg, nil
}

func CheckLatestSegment(dir string, ref SegmentRef) error {
	if ref.Kind != SegmentLatest {
		return fmt.Errorf("snapshots: segment %q is %q, want latest", ref.Path, ref.Kind)
	}
	if ref.Dataset == "" {
		ref.Dataset = ref.normalizedDataset()
	}
	if isLatestBinarySegmentPath(ref.Path) {
		return checkLatestBinarySegment(dir, ref)
	}
	_, err := OpenLatestSegment(dir, ref)
	return err
}

func CheckLatestAccessorSegment(dir string, ref SegmentRef) error {
	return checkLatestBinaryAccessor(dir, ref)
}

func CheckLatestBTreeSegment(dir string, ref SegmentRef) error {
	file, header, err := openLatestBinaryBTreeReader(dir, ref)
	if err != nil {
		return err
	}
	defer file.Close()
	var prev []byte
	var prevOrdinal uint64
	for i := uint64(0); i < header.count; i++ {
		entry, ok, err := readLatestBinaryBTreeEntryAt(file, i)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("snapshots: latest btree %q missing entry %d", ref.Path, i)
		}
		if len(entry.key) == 0 {
			return fmt.Errorf("snapshots: latest btree %q entry %d has empty key", ref.Path, i)
		}
		if i > 0 {
			if bytes.Compare(prev, entry.key) >= 0 {
				return fmt.Errorf("snapshots: latest btree %q entries are not sorted", ref.Path)
			}
			if entry.ordinal <= prevOrdinal {
				return fmt.Errorf("snapshots: latest btree %q ordinals are not increasing", ref.Path)
			}
		}
		prev = entry.key
		prevOrdinal = entry.ordinal
	}
	return nil
}

func OpenManager(dir string) (*Manager, error) {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return &Manager{dir: dir, cache: make(map[string]*LatestSegment)}, nil
		}
		return nil, err
	}
	return &Manager{dir: dir, manifest: manifest, cache: make(map[string]*LatestSegment)}, nil
}

func (m *Manager) Manifest() *Manifest {
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return nil
	}
	cp := *manifest
	if manifest.Progress != nil {
		progress := *manifest.Progress
		cp.Progress = &progress
	}
	cp.Segments = append([]SegmentRef(nil), manifest.Segments...)
	cp.Retired = append([]SegmentRef(nil), manifest.Retired...)
	return &cp
}

func (m *Manager) GetLatest(domain kvdomains.KVDomain, key []byte, txNum uint64) ([]byte, bool, error) {
	return m.getLatestValue(SegmentDatasetKVLatest, domain, key, txNum)
}

func (m *Manager) IterateLatestPrefix(domain kvdomains.KVDomain, prefix []byte, txNum uint64, fn func(key, value []byte) (bool, error)) error {
	return m.iterateLatestPrefix(SegmentDatasetKVLatest, domain, prefix, txNum, fn)
}

func (m *Manager) GetAccountLatest(owner common.Address, txNum uint64) ([]byte, bool, error) {
	return m.getLatestValue(SegmentDatasetAccountLatest, 0, AccountSnapshotKey(owner), txNum)
}

func (m *Manager) IterateAccountLatestPrefix(ownerPrefix []byte, txNum uint64, fn func(owner common.Address, value []byte) (bool, error)) error {
	return m.iterateLatestPrefix(SegmentDatasetAccountLatest, 0, ownerPrefix, txNum, func(key, value []byte) (bool, error) {
		owner, err := decodeAccountSnapshotKey(key)
		if err != nil {
			return false, err
		}
		return fn(owner, value)
	})
}

func (m *Manager) GetKVLatest(domain kvdomains.KVDomain, owner common.Address, generation uint64, logicalKey []byte, txNum uint64) ([]byte, bool, error) {
	return m.getLatestValue(SegmentDatasetKVLatest, domain, AccountKVSnapshotKey(owner, generation, logicalKey), txNum)
}

func (m *Manager) IterateKVLatestPrefix(domain kvdomains.KVDomain, owner common.Address, generation uint64, logicalPrefix []byte, txNum uint64, fn func(logicalKey, value []byte) (bool, error)) error {
	prefix := AccountKVSnapshotKey(owner, generation, logicalPrefix)
	return m.iterateLatestPrefix(SegmentDatasetKVLatest, domain, prefix, txNum, func(key, value []byte) (bool, error) {
		_, _, logicalKey, err := decodeKVLatestSnapshotKey(key)
		if err != nil {
			return false, err
		}
		return fn(logicalKey, value)
	})
}

func (m *Manager) GetKVGeneration(owner common.Address, txNum uint64) (uint64, bool, error) {
	value, ok, err := m.getLatestValue(SegmentDatasetKVGeneration, 0, KVGenerationSnapshotKey(owner), txNum)
	if err != nil || !ok {
		return 0, ok, err
	}
	generation, err := rawdb.DecodeStateKVGenerationValue(value)
	if err != nil {
		return 0, false, err
	}
	return generation, true, nil
}

func (m *Manager) GetCommitmentRoot(txNum uint64) (common.Hash, bool, error) {
	value, ok, err := m.getLatestValue(SegmentDatasetCommitmentRoot, 0, rawdb.LatestDomainCommitmentRootLogicalKey(), txNum)
	if err != nil || !ok {
		return common.Hash{}, ok, err
	}
	if len(value) != common.HashLength {
		return common.Hash{}, false, fmt.Errorf("snapshots: commitment root length %d", len(value))
	}
	return common.BytesToHash(value), true, nil
}

func (m *Manager) GetCommitmentNode(logicalKey []byte, txNum uint64) ([]byte, bool, error) {
	return m.getLatestValue(SegmentDatasetCommitmentNode, 0, CommitmentSnapshotKey(logicalKey), txNum)
}

func (m *Manager) IterateCommitmentNodes(logicalPrefix []byte, txNum uint64, fn func(logicalKey, value []byte) (bool, error)) error {
	return m.iterateLatestPrefix(SegmentDatasetCommitmentNode, 0, logicalPrefix, txNum, fn)
}

func (m *Manager) GetCode(hash common.Hash, txNum uint64) ([]byte, bool, error) {
	if hash == (common.Hash{}) {
		return nil, false, nil
	}
	return m.getLatestValue(SegmentDatasetCode, 0, CodeSnapshotKey(hash), txNum)
}

func (m *Manager) GetCodeAtOrBefore(hash common.Hash, txNum uint64) ([]byte, bool, error) {
	if hash == (common.Hash{}) {
		return nil, false, nil
	}
	code, ok, err := m.GetCode(hash, txNum)
	if err != nil || ok {
		return code, ok, err
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return nil, false, err
	}
	var refs []SegmentRef
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentLatest || ref.normalizedDataset() != SegmentDatasetCode || ref.Domain != 0 || ref.ToTxNum > txNum {
			continue
		}
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].ToTxNum != refs[j].ToTxNum {
			return refs[i].ToTxNum > refs[j].ToTxNum
		}
		return refs[i].FromTxNum > refs[j].FromTxNum
	})
	key := CodeSnapshotKey(hash)
	for _, ref := range refs {
		code, ok, err := m.getLatestValueFromRef(ref, key)
		if err != nil || ok {
			return code, ok, err
		}
	}
	return nil, false, nil
}

func (m *Manager) IterateCodePrefix(hashPrefix []byte, txNum uint64, fn func(hash common.Hash, code []byte) (bool, error)) error {
	return m.iterateLatestPrefix(SegmentDatasetCode, 0, hashPrefix, txNum, func(key, value []byte) (bool, error) {
		hash, err := decodeCodeSnapshotKey(key)
		if err != nil {
			return false, err
		}
		return fn(hash, value)
	})
}

func (m *Manager) RestoreLatest(db ethdb.KeyValueWriter, txNum uint64) error {
	manifest, err := m.currentManifest()
	if err != nil {
		return err
	}
	if manifest == nil {
		return nil
	}
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentLatest || txNum < ref.FromTxNum || txNum > ref.ToTxNum {
			continue
		}
		if isLatestBinarySegmentPath(ref.Path) {
			if err := restoreLatestBinarySegmentToStore(m.dir, ref, newRawDBLatestHotRestoreStore(db)); err != nil {
				return fmt.Errorf("restore %s segment %q: %w", ref.normalizedDataset(), ref.Path, err)
			}
			continue
		}
		seg, err := m.load(ref)
		if err != nil {
			return err
		}
		if err := seg.Restore(db); err != nil {
			return fmt.Errorf("restore %s segment %q: %w", ref.normalizedDataset(), ref.Path, err)
		}
	}
	return nil
}

func (m *Manager) getLatestValue(dataset SegmentDataset, domain kvdomains.KVDomain, key []byte, txNum uint64) ([]byte, bool, error) {
	ref, ok, err := m.latestSegmentRef(dataset, domain, txNum)
	if err != nil || !ok {
		return nil, false, err
	}
	return m.getLatestValueFromRef(ref, key)
}

func (m *Manager) getLatestValueFromRef(ref SegmentRef, key []byte) ([]byte, bool, error) {
	if isLatestBinarySegmentPath(ref.Path) {
		if btreeRef, ok := latestBinaryBTreeRef(m.manifest, ref); ok {
			return readLatestBinaryValueByBTreeFile(m.dir, filepath.Join(m.dir, ref.Path), ref, btreeRef, key)
		}
		if accessorRef, ok := latestBinaryAccessorRef(m.manifest, ref); ok {
			return readLatestBinaryValueByAccessorFile(m.dir, filepath.Join(m.dir, ref.Path), ref, accessorRef, key)
		}
		return readLatestBinaryValue(filepath.Join(m.dir, ref.Path), ref, key)
	}
	seg, err := m.load(ref)
	if err != nil {
		return nil, false, err
	}
	return seg.Get(key)
}

func (m *Manager) iterateLatestPrefix(dataset SegmentDataset, domain kvdomains.KVDomain, prefix []byte, txNum uint64, fn func(key, value []byte) (bool, error)) error {
	ref, ok, err := m.latestSegmentRef(dataset, domain, txNum)
	if err != nil || !ok {
		return err
	}
	if isLatestBinarySegmentPath(ref.Path) {
		if btreeRef, ok := latestBinaryBTreeRef(m.manifest, ref); ok {
			return iterateLatestBinaryPrefixByBTreeFile(m.dir, filepath.Join(m.dir, ref.Path), ref, btreeRef, prefix, fn)
		}
		if accessorRef, ok := latestBinaryAccessorRef(m.manifest, ref); ok {
			return iterateLatestBinaryPrefixByAccessorFile(m.dir, filepath.Join(m.dir, ref.Path), ref, accessorRef, prefix, fn)
		}
		return iterateLatestBinaryPrefix(filepath.Join(m.dir, ref.Path), ref, prefix, fn)
	}
	seg, err := m.load(ref)
	if err != nil {
		return err
	}
	return seg.IteratePrefix(prefix, fn)
}

func (m *Manager) latestSegment(dataset SegmentDataset, domain kvdomains.KVDomain, txNum uint64) (*LatestSegment, bool, error) {
	ref, ok, err := m.latestSegmentRef(dataset, domain, txNum)
	if err != nil || !ok {
		return nil, ok, err
	}
	seg, err := m.load(ref)
	if err != nil {
		return nil, false, err
	}
	return seg, true, nil
}

func (m *Manager) latestSegmentRef(dataset SegmentDataset, domain kvdomains.KVDomain, txNum uint64) (SegmentRef, bool, error) {
	manifest, err := m.currentManifest()
	if err != nil {
		return SegmentRef{}, false, err
	}
	if manifest == nil {
		return SegmentRef{}, false, nil
	}
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentLatest || ref.normalizedDataset() != dataset || ref.Domain != domain || txNum < ref.FromTxNum || txNum > ref.ToTxNum {
			continue
		}
		return ref, true, nil
	}
	return SegmentRef{}, false, nil
}

func latestBinaryAccessorRef(manifest *Manifest, latestRef SegmentRef) (SegmentRef, bool) {
	if manifest == nil {
		return SegmentRef{}, false
	}
	wantPath := latestBinaryAccessorPath(latestRef.Path)
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentAccessor &&
			ref.normalizedDataset() == latestRef.normalizedDataset() &&
			ref.Domain == latestRef.Domain &&
			ref.FromTxNum == latestRef.FromTxNum &&
			ref.ToTxNum == latestRef.ToTxNum &&
			ref.Path == wantPath {
			return ref, true
		}
	}
	return SegmentRef{}, false
}

func latestBinaryBTreeRef(manifest *Manifest, latestRef SegmentRef) (SegmentRef, bool) {
	if manifest == nil {
		return SegmentRef{}, false
	}
	wantPath := latestBinaryBTreePath(latestRef.Path)
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentBTree &&
			ref.normalizedDataset() == latestRef.normalizedDataset() &&
			ref.Domain == latestRef.Domain &&
			ref.FromTxNum == latestRef.FromTxNum &&
			ref.ToTxNum == latestRef.ToTxNum &&
			ref.Path == wantPath {
			return ref, true
		}
	}
	return SegmentRef{}, false
}

func (m *Manager) currentManifest() (*Manifest, error) {
	if m == nil {
		return nil, nil
	}
	manifest, err := LoadProductionManifest(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	m.manifest = manifest
	return manifest, nil
}

func (m *Manager) load(ref SegmentRef) (*LatestSegment, error) {
	if seg := m.cache[ref.Path]; seg != nil {
		return seg, nil
	}
	seg, err := OpenLatestSegment(m.dir, ref)
	if err != nil {
		return nil, err
	}
	m.cache[ref.Path] = seg
	return seg, nil
}

func (s *LatestSegment) Validate() error {
	if s == nil {
		return errors.New("snapshots: nil latest segment")
	}
	if s.Version != LatestSegmentVersion {
		return fmt.Errorf("snapshots: unsupported latest segment version %d", s.Version)
	}
	dataset := s.normalizedDataset()
	switch dataset {
	case SegmentDatasetKVLatest:
		if !kvdomains.IsRegistered(s.Domain) {
			return fmt.Errorf("snapshots: unregistered latest segment domain %#04x", uint16(s.Domain))
		}
	case SegmentDatasetAccountLatest, SegmentDatasetKVGeneration, SegmentDatasetCode, SegmentDatasetCommitmentRoot, SegmentDatasetCommitmentNode, SegmentDatasetCommitmentCheckpoint:
		if s.Domain != 0 {
			return fmt.Errorf("snapshots: %s latest segment must not set kv domain %#04x", dataset, uint16(s.Domain))
		}
	default:
		return fmt.Errorf("snapshots: unknown latest dataset %q", s.Dataset)
	}
	if s.ToTxNum < s.FromTxNum {
		return fmt.Errorf("snapshots: latest segment range [%d,%d] is inverted", s.FromTxNum, s.ToTxNum)
	}
	if dataset == SegmentDatasetCommitmentRoot && len(s.Entries) != 1 {
		return fmt.Errorf("snapshots: commitment root segment entries = %d, want 1", len(s.Entries))
	}
	for i := range s.Entries {
		if len(s.Entries[i].Key) == 0 {
			return errors.New("snapshots: latest segment contains empty key")
		}
		if i > 0 && bytes.Compare(s.Entries[i-1].Key, s.Entries[i].Key) >= 0 {
			return errors.New("snapshots: latest segment entries are not strictly sorted")
		}
		if err := validateLatestEntry(dataset, s.Entries[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *LatestSegment) Get(key []byte) ([]byte, bool, error) {
	if err := s.Validate(); err != nil {
		return nil, false, err
	}
	i := sort.Search(len(s.Entries), func(i int) bool {
		return bytes.Compare(s.Entries[i].Key, key) >= 0
	})
	if i == len(s.Entries) || !bytes.Equal(s.Entries[i].Key, key) {
		return nil, false, nil
	}
	return append([]byte(nil), s.Entries[i].Value...), true, nil
}

func (s *LatestSegment) IteratePrefix(prefix []byte, fn func(key, value []byte) (bool, error)) error {
	if err := s.Validate(); err != nil {
		return err
	}
	i := sort.Search(len(s.Entries), func(i int) bool {
		return bytes.Compare(s.Entries[i].Key, prefix) >= 0
	})
	for ; i < len(s.Entries); i++ {
		entry := s.Entries[i]
		if !bytes.HasPrefix(entry.Key, prefix) {
			break
		}
		cont, err := fn(append([]byte(nil), entry.Key...), append([]byte(nil), entry.Value...))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

func (s *LatestSegment) Restore(db ethdb.KeyValueWriter) error {
	if db == nil {
		return errors.New("snapshots: nil database")
	}
	return s.restoreToStore(newRawDBLatestHotRestoreStore(db))
}

func (s *LatestSegment) restoreToStore(store latestHotStore) error {
	if store == nil {
		return errors.New("snapshots: nil latest hot store")
	}
	if err := s.Validate(); err != nil {
		return err
	}
	switch s.normalizedDataset() {
	case SegmentDatasetAccountLatest:
		for _, entry := range s.Entries {
			if err := restoreLatestEntryToStore(s.normalizedDataset(), s.Domain, store, entry); err != nil {
				return err
			}
		}
	case SegmentDatasetKVLatest:
		for _, entry := range s.Entries {
			if err := restoreLatestEntryToStore(s.normalizedDataset(), s.Domain, store, entry); err != nil {
				return err
			}
		}
	case SegmentDatasetKVGeneration:
		for _, entry := range s.Entries {
			if err := restoreLatestEntryToStore(s.normalizedDataset(), s.Domain, store, entry); err != nil {
				return err
			}
		}
	case SegmentDatasetCode:
		for _, entry := range s.Entries {
			if err := restoreLatestEntryToStore(s.normalizedDataset(), s.Domain, store, entry); err != nil {
				return err
			}
		}
	case SegmentDatasetCommitmentRoot, SegmentDatasetCommitmentNode, SegmentDatasetCommitmentCheckpoint:
		for _, entry := range s.Entries {
			if err := restoreLatestEntryToStore(s.normalizedDataset(), s.Domain, store, entry); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("snapshots: unknown latest dataset %q", s.Dataset)
	}
	return nil
}

func restoreLatestEntryToStore(dataset SegmentDataset, domain kvdomains.KVDomain, store latestHotStore, entry LatestEntry) error {
	if store == nil {
		return errors.New("snapshots: nil latest hot store")
	}
	if err := validateLatestEntry(dataset, entry); err != nil {
		return err
	}
	switch dataset {
	case SegmentDatasetAccountLatest:
		owner, err := decodeAccountSnapshotKey(entry.Key)
		if err != nil {
			return err
		}
		return store.WriteAccountLatest(owner, entry.Value)
	case SegmentDatasetKVLatest:
		owner, generation, logicalKey, err := decodeKVLatestSnapshotKey(entry.Key)
		if err != nil {
			return err
		}
		return store.WriteKVLatest(owner, generation, domain, logicalKey, entry.Value)
	case SegmentDatasetKVGeneration:
		owner, err := decodeAccountSnapshotKey(entry.Key)
		if err != nil {
			return err
		}
		generation, err := rawdb.DecodeStateKVGenerationValue(entry.Value)
		if err != nil {
			return err
		}
		return store.WriteKVGeneration(owner, generation)
	case SegmentDatasetCode:
		hash, err := decodeCodeSnapshotKey(entry.Key)
		if err != nil {
			return err
		}
		return store.WriteCode(hash, entry.Value)
	case SegmentDatasetCommitmentRoot, SegmentDatasetCommitmentNode, SegmentDatasetCommitmentCheckpoint:
		return store.WriteCommitmentDomain(entry.Key, entry.Value)
	default:
		return fmt.Errorf("snapshots: unknown latest dataset %q", dataset)
	}
}

func (s *LatestSegment) normalizedDataset() SegmentDataset {
	if s.Dataset == "" {
		return SegmentDatasetKVLatest
	}
	return s.Dataset
}

func normalizeLatestEntries(entries []LatestEntry) []LatestEntry {
	out := make([]LatestEntry, len(entries))
	for i, entry := range entries {
		out[i] = LatestEntry{
			Key:   append([]byte(nil), entry.Key...),
			Value: append([]byte(nil), entry.Value...),
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Key, out[j].Key) < 0
	})
	return out
}

func validateLatestEntry(dataset SegmentDataset, entry LatestEntry) error {
	switch dataset {
	case SegmentDatasetAccountLatest:
		if _, err := decodeAccountSnapshotKey(entry.Key); err != nil {
			return err
		}
	case SegmentDatasetKVLatest:
		if _, _, _, err := decodeKVLatestSnapshotKey(entry.Key); err != nil {
			return err
		}
	case SegmentDatasetKVGeneration:
		if _, err := decodeAccountSnapshotKey(entry.Key); err != nil {
			return err
		}
		if _, err := rawdb.DecodeStateKVGenerationValue(entry.Value); err != nil {
			return err
		}
	case SegmentDatasetCode:
		hash, err := decodeCodeSnapshotKey(entry.Key)
		if err != nil {
			return err
		}
		if len(entry.Value) == 0 {
			return fmt.Errorf("snapshots: code segment has empty bytecode for %x", hash)
		}
		if common.Keccak256(entry.Value) != hash {
			return fmt.Errorf("snapshots: code segment hash mismatch for %x", hash)
		}
	case SegmentDatasetCommitmentRoot:
		if !rawdb.IsLatestDomainCommitmentRootLogicalKey(entry.Key) {
			return fmt.Errorf("snapshots: commitment root segment has key %q", entry.Key)
		}
		if len(entry.Value) != common.HashLength {
			return fmt.Errorf("snapshots: commitment root length %d", len(entry.Value))
		}
	case SegmentDatasetCommitmentNode:
		if !rawdb.IsLatestDomainCommitmentNodeLogicalKey(entry.Key) {
			return fmt.Errorf("snapshots: commitment node segment has key %q", entry.Key)
		}
		if len(entry.Value) != common.HashLength {
			return fmt.Errorf("snapshots: commitment node length %d", len(entry.Value))
		}
	case SegmentDatasetCommitmentCheckpoint:
		if !rawdb.IsStateCommitmentCheckpointLogicalKey(entry.Key) && !rawdb.IsLatestStateCommitmentCheckpointLogicalKey(entry.Key) {
			return fmt.Errorf("snapshots: commitment checkpoint segment has key %q", entry.Key)
		}
		if len(entry.Value) == 0 {
			return errors.New("snapshots: commitment checkpoint has empty value")
		}
		if _, err := rawdb.DecodeStateCommitmentCheckpointValue(entry.Value); err != nil {
			return fmt.Errorf("snapshots: decode commitment checkpoint %q: %w", entry.Key, err)
		}
	default:
		return fmt.Errorf("snapshots: unknown latest dataset %q", dataset)
	}
	return nil
}

func decodeAccountSnapshotKey(key []byte) (common.Address, error) {
	if len(key) != common.AccountIDLength {
		return common.Address{}, fmt.Errorf("snapshots: account key length %d", len(key))
	}
	var id common.AccountID
	copy(id[:], key)
	return id.Address(common.AddressPrefixMainnet), nil
}

func decodeCodeSnapshotKey(key []byte) (common.Hash, error) {
	if len(key) != common.HashLength {
		return common.Hash{}, fmt.Errorf("snapshots: code key length %d", len(key))
	}
	return common.BytesToHash(key), nil
}

func decodeKVLatestSnapshotKey(key []byte) (common.Address, uint64, []byte, error) {
	if len(key) < common.AccountIDLength+8 {
		return common.Address{}, 0, nil, fmt.Errorf("snapshots: kv latest key length %d", len(key))
	}
	owner, err := decodeAccountSnapshotKey(key[:common.AccountIDLength])
	if err != nil {
		return common.Address{}, 0, nil, err
	}
	generation := binary.BigEndian.Uint64(key[common.AccountIDLength : common.AccountIDLength+8])
	return owner, generation, append([]byte(nil), key[common.AccountIDLength+8:]...), nil
}
