package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	Domain    kvdomains.KVDomain `json:"domain"`
	FromTxNum uint64             `json:"fromTxNum"`
	ToTxNum   uint64             `json:"toTxNum"`
	Entries   []LatestEntry      `json:"entries"`
}

type Manager struct {
	dir      string
	manifest *Manifest
	cache    map[string]*LatestSegment
}

func AccountKVSnapshotKey(owner common.Address, generation uint64, logicalKey []byte) []byte {
	id := owner.AccountID()
	out := make([]byte, 0, common.AccountIDLength+8+len(logicalKey))
	out = append(out, id[:]...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], generation)
	out = append(out, buf[:]...)
	return append(out, logicalKey...)
}

func BuildLatestDomainSegmentFromDB(db ethdb.Iteratee, dir string, domain kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, errors.New("snapshots: nil database")
	}
	var entries []LatestEntry
	if err := rawdb.IterateStateKVLatestDomainRows(db, domain, func(row rawdb.StateKVLatestRow) (bool, error) {
		entries = append(entries, LatestEntry{
			Key:   AccountKVSnapshotKey(row.Owner, row.Generation, row.Key),
			Value: append([]byte(nil), row.Value...),
		})
		return true, nil
	}); err != nil {
		return SegmentRef{}, err
	}
	ref := SegmentRef{
		Domain:    domain,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}
	return WriteLatestSegment(dir, ref, entries)
}

func WriteLatestSegment(dir string, ref SegmentRef, entries []LatestEntry) (SegmentRef, error) {
	if ref.Kind == "" {
		ref.Kind = SegmentLatest
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return SegmentRef{}, err
	}
	seg := &LatestSegment{
		Version:   LatestSegmentVersion,
		Domain:    ref.Domain,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Entries:   normalizeLatestEntries(entries),
	}
	if err := seg.Validate(); err != nil {
		return SegmentRef{}, err
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

func OpenLatestSegment(dir string, ref SegmentRef) (*LatestSegment, error) {
	if ref.Kind != SegmentLatest {
		return nil, fmt.Errorf("snapshots: segment %q is %q, want latest", ref.Path, ref.Kind)
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
	if err := seg.Validate(); err != nil {
		return nil, err
	}
	if seg.Domain != ref.Domain || seg.FromTxNum != ref.FromTxNum || seg.ToTxNum != ref.ToTxNum {
		return nil, fmt.Errorf("snapshots: segment %q metadata does not match manifest", ref.Path)
	}
	return &seg, nil
}

func OpenManager(dir string) (*Manager, error) {
	manifest, err := LoadManifest(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return &Manager{dir: dir, cache: make(map[string]*LatestSegment)}, nil
		}
		return nil, err
	}
	return &Manager{dir: dir, manifest: manifest, cache: make(map[string]*LatestSegment)}, nil
}

func (m *Manager) Manifest() *Manifest {
	if m == nil || m.manifest == nil {
		return nil
	}
	cp := *m.manifest
	cp.Segments = append([]SegmentRef(nil), m.manifest.Segments...)
	return &cp
}

func (m *Manager) GetLatest(domain kvdomains.KVDomain, key []byte, txNum uint64) ([]byte, bool, error) {
	seg, ok, err := m.latestSegment(domain, txNum)
	if err != nil || !ok {
		return nil, false, err
	}
	return seg.Get(key)
}

func (m *Manager) IterateLatestPrefix(domain kvdomains.KVDomain, prefix []byte, txNum uint64, fn func(key, value []byte) (bool, error)) error {
	seg, ok, err := m.latestSegment(domain, txNum)
	if err != nil || !ok {
		return err
	}
	return seg.IteratePrefix(prefix, fn)
}

func (m *Manager) latestSegment(domain kvdomains.KVDomain, txNum uint64) (*LatestSegment, bool, error) {
	if m == nil || m.manifest == nil {
		return nil, false, nil
	}
	for _, ref := range m.manifest.Segments {
		if ref.Kind != SegmentLatest || ref.Domain != domain || txNum < ref.FromTxNum || txNum > ref.ToTxNum {
			continue
		}
		seg, err := m.load(ref)
		if err != nil {
			return nil, false, err
		}
		return seg, true, nil
	}
	return nil, false, nil
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
	if !kvdomains.IsRegistered(s.Domain) {
		return fmt.Errorf("snapshots: unregistered latest segment domain %#04x", uint16(s.Domain))
	}
	if s.ToTxNum < s.FromTxNum {
		return fmt.Errorf("snapshots: latest segment range [%d,%d] is inverted", s.FromTxNum, s.ToTxNum)
	}
	for i := range s.Entries {
		if len(s.Entries[i].Key) == 0 {
			return errors.New("snapshots: latest segment contains empty key")
		}
		if i > 0 && bytes.Compare(s.Entries[i-1].Key, s.Entries[i].Key) >= 0 {
			return errors.New("snapshots: latest segment entries are not strictly sorted")
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
