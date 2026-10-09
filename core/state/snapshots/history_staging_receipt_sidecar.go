package snapshots

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
)

// This file is advisory: it remembers a successful physical SHA audit, not a
// route, semantic receipt, or permission to prune hot history. A missing or
// invalid certificate always falls back to the real checksum verifier.
const historyStagingReceiptSidecar = ".history-staging-receipt-checksums-v1.json"
const historyStagingReceiptSidecarMaxBytes = 32 << 20
const historyStagingReceiptSidecarMaxEntries = 100000

var historyStagingReceiptSidecarMu sync.Mutex

var historyStagingReceiptSidecarMemory = struct {
	root    string
	info    os.FileInfo
	ctime   [2]int64
	known   bool
	entries map[[32]byte]historyStagingReceiptCertificate
}{}

func historyStagingSidecarUnchanged(root string, info os.FileInfo) bool {
	cache := &historyStagingReceiptSidecarMemory
	if cache.root != root || cache.info == nil || info == nil || !os.SameFile(cache.info, info) ||
		cache.info.Size() != info.Size() || cache.info.ModTime() != info.ModTime() || cache.info.Mode() != info.Mode() {
		return false
	}
	ctime, known := historyStagingChangeTime(info)
	return known && cache.known && ctime == cache.ctime
}

type historyStagingPhysicalFactKey struct{}

// HistoryStagingPhysicalFactCollector is opt-in for a publication writer.
// Read-only proofs never write sidecars. The caller commits only after its
// durable cold binding has been published; failed or interrupted proofs leave
// at most uncommitted in-memory facts.
type HistoryStagingPhysicalFactCollector struct {
	root    string
	mu      sync.Mutex
	entries map[[32]byte]historyStagingReceiptCertificate
	states  map[[32]byte][3]historyStagingFileState
}

func WithHistoryStagingPhysicalFacts(ctx context.Context, dir string) (context.Context, *HistoryStagingPhysicalFactCollector, error) {
	if ctx == nil {
		return nil, nil, errors.New("snapshots: missing physical fact context")
	}
	root, err := historyStagingCanonicalRoot(dir)
	if err != nil {
		return nil, nil, err
	}
	c := &HistoryStagingPhysicalFactCollector{root: root,
		entries: make(map[[32]byte]historyStagingReceiptCertificate),
		states:  make(map[[32]byte][3]historyStagingFileState)}
	return context.WithValue(ctx, historyStagingPhysicalFactKey{}, c), c, nil
}

func (c *HistoryStagingPhysicalFactCollector) record(dir string, id [32]byte, refs [3]SegmentRef, states [3]historyStagingFileState) {
	if c == nil {
		return
	}
	root, err := historyStagingCanonicalRoot(dir)
	if err != nil || root != c.root {
		return
	}
	cert, ok := historyStagingMakeCertificate(id, refs, states)
	if !ok {
		return
	}
	c.mu.Lock()
	c.entries[id], c.states[id] = cert, states
	c.mu.Unlock()
}

// RecheckAll rejects changes to files whose physical checksums were already
// authenticated by this collector. It does not authenticate a new trio,
// verify a semantic binding, or write the advisory sidecar.
func (c *HistoryStagingPhysicalFactCollector) RecheckAll(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errors.New("snapshots: missing physical fact collector")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recheckAllLocked(ctx)
}

func (c *HistoryStagingPhysicalFactCollector) recheckAllLocked(ctx context.Context) error {
	for id, cert := range c.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := historyStagingCheckFileStates(c.root, cert.Refs, c.states[id]); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// CommitPhysicalCertificates performs a final metadata recheck and persists
// only physical facts. A sidecar write error is advisory; changed files and
// cancellation are not. A binding's semantic/route authority is external.
func (c *HistoryStagingPhysicalFactCollector) CommitPhysicalCertificates(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errors.New("snapshots: missing physical fact collector")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.recheckAllLocked(ctx); err != nil {
		return err
	}
	writeErr := historyStagingWriteCertificates(ctx, c.root, c.entries, nil, nil)
	if err := c.recheckAllLocked(ctx); err != nil {
		return err
	}
	return historyStagingAdvisoryWriteError(ctx, writeErr)
}

// ErrHistoryStagingReceiptSidecarWrite marks only advisory cache persistence
// failures. File-change, cancellation, and proof failures must never be
// downgraded to warnings by startup or offline cleanup.
var ErrHistoryStagingReceiptSidecarWrite = errors.New("snapshots: receipt sidecar write failed")

func historyStagingAdvisoryWriteError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.Join(ErrHistoryStagingReceiptSidecarWrite, err)
}

type historyStagingDiskFingerprint struct {
	Device uint64   `json:"device"`
	Inode  uint64   `json:"inode"`
	Size   int64    `json:"size"`
	Mode   uint32   `json:"mode"`
	Mtime  int64    `json:"mtime_ns"`
	Ctime  [2]int64 `json:"ctime"`
}

type historyStagingReceiptCertificate struct {
	ID    [32]byte                         `json:"id"`
	Refs  [3]SegmentRef                    `json:"refs"`
	Files [3]historyStagingDiskFingerprint `json:"files"`
}

type historyStagingReceiptSidecarBody struct {
	Version int                                `json:"version"`
	Root    string                             `json:"root"`
	Entries []historyStagingReceiptCertificate `json:"entries"`
}

type historyStagingReceiptSidecarDocument struct {
	historyStagingReceiptSidecarBody
	Digest string `json:"digest"`
}

func historyStagingCanonicalRoot(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

func historyStagingUnsignedStatField(info os.FileInfo, name string) (uint64, bool) {
	value := reflect.ValueOf(info.Sys())
	if value.Kind() == reflect.Pointer && !value.IsNil() {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return 0, false
	}
	field := value.FieldByName(name)
	if !field.IsValid() {
		return 0, false
	}
	switch field.Kind() {
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		return field.Uint(), true
	case reflect.Int, reflect.Int32, reflect.Int64:
		if field.Int() < 0 {
			return 0, false
		}
		return uint64(field.Int()), true
	default:
		return 0, false
	}
}

func historyStagingDiskState(state historyStagingFileState) (historyStagingDiskFingerprint, bool) {
	if state.info == nil || !state.info.Mode().IsRegular() || !state.hasChangeTime {
		return historyStagingDiskFingerprint{}, false
	}
	dev, ok := historyStagingUnsignedStatField(state.info, "Dev")
	if !ok {
		return historyStagingDiskFingerprint{}, false
	}
	ino, ok := historyStagingUnsignedStatField(state.info, "Ino")
	if !ok {
		return historyStagingDiskFingerprint{}, false
	}
	return historyStagingDiskFingerprint{Device: dev, Inode: ino, Size: state.info.Size(),
		Mode: uint32(state.info.Mode()), Mtime: state.info.ModTime().UnixNano(), Ctime: state.changeTime}, true
}

func historyStagingMakeCertificate(id [32]byte, refs [3]SegmentRef, states [3]historyStagingFileState) (historyStagingReceiptCertificate, bool) {
	var out historyStagingReceiptCertificate
	actual, err := historyStagingTrioID(refs)
	if err != nil || actual != id {
		return out, false
	}
	out.ID, out.Refs = id, refs
	for i := range states {
		file, ok := historyStagingDiskState(states[i])
		if !ok {
			return historyStagingReceiptCertificate{}, false
		}
		out.Files[i] = file
	}
	return out, true
}

func historyStagingCertificateMatches(cert historyStagingReceiptCertificate, id [32]byte, refs [3]SegmentRef, states [3]historyStagingFileState) bool {
	if cert.ID != id || cert.Refs != refs {
		return false
	}
	actual, ok := historyStagingMakeCertificate(id, refs, states)
	return ok && cert == actual
}

func historyStagingReadCertificates(dir string) map[[32]byte]historyStagingReceiptCertificate {
	root, err := historyStagingCanonicalRoot(dir)
	if err != nil {
		return nil
	}
	path := filepath.Join(root, historyStagingReceiptSidecar)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > historyStagingReceiptSidecarMaxBytes {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() > historyStagingReceiptSidecarMaxBytes {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(file, historyStagingReceiptSidecarMaxBytes+1))
	if err != nil || len(data) > historyStagingReceiptSidecarMaxBytes {
		return nil
	}
	var doc historyStagingReceiptSidecarDocument
	if json.Unmarshal(data, &doc) != nil || doc.Version != 1 || doc.Root != root || len(doc.Entries) > historyStagingReceiptSidecarMaxEntries {
		return nil
	}
	body, err := json.Marshal(doc.historyStagingReceiptSidecarBody)
	if err != nil {
		return nil
	}
	digest := sha256.Sum256(body)
	if doc.Digest != hex.EncodeToString(digest[:]) {
		return nil
	}
	out := make(map[[32]byte]historyStagingReceiptCertificate, len(doc.Entries))
	for _, entry := range doc.Entries {
		if _, exists := out[entry.ID]; exists {
			return nil
		}
		if actual, err := historyStagingTrioID(entry.Refs); err != nil || actual != entry.ID {
			return nil
		}
		out[entry.ID] = entry
	}
	return out
}

// historyStagingWriteCertificates merges with the latest on-disk sidecar under
// a process-wide lock, then atomically replaces it. A crash leaves either the
// old complete document or the new one. Certificate write failure is advisory.
func historyStagingWriteCertificates(ctx context.Context, dir string, additions map[[32]byte]historyStagingReceiptCertificate,
	pruneKnown map[[32]byte]historyStagingReceiptCertificate, active map[[32]byte][3]SegmentRef) error {
	if len(additions) == 0 && len(pruneKnown) == 0 {
		return nil
	}
	if ctx == nil {
		return errors.New("snapshots: nil certificate context")
	}
	historyStagingReceiptSidecarMu.Lock()
	defer historyStagingReceiptSidecarMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := historyStagingCanonicalRoot(dir)
	if err != nil {
		return err
	}
	info, _ := os.Lstat(filepath.Join(root, historyStagingReceiptSidecar))
	var previous map[[32]byte]historyStagingReceiptCertificate
	if historyStagingSidecarUnchanged(root, info) {
		previous = historyStagingReceiptSidecarMemory.entries
	} else {
		previous = historyStagingReadCertificates(root)
	}
	all := make(map[[32]byte]historyStagingReceiptCertificate, len(previous)+len(additions))
	for id, entry := range previous {
		all[id] = entry
	}
	if info != nil && previous != nil {
		ctime, known := historyStagingChangeTime(info)
		historyStagingReceiptSidecarMemory.root, historyStagingReceiptSidecarMemory.info = root, info
		historyStagingReceiptSidecarMemory.ctime, historyStagingReceiptSidecarMemory.known = ctime, known
		historyStagingReceiptSidecarMemory.entries = previous
	}
	changed := false
	// Only a completed pinned audit may prune entries outside its manifest.
	// Keep records added or changed since that audit loaded its sidecar, so a
	// concurrent publication writer cannot lose its newly certified trio.
	if active != nil {
		for id, old := range pruneKnown {
			if _, stillActive := active[id]; stillActive {
				continue
			}
			if current, present := all[id]; present && current == old {
				delete(all, id)
				changed = true
			}
		}
	}
	for id, entry := range additions {
		if old, present := all[id]; !present || old != entry {
			all[id] = entry
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if len(all) > historyStagingReceiptSidecarMaxEntries {
		return errors.New("snapshots: receipt sidecar entry limit exceeded")
	}
	ids := make([][32]byte, 0, len(all))
	for id := range all {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return string(ids[i][:]) < string(ids[j][:]) })
	body := historyStagingReceiptSidecarBody{Version: 1, Root: root, Entries: make([]historyStagingReceiptCertificate, 0, len(ids))}
	for _, id := range ids {
		body.Entries = append(body.Entries, all[id])
	}
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(bodyJSON)
	doc := historyStagingReceiptSidecarDocument{historyStagingReceiptSidecarBody: body, Digest: hex.EncodeToString(digest[:])}
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if len(data) > historyStagingReceiptSidecarMaxBytes {
		return errors.New("snapshots: receipt sidecar size limit exceeded")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeSnapshotBytesAtomic(root, historyStagingReceiptSidecar, data); err != nil {
		return err
	}
	info, err = os.Lstat(filepath.Join(root, historyStagingReceiptSidecar))
	if err == nil {
		ctime, known := historyStagingChangeTime(info)
		historyStagingReceiptSidecarMemory.root, historyStagingReceiptSidecarMemory.info = root, info
		historyStagingReceiptSidecarMemory.ctime, historyStagingReceiptSidecarMemory.known = ctime, known
		historyStagingReceiptSidecarMemory.entries = all
	}
	return nil
}
