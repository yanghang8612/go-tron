package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

const (
	repairJournalName  = ".offline-history-repair-v1.json"
	repairJournalLimit = 1 << 20
	journalPrepared    = "prepared"
	journalPublished   = "published"
	journalRebound     = "rebound"
)

// This private, advisory operation journal makes a manifest-first publication
// resumable without ever treating an untrusted journal as a cold proof. The
// source DB and immutable manifest remain authoritative on every retry.
type repairJournal struct {
	Version       uint64                             `json:"version"`
	Phase         string                             `json:"phase"`
	OldSHA        [32]byte                           `json:"old_manifest_sha256"`
	CandidateSHA  [32]byte                           `json:"candidate_manifest_sha256"`
	PlanSHA       [32]byte                           `json:"plan_sha256"`
	Epoch         uint64                             `json:"epoch"`
	PublishedUnix int64                              `json:"published_unix"`
	Protected     cleanupProtected                   `json:"protected"`
	Barrier       rawdb.HistoryStagingRouteBarrier   `json:"barrier"`
	Plan          snapshots.OfflineHistoryRepairPlan `json:"plan"`
	NewRefs       []snapshots.SegmentRef             `json:"new_refs"`
	Bindings      []rawdb.HistoryStagingColdBinding  `json:"prepared_bindings"`
	Slices        []repairTargetSlice                `json:"target_slices"`
	OtherRefsSHA  [32]byte                           `json:"unrelated_refs_sha256"`
	IntegritySHA  [32]byte                           `json:"integrity_sha256"`
}

func (j repairJournal) hash() ([32]byte, error) {
	j.IntegritySHA = [32]byte{}
	encoded, err := json.Marshal(j)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func (j repairJournal) validate() error {
	if j.Version != 1 || j.Epoch == 0 || j.PublishedUnix <= 0 || j.OldSHA == ([32]byte{}) || j.CandidateSHA == ([32]byte{}) || j.OldSHA == j.CandidateSHA || j.PlanSHA == ([32]byte{}) || len(j.Plan.SourceRefs) == 0 || len(j.NewRefs) == 0 || len(j.Bindings) == 0 ||
		len(j.Slices) == 0 || (j.Phase != journalPrepared && j.Phase != journalPublished && j.Phase != journalRebound) {
		return errors.New("repair: invalid operation journal")
	}
	sum, err := j.hash()
	if err != nil || sum != j.IntegritySHA {
		return errors.Join(errors.New("repair: operation journal integrity differs"), err)
	}
	return nil
}

func repairJournalPath(cold string) string { return filepath.Join(cold, repairJournalName) }

func readRepairJournal(cold string) (repairJournal, bool, error) {
	var j repairJournal
	path := repairJournalPath(cold)
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return j, false, nil
	}
	if err != nil {
		return j, false, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() <= 0 || info.Size() > repairJournalLimit {
		return j, false, errors.New("repair: unsafe operation journal")
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Uid != uint32(os.Geteuid()) {
		return j, false, errors.New("repair: operation journal owner differs")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return j, false, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return j, false, errors.New("repair: operation journal changed during open")
	}
	data, err := io.ReadAll(io.LimitReader(f, repairJournalLimit+1))
	if err != nil || len(data) > repairJournalLimit {
		return j, false, errors.Join(errors.New("repair: operation journal read exceeds limit"), err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&j); err != nil {
		return j, false, err
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return j, false, errors.New("repair: trailing operation journal data")
	}
	return j, true, j.validate()
}

func writeRepairJournal(cold string, next repairJournal) error {
	if next.Version != 1 {
		return errors.New("repair: invalid operation journal version")
	}
	prior, exists, err := readRepairJournal(cold)
	if err != nil {
		return err
	}
	if exists {
		if prior.Phase == journalRebound || prior.Phase == journalPublished && next.Phase != journalRebound || prior.Phase == journalPrepared && next.Phase != journalPublished ||
			prior.OldSHA != next.OldSHA || prior.CandidateSHA != next.CandidateSHA || prior.PlanSHA != next.PlanSHA || prior.Epoch != next.Epoch || prior.PublishedUnix != next.PublishedUnix ||
			!reflect.DeepEqual(prior.Protected, next.Protected) || prior.Barrier != next.Barrier || !reflect.DeepEqual(prior.Plan, next.Plan) || !reflect.DeepEqual(prior.NewRefs, next.NewRefs) || !reflect.DeepEqual(prior.Bindings, next.Bindings) || !reflect.DeepEqual(prior.Slices, next.Slices) || prior.OtherRefsSHA != next.OtherRefsSHA {
			return errors.New("repair: operation journal transition differs")
		}
	} else if next.Phase != journalPrepared {
		return errors.New("repair: operation journal must begin prepared")
	}
	next.IntegritySHA, err = next.hash()
	if err != nil {
		return err
	}
	if err := next.validate(); err != nil {
		return err
	}
	data, err := json.Marshal(next)
	if err != nil || len(data) > repairJournalLimit {
		return errors.Join(errors.New("repair: operation journal exceeds limit"), err)
	}
	tmp, err := os.CreateTemp(cold, ".offline-history-repair-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), repairJournalPath(cold)); err != nil {
		return err
	}
	dir, err := os.Open(cold)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("repair: sync journal directory: %w", err)
	}
	return nil
}

func archiveRepairJournal(cold string, expected repairJournal) error {
	if expected.Phase != journalRebound {
		return errors.New("repair: incomplete journal cannot be archived")
	}
	current, found, err := readRepairJournal(cold)
	if err != nil || !found || !reflect.DeepEqual(current, expected) {
		return errors.Join(errors.New("repair: journal changed before archive"), err)
	}
	done := filepath.Join(cold, ".offline-history-repair-done-"+hex.EncodeToString(current.OldSHA[:])+".json")
	if _, err := os.Lstat(done); !os.IsNotExist(err) {
		return errors.Join(errors.New("repair: matching completed journal already exists"), err)
	}
	if err := os.Rename(repairJournalPath(cold), done); err != nil {
		return err
	}
	dir, err := os.Open(cold)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func verifyRepairCandidateManifest(manifest *snapshots.Manifest, j repairJournal) error {
	if manifest == nil || manifest.Generation == 0 || j.CandidateSHA == ([32]byte{}) {
		return errors.New("repair: candidate manifest identity missing")
	}
	active := make(map[string]snapshots.SegmentRef, len(manifest.Segments))
	retired := make(map[string]snapshots.SegmentRef, len(manifest.Retired))
	for _, ref := range manifest.Segments {
		active[ref.Path] = ref
	}
	for _, ref := range manifest.Retired {
		retired[ref.Path] = ref
	}
	for _, ref := range j.NewRefs {
		if active[ref.Path] != ref {
			return fmt.Errorf("repair: candidate replacement %q is not active", ref.Path)
		}
	}
	for _, ref := range j.Plan.SourceRefs {
		if retired[ref.Path] != ref {
			return fmt.Errorf("repair: original history %q is not retained", ref.Path)
		}
		if _, exists := active[ref.Path]; exists {
			return fmt.Errorf("repair: original history %q remained active", ref.Path)
		}
	}
	unrelated, err := repairOtherRefsSHA(manifest, j.Plan.SourceRefs, j.NewRefs)
	if err != nil || unrelated != j.OtherRefsSHA {
		return errors.Join(errors.New("repair: unrelated active manifest refs changed"), err)
	}
	return nil
}
