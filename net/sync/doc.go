// Package sync hosts the SyncService refactor (see
// docs/superpowers/specs/2026-05-19-sync-split-design.md). Slice 1 is the
// package skeleton + shared constants + pure helpers; later slices extract
// the PauseGate, Watchdog, Stats, Downloader, and Fetcher state machines
// out of the monolithic net/sync.go.
//
// TODO(slice-2): decide whether to introduce service.go here to host the
// SyncService type itself, or keep it in net/sync.go until Slice 6 retires
// the monolith. The plan's literal Slice 1 promised a service.go skeleton;
// we deferred to avoid shadowing the production SyncService type and to let
// Slice 2 make the call once PauseGate's home is known.
package sync
