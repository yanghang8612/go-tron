// Package sync hosts the SyncService refactor (see
// docs/superpowers/specs/2026-05-19-sync-split-design.md). Slice 1 is the
// package skeleton + shared constants + pure helpers; later slices extract
// the PauseGate, Watchdog, Stats, Downloader, and Fetcher state machines
// out of the monolithic net/sync.go.
package sync
