// Package fetcher will host the gossip-broadcast block intake path that
// today lives in net/handler.go's handleBlock branch after IsSyncing()
// returns false. Slice 5 of the refactor populates it; for slice 1 the
// package exists only so later slices can land without touching the
// directory layout again.
package fetcher
