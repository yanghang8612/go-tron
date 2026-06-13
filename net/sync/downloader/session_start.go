package downloader

// SessionStartupInput is the canonical chain boundary and operator knobs needed
// to plan one downloader session restart.
type SessionStartupInput struct {
	Head         uint64
	RestoreLimit int
}

// SessionStartupPlan describes the persistence and local-runtime boundaries a
// sync session should apply before asking peers for more inventory.
type SessionStartupPlan struct {
	InventoryFloor          uint64
	DeleteImportedThrough   uint64
	RestoreStagedBodiesFrom uint64
	RestoreLimit            int
	PruneStaleTail          bool
	ResetPeerJoinThrottle   bool
}

// PlanSessionStartup derives restart boundaries from the current canonical
// head. SyncService owns DB writes and timers; the downloader package owns the
// staging/import boundary decisions.
func PlanSessionStartup(in SessionStartupInput) SessionStartupPlan {
	restoreLimit := in.RestoreLimit
	if restoreLimit < 0 {
		restoreLimit = 0
	}
	restoreFrom := in.Head
	if restoreFrom != ^uint64(0) {
		restoreFrom++
	}
	return SessionStartupPlan{
		InventoryFloor:          in.Head,
		DeleteImportedThrough:   in.Head,
		RestoreStagedBodiesFrom: restoreFrom,
		RestoreLimit:            restoreLimit,
		PruneStaleTail:          restoreLimit > 0,
		ResetPeerJoinThrottle:   true,
	}
}
