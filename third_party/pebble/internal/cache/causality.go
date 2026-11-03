package cache

// ResidentActivity counts payload membership, not Value allocation/refcounts.
// All writes and the corresponding resident-size read use the existing shard
// mutex. A detached Value held by a reader is not resident. Ghost bytes never
// enter the resident conservation equation. Counters start at cache creation.
type ResidentActivity struct {
	AddedBytes, ResizeGrowBytes, ResizeShrinkBytes                             int64
	RemovedReserveBytes, RemovedFileBytes, RemovedClockBytes                   int64
	RemovedDeleteBytes, RemovedOtherBytes                                      int64
	FileGhostRemovedBytes, DeleteGhostRemovedBytes                             int64
	ClockGhostRemovedBytes, PromotionGhostRemovedBytes, OtherGhostRemovedBytes int64
	ReserveShardCalls, ReleaseShardCalls                                       int64
	ReserveAddedBytes, ReserveReleasedBytes                                    int64
	FileEvictShardCalls                                                        int64
}

// Balance is cumulative resident additions minus removals. Summing each shard's
// co-sampled balance must equal the co-sampled payload even though different
// shards are sampled at different instants. This is not a global atomic cut.
func (a ResidentActivity) Balance() int64 {
	return a.AddedBytes + a.ResizeGrowBytes - a.ResizeShrinkBytes - a.RemovedReserveBytes - a.RemovedFileBytes - a.RemovedClockBytes - a.RemovedDeleteBytes - a.RemovedOtherBytes
}
func (a *ResidentActivity) add(b ResidentActivity) {
	a.AddedBytes += b.AddedBytes
	a.ResizeGrowBytes += b.ResizeGrowBytes
	a.ResizeShrinkBytes += b.ResizeShrinkBytes
	a.RemovedReserveBytes += b.RemovedReserveBytes
	a.RemovedFileBytes += b.RemovedFileBytes
	a.RemovedClockBytes += b.RemovedClockBytes
	a.RemovedDeleteBytes += b.RemovedDeleteBytes
	a.RemovedOtherBytes += b.RemovedOtherBytes
	a.FileGhostRemovedBytes += b.FileGhostRemovedBytes
	a.DeleteGhostRemovedBytes += b.DeleteGhostRemovedBytes
	a.ClockGhostRemovedBytes += b.ClockGhostRemovedBytes
	a.PromotionGhostRemovedBytes += b.PromotionGhostRemovedBytes
	a.OtherGhostRemovedBytes += b.OtherGhostRemovedBytes
	a.ReserveShardCalls += b.ReserveShardCalls
	a.ReleaseShardCalls += b.ReleaseShardCalls
	a.ReserveAddedBytes += b.ReserveAddedBytes
	a.ReserveReleasedBytes += b.ReserveReleasedBytes
	a.FileEvictShardCalls += b.FileEvictShardCalls
}

type evictionReason uint8

const (
	reasonClock evictionReason = iota
	reasonReserve
	reasonFile
	reasonDelete
)

func (c *shard) accountRemoval(n int64, reason evictionReason) {
	switch reason {
	case reasonReserve:
		c.activity.RemovedReserveBytes += n
	case reasonFile:
		c.activity.RemovedFileBytes += n
	case reasonDelete:
		c.activity.RemovedDeleteBytes += n
	default:
		c.activity.RemovedClockBytes += n
	}
}
