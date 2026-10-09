package maintenance

import "context"

type workCheckpointKey struct{}

// WithWorkCheckpoint attaches a cooperative runtime work boundary. Callbacks
// may wait or release the maintenance lease, so callers must be outside writer
// and publication locks. A nil callback leaves offline callers unchanged.
func WithWorkCheckpoint(ctx context.Context, checkpoint func(uint64) error) context.Context {
	return context.WithValue(ctx, workCheckpointKey{}, checkpoint)
}

// HasWorkCheckpoint lets dependency-sharing code avoid waiting for another
// task that may need the same maintenance lease while this task is paused.
func HasWorkCheckpoint(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	checkpoint, _ := ctx.Value(workCheckpointKey{}).(func(uint64) error)
	return checkpoint != nil
}

func WorkCheckpoint(ctx context.Context, workBytes uint64) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if checkpoint, ok := ctx.Value(workCheckpointKey{}).(func(uint64) error); ok && checkpoint != nil {
		return checkpoint(workBytes)
	}
	return nil
}
