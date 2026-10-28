//go:build linux

package snapshots

import (
	"context"
	"os"

	"golang.org/x/sys/unix"
)

func adviseHistoryPointReads(ctx context.Context, file *os.File) {
	if ctx != nil && ctx.Value(historyPointReadKey{}) != nil {
		// The hint is descriptor-local. Point queries explicitly request their
		// bounded windows; fetching unrelated neighboring pages for thousands
		// of independent files amplifies cold I/O. Sequential builders/exporters
		// never carry this context. Failure affects performance only.
		_ = unix.Fadvise(int(file.Fd()), 0, 0, unix.FADV_RANDOM)
	}
}
