//go:build !linux

package snapshots

import (
	"context"
	"os"
)

func adviseHistoryPointReads(context.Context, *os.File) {}
