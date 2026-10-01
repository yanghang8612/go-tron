package main

import (
	"strings"
	"testing"
	"time"
)

type historyStagingProgressLines chan string

func (lines historyStagingProgressLines) Write(data []byte) (int, error) {
	lines <- string(data)
	return len(data), nil
}

func TestHistoryStagingCLIProgressReportsLongBucketOnStderr(t *testing.T) {
	lines := make(historyStagingProgressLines, 8)
	progress := startHistoryStagingCLIProgress(lines, "migrate", "inspect-boundary", 10*time.Millisecond)
	defer progress.close()
	<-lines // Initial status is available before the first long scan.
	progress.stage.Store("plan-buckets")
	progress.bucket.Store(7)
	progress.completed.Store(6)
	progress.total.Store(100)
	select {
	case line := <-lines:
		for _, want := range []string{"phase=migrate", "stage=plan-buckets", "bucket=7", "completed=6", "total=100"} {
			if !strings.Contains(line, want) {
				t.Fatalf("progress line %q omits %q", line, want)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("long bucket produced no periodic stderr progress")
	}
}
