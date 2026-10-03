package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestHistoryStagingOfflineCertificateRejectsReplacementAndCancellation(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	p, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	mask := make([]bool, len(blocks))
	for i := range mask {
		mask[i] = true
	}
	spans, err := p.Build(context.Background(), blocks, mask)
	if err != nil {
		t.Fatal(err)
	}
	check, err := p.PrepareOfflineBinding(context.Background(), rawdb.HistoryStagingColdBinding{Bucket: 1, Spans: spans}, blocks)
	if err != nil {
		t.Fatal(err)
	}
	if p.Stats().SemanticBindingVerifies != 1 {
		t.Fatal("proof not verified exactly once")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := check(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(check(ctx), context.Canceled) {
		t.Fatal("certificate ignored cancellation")
	}
	path := filepath.Join(dir, manifest.Segments[0].Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	replacement := path + ".new"
	if err := os.WriteFile(replacement, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if err := check(context.Background()); err == nil {
		t.Fatal("same-byte different-inode replacement accepted")
	}
}

func BenchmarkHistoryStagingOfflineWorkerProof(b *testing.B) {
	for _, count := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers-%d", count), func(b *testing.B) {
			dir, manifest, blocks := stagingDenseColdFixture(b, "v5", 512)
			baseline, err := NewHistoryStagingColdProver(dir, manifest)
			if err != nil {
				b.Fatal(err)
			}
			mask := make([]bool, len(blocks))
			for i := range mask {
				mask[i] = true
			}
			spans, err := baseline.Build(context.Background(), blocks, mask)
			if err != nil {
				b.Fatal(err)
			}
			baseline.Close()
			binding := rawdb.HistoryStagingColdBinding{Bucket: 1, Spans: spans}
			provers := make([]*HistoryStagingColdProver, count)
			for i := range provers {
				provers[i], err = NewHistoryStagingColdProver(dir, manifest)
				if err != nil {
					b.Fatal(err)
				}
				provers[i].EnableReaderReuse()
				defer provers[i].Close()
			}
			const buckets = 16
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				var group sync.WaitGroup
				errs := make(chan error, count)
				for id := range provers {
					group.Add(1)
					go func(id int) {
						defer group.Done()
						for task := id; task < buckets; task += count {
							check, err := provers[id].PrepareOfflineBinding(context.Background(), binding, blocks)
							if err == nil {
								err = check(context.Background())
							}
							if err != nil {
								errs <- err
								return
							}
						}
					}(id)
				}
				group.Wait()
				close(errs)
				for err := range errs {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			var verifies, opens, auth uint64
			for _, p := range provers {
				s := p.Stats()
				verifies += s.SemanticBindingVerifies
				opens += s.HistoryOpens
				auth += s.FullTrioAuthenticates
			}
			if verifies != uint64(b.N*buckets) {
				b.Fatalf("proof work=%d", verifies)
			}
			b.ReportMetric(float64(buckets), "buckets/op")
			b.ReportMetric(float64(buckets*1024*512), "records/op")
			b.ReportMetric(float64(opens), "reader-opens")
			b.ReportMetric(float64(auth+baseline.Stats().FullTrioAuthenticates), "full-trio-audits")
			b.ReportMetric(float64(verifies)/float64(b.N), "semantic-verifies/op")
		})
	}
}

func TestHistoryStagingBulkRangesMatchPointReadsAndRejectWrongCanonical(t *testing.T) {
	p, ref, _, blocks, counter := stagingCountedProver(t, "reference", 8)
	defer p.Close()
	counter.reads = 0
	var point []rawdb.StateTxRange
	for _, block := range blocks {
		row, table, found, err := findStateDomainChangeBinaryTxRangeForBlock(p.open.history, p.open.historySize, ref, p.open.header, block.Number)
		if err != nil || !table || !found {
			t.Fatalf("point range: %v", err)
		}
		point = append(point, *row)
	}
	pointReads := counter.reads
	counter.reads = 0
	got, err := historyStagingCertifiedRanges(context.Background(), p.open.history, p.open.historySize, ref, p.open.header, blocks)
	if err != nil {
		t.Fatal(err)
	}
	bulkReads := counter.reads
	if len(got) != len(point) {
		t.Fatal("range count mismatch")
	}
	for i := range got {
		if got[i] != point[i] {
			t.Fatalf("range %d mismatch", i)
		}
	}
	if bulkReads >= pointReads/100 {
		t.Fatalf("point=%d bulk=%d", pointReads, bulkReads)
	}
	t.Logf("1024 complete canonical block ranges: point ReadAt=%d bulk ReadAt=%d", pointReads, bulkReads)
	wrong := append([]rawdb.HistoryStagingBlockProof(nil), blocks...)
	wrong[512].Hash[0] ^= 1
	if _, err := historyStagingCertifiedRanges(context.Background(), p.open.history, p.open.historySize, ref, p.open.header, wrong); err == nil {
		t.Fatal("bulk skipped canonical mismatch")
	}
	wrong = append([]rawdb.HistoryStagingBlockProof(nil), blocks...)
	wrong[100].BeginTxNum++
	if _, err := historyStagingCertifiedRanges(context.Background(), p.open.history, p.open.historySize, ref, p.open.header, wrong); err == nil {
		t.Fatal("bulk skipped txrange mismatch")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := historyStagingCertifiedRanges(ctx, p.open.history, p.open.historySize, ref, p.open.header, blocks); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
