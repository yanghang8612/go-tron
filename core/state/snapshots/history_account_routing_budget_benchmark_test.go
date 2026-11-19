package snapshots

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// The fixture scales dictionary budgets to 750,000 and 1,500,000 keys,
// preserving the 1:2 ratio. It retains the actual 32 MiB ETL limit and spills
// each dictionary. Both modes use the same writer, inputs and output oracle.
func BenchmarkAccountRoutingDictionaryBudgetLifecycle(b *testing.B) {
	ctx := context.Background()
	b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	b.Setenv(manifestCacheBudgetEnv, "805306368")
	b.Cleanup(loadedManifestCache.clear)
	dir := b.TempDir()
	const dictionaryKeys = 750_000
	first := buildRoutingBudgetBenchmarkHistory(b, dir, 1, 1, 1)
	second := buildRoutingBudgetBenchmarkHistory(b, dir, 2, 1, dictionaryKeys)
	third := buildRoutingBudgetBenchmarkHistory(b, dir, 3, 3*dictionaryKeys/4, dictionaryKeys)
	oldRoute := benchmarkOldRoute(b, dir, first, 1)
	manifestRefs := append(append(append(append([]SegmentRef(nil), first...), second...), third...), oldRoute)

	for _, expand := range []bool{false, true} {
		b.Run(fmt.Sprintf("scaled-expansion=%t", expand), func(b *testing.B) {
			gate := maintenance.NewHeavyWorkGate()
			var outputBytes, rewrites, spills uint64
			var maxLease, recoveryElapsed, queryElapsed time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m := NewManifest(1, 3, manifestRefs)
				if err := PublishManifest(dir, m); err != nil {
					b.Fatal(err)
				}
				covered := 1
				for covered < 3 {
					release, ok := gate.TryAcquireWithCooldown(3 * time.Second)
					if !ok {
						b.Fatal("unexpected gate refusal after recovery")
					}
					started := time.Now()
					prior, _ := accountRoutingRef(m, 0, ^uint64(0))
					old, err := openAccountHistoryRouting(ctx, dir, prior)
					if err != nil {
						release()
						b.Fatal(err)
					}
					budget := accountRoutingDictionaryBudget{
						remaining: dictionaryKeys, extra: dictionaryKeys,
						permitExtra: func() bool {
							// Observe the identical spill before deciding if this
							// simulated resource check permits the second source.
							n := routingBudgetSpills(b, dir)
							if n == 0 {
								b.Error("no actual ETL spill before the budget boundary")
							}
							spills += n
							return expand
						},
					}
					ref, built, err := buildAccountHistoryRoutingContext(ctx, dir, m, 64, old, budget)
					old.Close()
					if err != nil || !built {
						release()
						b.Fatalf("routing refresh: %t %v", built, err)
					}
					m, err = NewAggregator(dir).Integrate(1, 3, []SegmentRef{ref})
					release()
					work := time.Since(started)
					if err != nil {
						b.Fatal(err)
					}
					maxLease = max(maxLease, work)
					rewrites++
					outputBytes += ref.Size
					index, err := openAccountHistoryRouting(ctx, dir, ref)
					if err != nil {
						b.Fatal(err)
					}
					covered = len(index.metadata.covered)
					index.Close()
					checks := []StateHistoryQuery{
						routingBudgetBenchmarkQuery(1, rawdb.StateFlatDomainAccountLatest),
						routingBudgetBenchmarkQuery(3*dictionaryKeys/4, rawdb.StateFlatDomainKVGeneration),
						routingBudgetBenchmarkQuery(3_000_000, rawdb.StateFlatDomainAccountLatest),
					}
					loadedManifestCache.clear()
					manager, err := OpenManager(dir)
					if err != nil {
						b.Fatal(err)
					}
					queryStarted := time.Now()
					rows, err := manager.FirstStateDomainChangesContext(ctx, 2, 3, checks)
					queryElapsed += time.Since(queryStarted)
					if err != nil || rows[0] == nil || rows[0].TxNum != 2 || rows[1] == nil || rows[1].TxNum != 3 || rows[2] != nil {
						b.Fatalf("covered and fallback query differs: %v %v", rows, err)
					}
					if covered == 3 {
						if err := CheckAccountHistoryRouting(dir, ref); err != nil {
							b.Fatal(err)
						}
					}
					deadline := started.Add(work + historyCompactionRecovery(work, false))
					sleepStarted := time.Now()
					time.Sleep(time.Until(deadline))
					recoveryElapsed += time.Since(sleepStarted)
				}
			}
			b.ReportMetric(float64(rewrites)/float64(b.N), "route-rewrites/op")
			b.ReportMetric(float64(outputBytes)/float64(b.N), "route-output-B/op")
			b.ReportMetric(float64(spills)/float64(b.N), "observed-boundary-spills/op")
			b.ReportMetric(float64(maxLease.Nanoseconds()), "max-lease-ns")
			b.ReportMetric(float64(recoveryElapsed.Nanoseconds())/float64(b.N), "recovery-sleep-ns/op")
			b.ReportMetric(float64(queryElapsed.Nanoseconds())/float64(rewrites), "query-ns/refresh")
			b.ReportMetric(2*dictionaryKeys, "new-dictionary-keys/op")
		})
	}
}

func routingBudgetBenchmarkQuery(n uint64, domain rawdb.StateFlatDomain) StateHistoryQuery {
	_, addr := benchmarkRouteKey(n)
	return StateHistoryQuery{FlatDomain: domain, Owner: addr}
}

func buildRoutingBudgetBenchmarkHistory(b *testing.B, dir string, tx, first, count uint64) []SegmentRef {
	b.Helper()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	hash := common.Hash{byte(tx)}
	if err := rawdb.WriteStateTxRange(db, tx, hash, tx, tx); err != nil {
		b.Fatal(err)
	}
	for i := uint64(0); i < count; i++ {
		flat := rawdb.StateFlatDomainAccountLatest
		if tx == 3 && i%2 == 0 {
			flat = rawdb.StateFlatDomainKVGeneration
		}
		q := routingBudgetBenchmarkQuery(first+i, flat)
		row := &rawdb.StateDomainChange{BlockNum: tx, BlockHash: hash, TxNum: tx, Seq: i, FlatDomain: q.FlatDomain, Owner: q.Owner}
		if err := rawdb.WriteStateDomainChangeRow(db, row); err != nil {
			b.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, tx, tx, fmt.Sprintf("history/state-domain-change-budget-%d.seg", tx))
	if err != nil {
		b.Fatal(err)
	}
	return refs
}

func routingBudgetSpills(b *testing.B, dir string) uint64 {
	b.Helper()
	var runs uint64
	err := filepath.WalkDir(filepath.Join(dir, "etl"), func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasPrefix(d.Name(), "run-") {
			runs++
		}
		return err
	})
	if err != nil && !os.IsNotExist(err) {
		b.Error(err)
	}
	return runs
}

func benchmarkRouteKey(n uint64) (key [21]byte, owner common.Address) {
	key[0] = byte(rawdb.StateFlatDomainAccountLatest)
	binary.BigEndian.PutUint64(key[13:], n)
	owner = common.Address(key)
	owner[0] = common.AddressPrefixMainnet
	return key, owner
}

// Build an authenticated old routing object with conservative extra rows.
// Small canonical sources keep fixture preparation bounded. Every covered
// canonical key still has its real upper bound and strict verification checks
// the complete authenticated object, including the extra old rows.
func benchmarkOldRoute(b *testing.B, dir string, refs []SegmentRef, count uint64) SegmentRef {
	b.Helper()
	var pages, directories bytes.Buffer
	w := &accountRoutingPageWriter{
		data: bufio.NewWriter(&pages), directories: bufio.NewWriter(&directories), maxTx: count,
	}
	for n := uint64(1); n <= 1_000_000; n++ {
		key, _ := benchmarkRouteKey(n)
		bound := min(n, count)
		if err := w.put(key[:], bound); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.finish(); err != nil {
		b.Fatal(err)
	}
	header := make([]byte, accountRoutingHeaderBytes)
	copy(header, accountRoutingMagic[:])
	binary.BigEndian.PutUint32(header[8:], 1)
	binary.BigEndian.PutUint32(header[12:], uint32(count))
	binary.BigEndian.PutUint64(header[16:], w.rows)
	binary.BigEndian.PutUint64(header[24:], w.pages)
	binary.BigEndian.PutUint64(header[32:], w.dirPages)
	binary.BigEndian.PutUint64(header[40:], 1)
	binary.BigEndian.PutUint64(header[48:], count)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	m := manifestLookupView(NewManifest(1, count, refs))
	for _, history := range m.Segments {
		if history.Kind != SegmentHistory {
			continue
		}
		accessor, ok := cfg.HistoryAccessorRef(m, history)
		if !ok {
			b.Fatal("missing lifecycle fixture accessor")
		}
		id := systemHistoryRoutingPairID(history, accessor)
		header = append(header, id[:]...)
	}
	header = append(header, w.roots...)
	root := sha256.Sum256(header)
	data := append(header, directories.Bytes()...)
	data = append(data, pages.Bytes()...)
	hash := sha256.Sum256(data)
	ref := SegmentRef{
		Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccountHistoryRouting,
		Rows: w.rows, FromTxNum: 1, ToTxNum: count, Size: uint64(len(data)),
		Checksum: fmt.Sprintf("sha256:%x", hash), LookupRoot: fmt.Sprintf("sha256:%x", root),
		Path: fmt.Sprintf("history/account-routing-1-%d-%x.route", count, hash),
	}
	if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0644); err != nil {
		b.Fatal(err)
	}
	return ref
}
