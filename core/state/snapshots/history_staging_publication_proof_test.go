package snapshots

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func publicationProofFixture(t *testing.T) (string, *Manifest, *Manifest, rawdb.HistoryStagingColdBinding, *HistoryStagingPhysicalFactCollector) {
	t.Helper()
	dir, proved, blocks := historyStagingProofFixture(t)
	proved.Chain.GenesisHash = (common.Hash{1}).Hex()
	ctx, facts, err := WithHistoryStagingPhysicalFacts(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	mask := make([]bool, len(blocks))
	for i := range mask {
		mask[i] = true
	}
	spans, err := BuildHistoryStagingColdSpans(ctx, dir, proved, blocks, mask)
	if err != nil {
		t.Fatal(err)
	}
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i := range ranges {
		n := uint64(2048 + i)
		ranges[i] = &rawdb.StateTxRange{BlockNum: n, BlockHash: common.Hash{byte(n >> 8), byte(n)}, BeginTxNum: n, EndTxNum: n}
	}
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 2048, ToTxNum: 3071, Path: stateDomainChangeHistorySegmentPath(2048, 3071)}
	history, index, accessor, err := writeHistorySegmentFiles(dir, ref, nil, ranges)
	if err != nil {
		t.Fatal(err)
	}
	live := cloneManifest(proved)
	live.Generation++
	live.VisibleTxEnd = 3071
	live.Segments = append(live.Segments, history, index, accessor)
	if err := live.Validate(); err != nil {
		t.Fatal(err)
	}
	binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion, Bucket: 1, Epoch: 1, BindingEpoch: 3, ManifestEpoch: proved.Generation, Spans: spans}
	return dir, proved, live, binding, facts
}

func TestHistoryStagingPublicationRechecksUnchangedTrioAcrossAppend(t *testing.T) {
	_, proved, live, binding, facts := publicationProofFixture(t)
	original := binding
	original.Spans = append([]rawdb.HistoryStagingColdSpan(nil), binding.Spans...)
	checkpoints := 0
	ctx := maintenance.WithWorkCheckpoint(context.Background(), func(uint64) error { checkpoints++; return errors.New("must not yield in publication guard") })
	got, err := facts.RecheckColdBindingPublication(ctx, proved, live, binding)
	if err != nil {
		t.Fatal(err)
	}
	expected := original
	expected.ManifestEpoch = live.Generation
	if !reflect.DeepEqual(got, expected) || !reflect.DeepEqual(binding, original) || checkpoints != 0 {
		t.Fatalf("revalidation changed proof or yielded: got=%+v checkpoints=%d", got, checkpoints)
	}
	if _, err := facts.RecheckColdBindingPublication(ctx, proved, proved, binding); err != nil {
		t.Fatalf("unchanged generation: %v", err)
	}
}

func TestHistoryStagingPublicationRejectsChangedProofDependencies(t *testing.T) {
	for _, fault := range []string{"retired-only", "changed-ref", "missing-companion", "missing-original", "reset", "chain", "nil-chain", "regressed", "shrunk", "missing-fact", "missing-state", "wrong-fact-id", "cancel", "same-size-rewrite", "same-bytes-new-inode"} {
		t.Run(fault, func(t *testing.T) {
			dir, proved, live, binding, facts := publicationProofFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch fault {
			case "retired-only":
				live.Retired = append(live.Retired, live.Segments[:3]...)
				live.Segments = live.Segments[3:]
			case "changed-ref":
				live.Segments[0].Size++
			case "missing-companion":
				live.Segments = append(live.Segments[:1], live.Segments[2:]...)
			case "missing-original":
				proved.Segments = nil
			case "reset":
				live.HistoryStagingResetEpoch++
			case "chain":
				live.Chain.NetworkID++
			case "nil-chain":
				proved.Chain = nil
			case "regressed":
				live.Generation = 0
			case "shrunk":
				live.VisibleTxStart++
			case "missing-fact":
				delete(facts.entries, binding.Spans[0].ContentID)
			case "missing-state":
				delete(facts.states, binding.Spans[0].ContentID)
			case "wrong-fact-id":
				id := binding.Spans[0].ContentID
				fact := facts.entries[id]
				fact.Refs[0].Checksum = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
				facts.entries[id] = fact
			case "cancel":
				cancel()
			case "same-size-rewrite", "same-bytes-new-inode":
				path := filepath.Join(dir, proved.Segments[0].Path)
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if fault == "same-size-rewrite" {
					data[len(data)-1] ^= 1
					if err := os.WriteFile(path, data, info.Mode()); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := os.WriteFile(path+".replacement", data, info.Mode()); err != nil {
						t.Fatal(err)
					}
					if err := os.Rename(path+".replacement", path); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
			}
			got, err := facts.RecheckColdBindingPublication(ctx, proved, live, binding)
			if err == nil || got.BindingEpoch != 0 || len(got.Spans) != 0 {
				t.Fatalf("changed dependency accepted: %+v, %v", got, err)
			}
			if fault == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("wrong cancellation: %v", err)
			}
		})
	}
}
