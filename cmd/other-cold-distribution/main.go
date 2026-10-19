// other-cold-distribution reads a pinned copy of immutable history trios. It
// never opens the node databases or mutates its source. The optional CDC
// comparison publishes and migrates manifests only in new private directories.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
)

type stats struct {
	Rows                uint64
	Present             uint64
	Payload             uint64
	KeyBytes            uint64
	Zero                uint64
	Widths              map[int]uint64
	AddressElisionBytes uint64
}
type sample struct {
	Path    string
	From    uint64
	To      uint64
	Rows    uint64
	Bytes   uint64
	SHA256  string
	Domains map[string]*stats
}
type report struct {
	Complete           bool
	ManifestSHA256     string
	ManifestGeneration uint64
	Started            time.Time
	Finished           time.Time
	Samples            []sample
	Total              map[string]*stats
}

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func zeros(v []byte) int {
	i := 0
	for i < len(v) && v[i] == 0 {
		i++
	}
	return i
}
func observe(m map[string]*stats, name string, c *rawdb.StateDomainChange) error {
	s := m[name]
	if s == nil {
		s = &stats{Widths: map[int]uint64{}}
		m[name] = s
	}
	s.Rows++
	s.KeyBytes += uint64(len(c.Key))
	if !c.PrevExists {
		return nil
	}
	s.Present++
	s.Payload += uint64(len(c.Prev))
	if len(c.Prev) == 8 || len(c.Prev) == 32 {
		s.Widths[len(c.Prev)-zeros(c.Prev)]++
		if zeros(c.Prev) == len(c.Prev) {
			s.Zero++
		}
	}
	if c.FlatDomain == rawdb.StateFlatDomainAccountLatest {
		env, e := state.DecodeStateAccountV3(c.Prev)
		if e != nil {
			return e
		}
		acc, e := types.UnmarshalAccountStorageCoreV4(env.AccountProto)
		if e != nil {
			return e
		}
		if acc.Address() == c.Owner && len(acc.Proto().Address) > 0 {
			acc.Proto().Address = nil
			b, e := acc.MarshalStorageCoreV4()
			if e != nil {
				return e
			}
			env.AccountProto = b
			b, e = env.Encode()
			if e != nil {
				return e
			}
			s.AddressElisionBytes += uint64(len(c.Prev) - len(b))
		}
	}
	return nil
}
func run() error {
	dir := flag.String("dir", "", "pinned immutable file directory")
	out := flag.String("out", "", "exclusive report file")
	compare := flag.Bool("compare-cdc", false, "compare existing CDC codec on new private trio copies")
	accessors := flag.Bool("compare-accessors", false, "measure page compression of complete selected sidecars")
	flag.Parse()
	if *dir == "" || *out == "" {
		return errors.New("dir and out required")
	}
	if *accessors {
		return compareAccessors(*dir, *out)
	}
	if *compare {
		return compareCDC(*dir, *out)
	}
	raw, e := os.ReadFile(filepath.Join(*dir, "manifest.json"))
	if e != nil {
		return e
	}
	var m snapshots.Manifest
	if e = json.Unmarshal(raw, &m); e != nil {
		return e
	}
	hash := sha256.Sum256(raw)
	r := report{ManifestSHA256: hex.EncodeToString(hash[:]), ManifestGeneration: m.Generation, Started: time.Now().UTC(), Total: map[string]*stats{}}
	refs := []snapshots.SegmentRef{}
	for _, s := range m.Segments {
		if s.Dataset == snapshots.SegmentDatasetStateDomainChange && s.Kind == snapshots.SegmentHistory {
			refs = append(refs, s)
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].FromTxNum < refs[j].FromTxNum })
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	for _, ref := range refs {
		file, e := os.ReadFile(filepath.Join(*dir, ref.Path))
		if e != nil {
			return e
		}
		sum := sha256.Sum256(file)
		got := "sha256:" + hex.EncodeToString(sum[:])
		if uint64(len(file)) != ref.Size || got != ref.Checksum {
			return errors.New("history SHA/size mismatch")
		}
		file = nil
		cfg, ok := snapshots.DefaultDomainRegistry().ConfigForRef(ref)
		if !ok || cfg.IterateHistoryRange == nil {
			return errors.New("no streaming history reader")
		}
		s := sample{Path: ref.Path, From: ref.FromTxNum, To: ref.ToTxNum, Bytes: ref.Size, SHA256: got, Domains: map[string]*stats{}}
		e = cfg.IterateHistoryRange(*dir, &m, ref, ref.FromTxNum, ref.ToTxNum, func(c *rawdb.StateDomainChange) (bool, error) {
			if e := ctx.Err(); e != nil {
				return false, e
			}
			s.Rows++
			name := c.FlatDomain.String()
			if c.FlatDomain == rawdb.StateFlatDomainKVLatest {
				name += "/" + kvdomains.Name(c.Domain)
				if c.Domain == kvdomains.WitnessVoteState {
					if bytes.Equal(c.Key, []byte("VotesIndex")) {
						name += "/VotesIndex"
					} else {
						name += "/voter-record"
					}
				}
			}
			if e := observe(s.Domains, name, c); e != nil {
				return false, e
			}
			e := observe(r.Total, name, c)
			return e == nil, e
		})
		if e != nil {
			return e
		}
		r.Samples = append(r.Samples, s)
		fmt.Fprintln(os.Stderr, "sample", ref.Path, "rows", s.Rows)
	}
	r.Complete = true
	r.Finished = time.Now().UTC()
	f, e := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return errors.Join(enc.Encode(r), f.Sync(), f.Close())
}
