package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestCompactV4VerifiedRestoreRejectsV3BeforeMutation(t *testing.T) {
	for _, part := range []string{"version", "scheme", "element", "complete"} {
		t.Run(part, func(t *testing.T) {
			dir, identity, _, _ := writeVerifiableCommitmentManifest(t)
			m, err := LoadManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			if part == "version" || part == "complete" {
				m.Version = 4
			}
			if part == "scheme" || part == "complete" {
				m.CommitmentScheme = "state-flat-latest-muhash3072-compact-v3"
			}
			if part == "element" || part == "complete" {
				m.ElementVersion = 3
			}
			old, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			// Write the unsupported fixture directly: PublishManifest must refuse it.
			if err := os.WriteFile(filepath.Join(dir, ManifestFile), old, 0600); err != nil {
				t.Fatal(err)
			}
			disk := rawdb.NewMemoryDatabase()
			defer disk.Close()
			if _, err := RestoreLatestFromVerifiedManifest(disk, dir, identity); err == nil {
				t.Fatal("restored old manifest")
			}
			it := disk.NewIterator(nil, nil)
			defer it.Release()
			if it.Next() || it.Error() != nil {
				t.Fatal("rejected import mutated empty target", it.Error())
			}
		})
	}
}
