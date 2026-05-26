package state

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNoLegacyRootedStoreOrFlatCodeInProductionPaths(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	var violations []string
	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", "build", "vendor":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(data)
		if strings.Contains(text, "NewRootedStore(") {
			violations = append(violations, rel+": production code must not wrap execution DBs in RootedStore")
		}
		if strings.Contains(text, "rawdb.ReadCode(") || strings.Contains(text, "rawdb.WriteCode(") || strings.Contains(text, "rawdb.DeleteCode(") {
			violations = append(violations, rel+": production code must not read/write legacy address-keyed CodeStore")
		}
		if strings.Contains(text, "rawdb.ReadStorage(") || strings.Contains(text, "rawdb.WriteStorage(") {
			violations = append(violations, rel+": production code must not read/write legacy address-keyed ContractStorage")
		}
		if strings.Contains(text, "rawdb.ReadContract(") || strings.Contains(text, "rawdb.WriteContract(") ||
			strings.Contains(text, "rawdb.ReadContractABI(") || strings.Contains(text, "rawdb.WriteContractABI(") ||
			strings.Contains(text, "rawdb.DeleteContract(") || strings.Contains(text, "rawdb.DeleteContractABI(") {
			violations = append(violations, rel+": production code must not read/write legacy address-keyed ContractStore/ABIStore")
		}
		if strings.Contains(text, "rawdb.ReadWitness(") || strings.Contains(text, "rawdb.WriteWitness(") {
			violations = append(violations, rel+": production code must not read/write legacy address-keyed WitnessStore")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) > 0 {
		t.Fatalf("legacy flat-state guard failed:\n%s", strings.Join(violations, "\n"))
	}
}
