package rawdb

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestNoProductionDirectHotBlockKVReads(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBCalls(t, root, map[string]struct{}{
		"ReadBlockKV": {},
	}, nil)
	if len(offenders) > 0 {
		t.Fatalf("production code must use freezer-aware chain accessors instead of hot-only rawdb calls:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestNoUnexpectedProductionDirectRawFreezerReads(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBCalls(t, root, map[string]struct{}{
		"ReadBlockRaw":            {},
		"ReadTransactionInfosRaw": {},
		"ReadBlockStateRootRaw":   {},
	}, map[string]map[string]struct{}{
		"cmd/gtron/freezer_adapter.go": {
			"ReadBlockRaw":            {},
			"ReadTransactionInfosRaw": {},
			"ReadBlockStateRootRaw":   {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production code must route raw freezer reads through the freezer adapter boundary:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestSnapshotPublishersUseStrictTransactionInfoReads(t *testing.T) {
	repoRoot := findRepoRoot(t)
	snapshotRoot := filepath.Join(repoRoot, "core", "state", "snapshots")
	offenders := auditForbiddenRawDBCalls(t, snapshotRoot, map[string]struct{}{
		"ReadTransactionInfosByBlock": {},
	}, nil)
	if len(offenders) > 0 {
		t.Fatalf("snapshot builders must use ReadTransactionInfosByBlockStrict so corrupt TransactionRet rows fail cold coverage publishing:\n%s", strings.Join(offenders, "\n"))
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository root with go.mod not found")
		}
		dir = parent
	}
}

func auditForbiddenRawDBCalls(t *testing.T, root string, forbidden map[string]struct{}, allowed map[string]map[string]struct{}) []string {
	t.Helper()
	var offenders []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", ".codex", "build", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if strings.HasPrefix(path, filepath.Join(root, "core", "rawdb")+string(os.PathSeparator)) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rawdbNames := rawdbImportNames(file)
		if len(rawdbNames) == 0 {
			return nil
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				ident, ok := fun.X.(*ast.Ident)
				if !ok {
					return true
				}
				if _, imported := rawdbNames[ident.Name]; !imported {
					return true
				}
				if _, banned := forbidden[fun.Sel.Name]; banned && !isAllowedRawDBCall(root, path, fun.Sel.Name, allowed) {
					offenders = append(offenders, formatAuditOffender(fset, root, path, call.Pos(), ident.Name+"."+fun.Sel.Name))
				}
			case *ast.Ident:
				if _, dotImported := rawdbNames["."]; !dotImported {
					return true
				}
				if _, banned := forbidden[fun.Name]; banned {
					if isAllowedRawDBCall(root, path, fun.Name, allowed) {
						return true
					}
					offenders = append(offenders, formatAuditOffender(fset, root, path, call.Pos(), fun.Name))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("audit forbidden rawdb calls: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func isAllowedRawDBCall(root, path, function string, allowed map[string]map[string]struct{}) bool {
	if len(allowed) == 0 {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = path
	}
	rel = filepath.ToSlash(rel)
	functions := allowed[rel]
	if len(functions) == 0 {
		return false
	}
	_, ok := functions[function]
	return ok
}

func rawdbImportNames(file *ast.File) map[string]struct{} {
	names := make(map[string]struct{})
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil || path != "github.com/tronprotocol/go-tron/core/rawdb" {
			continue
		}
		name := "rawdb"
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if name == "_" {
			continue
		}
		names[name] = struct{}{}
	}
	return names
}

func formatAuditOffender(fset *token.FileSet, root, path string, pos token.Pos, call string) string {
	position := fset.Position(pos)
	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = path
	}
	return rel + ":" + strconv.Itoa(position.Line) + ": " + call
}
