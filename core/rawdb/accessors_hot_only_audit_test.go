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

func TestNoActuatorDirectHotBlockHashReads(t *testing.T) {
	repoRoot := findRepoRoot(t)
	actuatorRoot := filepath.Join(repoRoot, "actuator")
	offenders := auditForbiddenRawDBCalls(t, actuatorRoot, map[string]struct{}{
		"ReadBlockHashByNumber": {},
	}, map[string]map[string]struct{}{
		"actuator.go": {
			"ReadBlockHashByNumber": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("actuator historical compatibility checks must use Context.EffectiveGenesisHash instead of direct hot block hash reads:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionBlockHashByNumberReadsStayOnAuditedBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditForbiddenRawDBCalls(t, root, map[string]struct{}{
		"ReadBlockHashByNumber": {},
	}, map[string]map[string]struct{}{
		"actuator/actuator.go": {
			"ReadBlockHashByNumber": {},
		},
		"cmd/gtron/db_cmd.go": {
			"ReadBlockHashByNumber": {},
		},
		"cmd/gtron/freezer_adapter.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/blockbuffer/buffer.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/state/pruning/pruner.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/state/snapshots/cold_builder.go": {
			"ReadBlockHashByNumber": {},
		},
		"vm/instructions.go": {
			"ReadBlockHashByNumber": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production block-hash-by-number reads must stay behind audited freezer/cold-index boundaries:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionHotOnlyChainDBConstructorsStayOnAuditedBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditHotOnlyChainDBConstructors(t, root, map[string]struct{}{
		"cmd/gtron/db_cmd.go":            {},
		"core/balance_trace_backfill.go": {},
		"core/blockchain.go":             {},
		"core/genesis.go":                {},
	})
	if len(offenders) > 0 {
		t.Fatalf("production code must not construct hot-only ChainDB wrappers outside audited constructor/replay/diagnostic boundaries:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestHotOnlyChainDBAuditRecognizesConvertedNoopAncient(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func build() {
	_ = rawdb.NewChainDB(nil, rawdb.AncientReader(rawdb.NoopAncient{}))
}
`)

	offenders := auditHotOnlyChainDBConstructors(t, root, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.NewChainDB(..., rawdb.NoopAncient{})") {
		t.Fatalf("offenders = %+v, want converted NoopAncient constructor", offenders)
	}
}

func TestHotOnlyChainDBAuditRecognizesNoopAncientAlias(t *testing.T) {
	root := writeAuditFixture(t, "app/offender.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func build() {
	var ancient rawdb.AncientReader = rawdb.NoopAncient{}
	_ = rawdb.NewChainDB(nil, ancient)
}
`)

	offenders := auditHotOnlyChainDBConstructors(t, root, nil)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "rawdb.NewChainDB(..., rawdb.NoopAncient{})") {
		t.Fatalf("offenders = %+v, want aliased NoopAncient constructor", offenders)
	}
}

func TestHotOnlyChainDBAuditClearsNoopAncientAliasAfterRewrap(t *testing.T) {
	root := writeAuditFixture(t, "app/rewrapped.go", `package app

import rawdb "github.com/tronprotocol/go-tron/core/rawdb"

func build() {
	var ancient rawdb.AncientReader = rawdb.NoopAncient{}
	ancient = rawdb.NewFallbackAncientReader(ancient, nil)
	_ = rawdb.NewChainDB(nil, ancient)
}
`)

	if offenders := auditHotOnlyChainDBConstructors(t, root, nil); len(offenders) != 0 {
		t.Fatalf("offenders = %+v, want rewrapped ancient reader accepted", offenders)
	}
}

func TestProductionColdArchiveReadersUseChainDBBoundary(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditColdArchiveReaderCalls(t, root, map[string]struct{}{
		"ReadAccountTrace":            {},
		"ReadAccountTraceAtOrBefore":  {},
		"ReadBlock":                   {},
		"ReadBlockBalanceTrace":       {},
		"ReadBlockHashByNumber":       {},
		"ReadBlockNumber":             {},
		"ReadBlockStateRoot":          {},
		"ReadSectionBloom":            {},
		"ReadSectionBloomBitSet":      {},
		"ReadTransactionIndex":        {},
		"ReadTransactionInfo":         {},
		"ReadTransactionInfosByBlock": {},
	}, map[string]map[string]struct{}{
		"actuator/actuator.go": {
			"ReadBlockHashByNumber": {},
		},
		"cmd/balance-trace/main.go": {
			"ReadAccountTrace":      {},
			"ReadBlockBalanceTrace": {},
		},
		"cmd/gtron/db_cmd.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/balance_trace_backfill.go": {
			"ReadAccountTrace":      {},
			"ReadBlockBalanceTrace": {},
		},
		"core/blockbuffer/buffer.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/state/pruning/pruner.go": {
			"ReadBlockHashByNumber": {},
		},
		"core/state/snapshots/cold_builder.go": {
			"ReadBlockHashByNumber": {},
		},
		"vm/instructions.go": {
			"ReadBlockHashByNumber": {},
		},
	})
	if len(offenders) > 0 {
		t.Fatalf("production archive readers must use the freezer/cold-sidecar-aware ChainDB boundary:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestProductionEventLogQueriesUseChainDBBoundary(t *testing.T) {
	root := findRepoRoot(t)
	offenders := auditEventLogMethodCalls(t, root, map[string]struct{}{
		"EventLogRangeCovered":          {},
		"EventLogRangeCoveredForFilter": {},
		"IterateEventLogs":              {},
	})
	if len(offenders) > 0 {
		t.Fatalf("production event-log queries must go through the cold-sidecar-aware ChainDB boundary:\n%s", strings.Join(offenders, "\n"))
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

func writeAuditFixture(t *testing.T, rel, body string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module auditfixture\n"), 0o644); err != nil {
		t.Fatalf("write fixture go.mod: %v", err)
	}
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir fixture: %v", err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write fixture source: %v", err)
	}
	return root
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

func auditHotOnlyChainDBConstructors(t *testing.T, root string, allowed map[string]struct{}) []string {
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
			fn, ok := node.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				return true
			}
			noopAncientAliases := make(map[string]struct{})
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.AssignStmt:
					recordNoopAncientAliases(n.Lhs, n.Rhs, rawdbNames, noopAncientAliases)
					return true
				case *ast.ValueSpec:
					recordNoopAncientAliases(exprsFromIdents(n.Names), n.Values, rawdbNames, noopAncientAliases)
					return true
				case *ast.CallExpr:
					if len(n.Args) < 2 {
						return true
					}
					if !isRawDBCall(n.Fun, rawdbNames, "NewChainDB") {
						return true
					}
					if !isNoopAncientExpr(n.Args[1], rawdbNames) && !isNoopAncientAlias(n.Args[1], noopAncientAliases) {
						return true
					}
					if isAllowedAuditPath(root, path, allowed) {
						return true
					}
					offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), "rawdb.NewChainDB(..., rawdb.NoopAncient{})"))
				}
				return true
			})
			return false
		})
		return nil
	})
	if err != nil {
		t.Fatalf("audit hot-only ChainDB constructors: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func auditColdArchiveReaderCalls(t *testing.T, root string, watched map[string]struct{}, allowed map[string]map[string]struct{}) []string {
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
		rel := auditRelPath(root, path)
		aliases := map[string]struct{}{
			"chain":   {},
			"chainDB": {},
			"chaindb": {},
			"cdb":     {},
			"source":  {},
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				recordChainDBAliases(n.Lhs, n.Rhs, rawdbNames, aliases)
			case *ast.ValueSpec:
				recordChainDBAliases(exprsFromIdents(n.Names), n.Values, rawdbNames, aliases)
			case *ast.CallExpr:
				if len(n.Args) == 0 {
					return true
				}
				name, ok := rawDBCallName(n.Fun, rawdbNames)
				if !ok {
					return true
				}
				if _, watch := watched[name]; !watch {
					return true
				}
				if isAllowedRawDBCall(root, path, name, allowed) {
					return true
				}
				if isColdAwareArchiveReaderArg(rel, name, n.Args[0], rawdbNames, aliases) {
					return true
				}
				offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), "rawdb."+name))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("audit cold archive reader calls: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func auditEventLogMethodCalls(t *testing.T, root string, watched map[string]struct{}) []string {
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
		if strings.HasPrefix(path, filepath.Join(root, "core", "state", "snapshots")+string(os.PathSeparator)) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rawdbNames := rawdbImportNames(file)
		aliases := map[string]struct{}{
			"chainDB": {},
			"chaindb": {},
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				recordChainDBAliases(n.Lhs, n.Rhs, rawdbNames, aliases)
			case *ast.ValueSpec:
				recordChainDBAliases(exprsFromIdents(n.Names), n.Values, rawdbNames, aliases)
			case *ast.CallExpr:
				sel, ok := n.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if _, watch := watched[sel.Sel.Name]; !watch {
					return true
				}
				if isChainDBBoundaryExpr(sel.X, rawdbNames, aliases) {
					return true
				}
				offenders = append(offenders, formatAuditOffender(fset, root, path, n.Pos(), sel.Sel.Name))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("audit event-log method calls: %v", err)
	}
	sort.Strings(offenders)
	return offenders
}

func recordChainDBAliases(lhs, rhs []ast.Expr, rawdbNames map[string]struct{}, aliases map[string]struct{}) {
	for i, left := range lhs {
		if i >= len(rhs) {
			return
		}
		ident, ok := left.(*ast.Ident)
		if !ok || ident.Name == "_" {
			continue
		}
		if isChainDBBoundaryExpr(rhs[i], rawdbNames, aliases) {
			aliases[ident.Name] = struct{}{}
		}
	}
}

func recordNoopAncientAliases(lhs, rhs []ast.Expr, rawdbNames map[string]struct{}, aliases map[string]struct{}) {
	for i, left := range lhs {
		if i >= len(rhs) {
			return
		}
		ident, ok := left.(*ast.Ident)
		if !ok || ident.Name == "_" {
			continue
		}
		if isNoopAncientExpr(rhs[i], rawdbNames) {
			aliases[ident.Name] = struct{}{}
			continue
		}
		delete(aliases, ident.Name)
	}
}

func exprsFromIdents(idents []*ast.Ident) []ast.Expr {
	exprs := make([]ast.Expr, 0, len(idents))
	for _, ident := range idents {
		exprs = append(exprs, ident)
	}
	return exprs
}

func isAllowedRawDBCall(root, path, function string, allowed map[string]map[string]struct{}) bool {
	if len(allowed) == 0 {
		return false
	}
	rel := auditRelPath(root, path)
	functions := allowed[rel]
	if len(functions) == 0 {
		return false
	}
	_, ok := functions[function]
	return ok
}

func isAllowedAuditPath(root, path string, allowed map[string]struct{}) bool {
	if len(allowed) == 0 {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = path
	}
	rel = filepath.ToSlash(rel)
	_, ok := allowed[rel]
	return ok
}

func rawDBCallName(expr ast.Expr, rawdbNames map[string]struct{}) (string, bool) {
	switch fun := expr.(type) {
	case *ast.SelectorExpr:
		ident, ok := fun.X.(*ast.Ident)
		if !ok {
			return "", false
		}
		if _, imported := rawdbNames[ident.Name]; !imported {
			return "", false
		}
		return fun.Sel.Name, true
	case *ast.Ident:
		if _, dotImported := rawdbNames["."]; !dotImported {
			return "", false
		}
		return fun.Name, true
	default:
		return "", false
	}
}

func isRawDBCall(expr ast.Expr, rawdbNames map[string]struct{}, name string) bool {
	if call, ok := expr.(*ast.CallExpr); ok {
		expr = call.Fun
	}
	got, ok := rawDBCallName(expr, rawdbNames)
	return ok && got == name
}

func isChainDBBoundaryExpr(expr ast.Expr, rawdbNames map[string]struct{}, aliases map[string]struct{}) bool {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	if ident, ok := expr.(*ast.Ident); ok {
		_, exists := aliases[ident.Name]
		return exists
	}
	if isRawDBCall(expr, rawdbNames, "NewChainDB") {
		return true
	}
	if call, ok := expr.(*ast.CallExpr); ok {
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "ChainDB" {
			return true
		}
	}
	path := selectorPath(expr)
	return len(path) > 0 && strings.EqualFold(path[len(path)-1], "chaindb")
}

func isColdAwareArchiveReaderArg(rel, function string, expr ast.Expr, rawdbNames map[string]struct{}, aliases map[string]struct{}) bool {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	if isChainDBBoundaryExpr(expr, rawdbNames, aliases) {
		return true
	}
	path := selectorPath(expr)
	if len(path) == 0 {
		return false
	}
	if rel == "core/tron_backend.go" && function == "ReadSectionBloomBitSet" && strings.Join(path, ".") == "m.db" {
		return true
	}
	return false
}

func selectorPath(expr ast.Expr) []string {
	switch v := expr.(type) {
	case *ast.Ident:
		return []string{v.Name}
	case *ast.SelectorExpr:
		base := selectorPath(v.X)
		if len(base) == 0 {
			return nil
		}
		return append(base, v.Sel.Name)
	default:
		return nil
	}
}

func isNoopAncientAlias(expr ast.Expr, aliases map[string]struct{}) bool {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	_, exists := aliases[ident.Name]
	return exists
}

func isNoopAncientExpr(expr ast.Expr, rawdbNames map[string]struct{}) bool {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	if call, ok := expr.(*ast.CallExpr); ok && len(call.Args) == 1 && isRawDBType(call.Fun, rawdbNames, "AncientReader") {
		return isNoopAncientExpr(call.Args[0], rawdbNames)
	}
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return false
	}
	return isRawDBType(lit.Type, rawdbNames, "NoopAncient")
}

func isRawDBType(expr ast.Expr, rawdbNames map[string]struct{}, name string) bool {
	switch typ := expr.(type) {
	case *ast.SelectorExpr:
		ident, ok := typ.X.(*ast.Ident)
		if !ok || typ.Sel.Name != name {
			return false
		}
		_, imported := rawdbNames[ident.Name]
		return imported
	case *ast.Ident:
		if typ.Name != name {
			return false
		}
		_, dotImported := rawdbNames["."]
		return dotImported
	default:
		return false
	}
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
	rel := auditRelPath(root, path)
	return rel + ":" + strconv.Itoa(position.Line) + ": " + call
}

func auditRelPath(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = path
	}
	return filepath.ToSlash(rel)
}
