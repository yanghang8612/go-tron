package core

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

type archiveStateAtAssignment struct {
	session string
	line    int
}

func TestArchiveStateAtCallersCloseSession(t *testing.T) {
	const sourceFile = "tron_backend.go"

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", sourceFile, err)
	}

	expectedCallers := map[string]int{
		"CanDelegateResourceAt":                1,
		"EstimateGasAt":                        1,
		"GetAccountAt":                         1,
		"GetAccountByIdAt":                     1,
		"GetAccountNetAt":                      1,
		"GetAccountResourceAt":                 1,
		"GetAssetIssueByIDAt":                  1,
		"GetAssetIssueByNameAt":                1,
		"GetBalanceAt":                         1,
		"GetBandwidthPricesAt":                 1,
		"GetBrokerageInfoAt":                   1,
		"GetBurnTrxAt":                         1,
		"GetChainParametersAt":                 1,
		"GetCodeAt":                            1,
		"GetContractAt":                        1,
		"GetDelegatedResourceAccountIndexV2At": 1,
		"GetDelegatedResourceV2At":             1,
		"GetEnergyPricesAt":                    1,
		"GetMarketOrderByIDAt":                 1,
		"GetMarketOrdersByAccountAt":           1,
		"GetMarketPriceByPairAt":               1,
		"GetProposalByIDAt":                    1,
		"GetRewardAt":                          1,
		"GetStorageAtBlock":                    1,
		"ListExchangesAt":                      1,
		"ListProposalsAt":                      1,
		"ListWitnessesAt":                      1,
		"NextMaintenanceTimeAt":                1,
		"TraceBlock":                           1,
		"TraceTransaction":                     1,
		"TriggerConstantContractAt":            1,
		"accountAtOrNil":                       1,
		"listAssetsAt":                         1,
		"traceStateContext":                    1,
	}
	directCloseAllowed := map[string]bool{
		"EstimateGasAt": true,
	}
	releaseHandoffAllowed := map[string]bool{
		"traceStateContext": true,
	}

	actualCallers := make(map[string]int)
	var failures []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		funcName := fn.Name.Name
		callLines := archiveStateAtCallLines(fset, fn.Body)
		if len(callLines) == 0 {
			continue
		}
		actualCallers[funcName] = len(callLines)

		assignments := archiveStateAtAssignments(fset, fn.Body)
		if len(assignments) != len(callLines) {
			failures = append(failures, fmt.Sprintf("%s has %d archiveStateAt calls but %d session assignments", funcName, len(callLines), len(assignments)))
			continue
		}
		for _, assignment := range assignments {
			switch {
			case hasDeferSessionClose(fn.Body, assignment.session):
				continue
			case directCloseAllowed[funcName] && hasSessionCloseCall(fn.Body, assignment.session):
				continue
			case releaseHandoffAllowed[funcName] && hasReleaseHandoff(fn.Body, assignment.session):
				continue
			default:
				failures = append(failures, fmt.Sprintf("%s:%d opens archive session %q without an audited Close path", funcName, assignment.line, assignment.session))
			}
		}
	}
	if diff := compareArchiveStateAtCallers(expectedCallers, actualCallers); diff != "" {
		t.Fatalf("archiveStateAt caller set changed; audit the new close path and update this test:\n%s", diff)
	}
	if len(failures) > 0 {
		sort.Strings(failures)
		t.Fatalf("archiveStateAt session close audit failed:\n%s", strings.Join(failures, "\n"))
	}
}

func TestPublicBlockBoundArchiveAPIsUseArchiveBoundary(t *testing.T) {
	const sourceFile = "tron_backend.go"

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", sourceFile, err)
	}

	expected := map[string][]string{
		"CallAt":                               {"TriggerConstantContractAt"},
		"CanDelegateResourceAt":                {"archiveStateAt"},
		"EstimateEnergyAt":                     {"TriggerConstantContractAt"},
		"EstimateGasAt":                        {"TriggerConstantContractAt", "archiveStateAt"},
		"GetAccountAt":                         {"archiveStateAt"},
		"GetAccountByIdAt":                     {"archiveStateAt"},
		"GetAccountNetAt":                      {"archiveStateAt"},
		"GetAccountResourceAt":                 {"archiveStateAt"},
		"GetAssetIssueByIDAt":                  {"archiveStateAt"},
		"GetAssetIssueByNameAt":                {"archiveStateAt"},
		"GetAssetIssueListAt":                  {"listAssetsAt"},
		"GetAssetIssueListPaginatedAt":         {"listAssetsAt"},
		"GetAvailableUnfreezeCountAt":          {"accountAtOrNil"},
		"GetBalanceAt":                         {"archiveStateAt"},
		"GetBandwidthPricesAt":                 {"archiveStateAt"},
		"GetBrokerageInfoAt":                   {"archiveStateAt"},
		"GetBurnTrxAt":                         {"archiveStateAt"},
		"GetCanWithdrawUnfreezeAmountAt":       {"accountAtOrNil"},
		"GetChainParametersAt":                 {"archiveStateAt"},
		"GetCodeAt":                            {"archiveStateAt"},
		"GetContractAt":                        {"archiveStateAt"},
		"GetDelegatedResourceAccountIndexV2At": {"archiveStateAt"},
		"GetDelegatedResourceV2At":             {"archiveStateAt"},
		"GetEnergyPricesAt":                    {"archiveStateAt"},
		"GetMarketOrderByIDAt":                 {"archiveStateAt"},
		"GetMarketOrdersByAccountAt":           {"archiveStateAt"},
		"GetMarketPriceByPairAt":               {"archiveStateAt"},
		"GetProposalByIDAt":                    {"archiveStateAt"},
		"GetRewardAt":                          {"archiveStateAt"},
		"GetStorageAtBlock":                    {"archiveStateAt"},
		"ListExchangesAt":                      {"archiveStateAt"},
		"ListProposalsAt":                      {"archiveStateAt"},
		"ListProposalsPaginatedAt":             {"ListProposalsAt"},
		"ListWitnessesAt":                      {"archiveStateAt"},
		"NextMaintenanceTimeAt":                {"archiveStateAt"},
		"TriggerConstantContractAt":            {"archiveStateAt"},
	}

	actual := make(map[string][]string)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !isTronBackendMethod(fn) {
			continue
		}
		if !isPublicBlockBoundArchiveAPI(fn) {
			continue
		}
		actual[fn.Name.Name] = archiveBoundaryCallNames(fn.Body)
	}

	if diff := compareArchiveBoundaryAPIs(expected, actual); diff != "" {
		t.Fatalf("public block-bound archive API set changed; audit the archive/as-of boundary and update this test:\n%s", diff)
	}
}

func TestArchiveStateAtIsOnlyDirectlyCalled(t *testing.T) {
	const sourceFile = "tron_backend.go"

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", sourceFile, err)
	}

	offenders := archiveStateAtNonDirectReferences(fset, sourceFile, file)
	if len(offenders) > 0 {
		t.Fatalf("archiveStateAt must not be aliased or passed as a function value; indirect calls bypass the close/archive-boundary audit:\n%s",
			strings.Join(offenders, "\n"))
	}
}

func TestArchiveStateAtAuditRejectsFunctionValueAlias(t *testing.T) {
	const sourceFile = "fixture.go"
	const source = `package core

type TronBackend struct{}
type session struct{}

func (b *TronBackend) archiveStateAt(uint64) (*session, error) { return nil, nil }

func (b *TronBackend) GetAccountAt(blockNum uint64) error {
	open := b.archiveStateAt
	_, err := open(blockNum)
	return err
}
`

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", sourceFile, err)
	}
	offenders := archiveStateAtNonDirectReferences(fset, sourceFile, file)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "archiveStateAt referenced outside a direct call") {
		t.Fatalf("offenders = %+v, want archiveStateAt function-value alias rejected", offenders)
	}
}

func archiveStateAtNonDirectReferences(fset *token.FileSet, sourceFile string, file *ast.File) []string {
	var offenders []string
	stack := make([]ast.Node, 0, 16)
	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		var parent ast.Node
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		switch expr := n.(type) {
		case *ast.SelectorExpr:
			if expr.Sel.Name == "archiveStateAt" && !isDirectCallFun(parent, expr) {
				offenders = append(offenders, fmt.Sprintf("%s:%d: archiveStateAt referenced outside a direct call",
					sourceFile, fset.Position(expr.Pos()).Line))
			}
		case *ast.Ident:
			if expr.Name == "archiveStateAt" && !isFunctionName(parent, expr) && !isSelectorName(parent, expr) && !isDirectCallFun(parent, expr) {
				offenders = append(offenders, fmt.Sprintf("%s:%d: archiveStateAt referenced outside a direct call",
					sourceFile, fset.Position(expr.Pos()).Line))
			}
		}
		stack = append(stack, n)
		return true
	})
	return offenders
}

func archiveStateAtCallLines(fset *token.FileSet, body *ast.BlockStmt) []int {
	var lines []int
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isArchiveStateAtCall(call) {
			return true
		}
		lines = append(lines, fset.Position(call.Pos()).Line)
		return true
	})
	return lines
}

func archiveStateAtAssignments(fset *token.FileSet, body *ast.BlockStmt) []archiveStateAtAssignment {
	var assignments []archiveStateAtAssignment
	ast.Inspect(body, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for _, rhs := range assign.Rhs {
			call, ok := rhs.(*ast.CallExpr)
			if !ok || !isArchiveStateAtCall(call) {
				continue
			}
			if len(assign.Lhs) == 0 {
				continue
			}
			session, ok := assign.Lhs[0].(*ast.Ident)
			if !ok || session.Name == "_" {
				continue
			}
			assignments = append(assignments, archiveStateAtAssignment{
				session: session.Name,
				line:    fset.Position(call.Pos()).Line,
			})
		}
		return true
	})
	return assignments
}

func isArchiveStateAtCall(call *ast.CallExpr) bool {
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		return fun.Sel.Name == "archiveStateAt"
	case *ast.Ident:
		return fun.Name == "archiveStateAt"
	default:
		return false
	}
}

func isDirectCallFun(parent ast.Node, expr ast.Expr) bool {
	call, ok := parent.(*ast.CallExpr)
	return ok && call.Fun == expr
}

func isFunctionName(parent ast.Node, ident *ast.Ident) bool {
	fn, ok := parent.(*ast.FuncDecl)
	return ok && fn.Name == ident
}

func isSelectorName(parent ast.Node, ident *ast.Ident) bool {
	sel, ok := parent.(*ast.SelectorExpr)
	return ok && sel.Sel == ident
}

func hasDeferSessionClose(body *ast.BlockStmt, session string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		deferStmt, ok := n.(*ast.DeferStmt)
		if !ok {
			return true
		}
		found = isSessionCloseCall(deferStmt.Call, session)
		return !found
	})
	return found
}

func hasSessionCloseCall(body *ast.BlockStmt, session string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		found = isSessionCloseCall(call, session)
		return !found
	})
	return found
}

func isSessionCloseCall(call *ast.CallExpr, session string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Close" {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == session && len(call.Args) == 0
}

func hasReleaseHandoff(body *ast.BlockStmt, session string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, rhs := range assign.Rhs {
			sel, ok := rhs.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Close" {
				continue
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || ident.Name != session {
				continue
			}
			if i >= len(assign.Lhs) {
				continue
			}
			lhs, ok := assign.Lhs[i].(*ast.Ident)
			if ok && lhs.Name == "release" {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

func compareArchiveStateAtCallers(expected, actual map[string]int) string {
	var lines []string
	for name, want := range expected {
		if got := actual[name]; got != want {
			lines = append(lines, fmt.Sprintf("%s: got %d call(s), want %d", name, got, want))
		}
	}
	for name, got := range actual {
		if _, ok := expected[name]; !ok {
			lines = append(lines, fmt.Sprintf("%s: got %d unexpected call(s)", name, got))
		}
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func isTronBackendMethod(fn *ast.FuncDecl) bool {
	if fn == nil || fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	return exprTypeName(fn.Recv.List[0].Type) == "*TronBackend"
}

func isPublicBlockBoundArchiveAPI(fn *ast.FuncDecl) bool {
	if fn == nil || !ast.IsExported(fn.Name.Name) {
		return false
	}
	if !strings.HasSuffix(fn.Name.Name, "At") && !strings.HasSuffix(fn.Name.Name, "AtBlock") {
		return false
	}
	return hasUint64ParamNamed(fn.Type.Params, "blockNum")
}

func hasUint64ParamNamed(fields *ast.FieldList, name string) bool {
	if fields == nil {
		return false
	}
	for _, field := range fields.List {
		if exprTypeName(field.Type) != "uint64" {
			continue
		}
		for _, fieldName := range field.Names {
			if fieldName.Name == name {
				return true
			}
		}
	}
	return false
}

func exprTypeName(expr ast.Expr) string {
	switch typ := expr.(type) {
	case *ast.Ident:
		return typ.Name
	case *ast.StarExpr:
		return "*" + exprTypeName(typ.X)
	default:
		return ""
	}
}

func archiveBoundaryCallNames(body *ast.BlockStmt) []string {
	boundaries := map[string]struct{}{
		"ListProposalsAt":           {},
		"TriggerConstantContractAt": {},
		"accountAtOrNil":            {},
		"archiveStateAt":            {},
		"listAssetsAt":              {},
	}
	seen := make(map[string]struct{})
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := callName(call)
		if _, ok := boundaries[name]; ok {
			seen[name] = struct{}{}
		}
		return true
	})
	return sortedArchiveBoundaryKeys(seen)
}

func callName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	default:
		return ""
	}
}

func compareArchiveBoundaryAPIs(expected, actual map[string][]string) string {
	var lines []string
	for name, want := range expected {
		got, ok := actual[name]
		if !ok {
			lines = append(lines, fmt.Sprintf("%s: missing audited public archive method", name))
			continue
		}
		want = sortedStringSlice(want)
		got = sortedStringSlice(got)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			lines = append(lines, fmt.Sprintf("%s: archive boundaries got [%s], want [%s]",
				name, strings.Join(got, ","), strings.Join(want, ",")))
		}
	}
	for name, got := range actual {
		if _, ok := expected[name]; ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: unexpected public block-bound archive method with boundaries [%s]",
			name, strings.Join(got, ",")))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func sortedArchiveBoundaryKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedStringSlice(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
