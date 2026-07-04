package jsonrpc

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

func TestEthAPIBlockTagMethodsUseArchiveBackends(t *testing.T) {
	file := parseJSONRPCSource(t, "ethapi.go")
	expected := map[string][]string{
		"Call":                {"Call", "CallAt"},
		"EstimateGas":         {"EstimateGas", "EstimateGasAt"},
		"GetBalance":          {"GetBalance", "GetBalanceAt"},
		"GetCode":             {"GetCode", "GetCodeAt"},
		"GetStorageAt":        {"GetStorageAt", "GetStorageAtBlock"},
		"GetTransactionCount": nil,
	}

	actual := make(map[string][]string)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !isJSONRPCReceiverMethod(fn, "*EthAPI") {
			continue
		}
		if !ast.IsExported(fn.Name.Name) || !hasStarStringParam(fn.Type.Params) {
			continue
		}
		actual[fn.Name.Name] = archiveBackendCalls(fn.Body)
	}

	if diff := compareArchiveBackendCalls(expected, actual); diff != "" {
		t.Fatalf("EthAPI block-tag archive routing changed; audit live/archive backend calls and update this test:\n%s", diff)
	}
}

func TestLegacyJSONRPCHistoricalHandlersUseArchiveBackends(t *testing.T) {
	file := parseJSONRPCSource(t, "api.go")
	expected := map[string][]string{
		"ethCall":         {"Call", "CallAt"},
		"ethEstimateGas":  {"EstimateGas", "EstimateGasAt"},
		"ethGetBalance":   {"GetBalance", "GetBalanceAt"},
		"ethGetCode":      {"GetCode", "GetCodeAt"},
		"ethGetStorageAt": {"GetStorageAt", "GetStorageAtBlock"},
	}

	actual := make(map[string][]string)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !isJSONRPCReceiverMethod(fn, "*API") {
			continue
		}
		if _, ok := expected[fn.Name.Name]; !ok {
			continue
		}
		actual[fn.Name.Name] = archiveBackendCalls(fn.Body)
	}

	if diff := compareArchiveBackendCalls(expected, actual); diff != "" {
		t.Fatalf("legacy JSON-RPC archive routing changed; audit live/archive backend calls and update this test:\n%s", diff)
	}
}

func TestJSONRPCArchiveBackendMethodsAreOnlyDirectlyCalled(t *testing.T) {
	var offenders []string
	for _, sourceFile := range []string{"api.go", "ethapi.go"} {
		fset, file := parseJSONRPCSourceWithFileSet(t, sourceFile)
		offenders = append(offenders, archiveBackendNonDirectReferences(fset, sourceFile, file)...)
	}
	if len(offenders) > 0 {
		t.Fatalf("JSON-RPC archive/live backend methods must not be aliased or passed as function values; indirect calls bypass archive-boundary audits:\n%s",
			strings.Join(offenders, "\n"))
	}
}

func TestJSONRPCArchiveBackendReceiverIsNotAliased(t *testing.T) {
	var offenders []string
	for _, sourceFile := range []string{"api.go", "ethapi.go"} {
		fset, file := parseJSONRPCSourceWithFileSet(t, sourceFile)
		offenders = append(offenders, jsonRPCBackendAliasAssignments(fset, sourceFile, file)...)
		offenders = append(offenders, jsonRPCBackendReceiverEscapes(fset, sourceFile, file)...)
	}
	if len(offenders) > 0 {
		t.Fatalf("JSON-RPC backend receiver must not be aliased or passed through helpers; indirect calls bypass archive-boundary selector audits:\n%s",
			strings.Join(offenders, "\n"))
	}
}

func TestJSONRPCArchiveBackendAuditRejectsFunctionValueAlias(t *testing.T) {
	const sourceFile = "fixture.go"
	const source = `package jsonrpc

type API struct{ backend *backend }
type backend struct{}

func (b *backend) GetBalanceAt(addr string, block uint64) (int64, error) { return 0, nil }

func (api *API) ethGetBalance(block uint64) error {
	read := api.backend.GetBalanceAt
	_, err := read("addr", block)
	return err
}
`

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", sourceFile, err)
	}
	offenders := archiveBackendNonDirectReferences(fset, sourceFile, file)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "GetBalanceAt referenced outside a direct call") {
		t.Fatalf("offenders = %+v, want GetBalanceAt function-value alias rejected", offenders)
	}
}

func TestJSONRPCArchiveBackendAuditRejectsBackendAlias(t *testing.T) {
	const sourceFile = "fixture.go"
	const source = `package jsonrpc

type API struct{ backend *backend }
type backend struct{}

func (b *backend) GetBalanceAt(addr string, block uint64) (int64, error) { return 0, nil }

func (api *API) ethGetBalance(block uint64) error {
	backend := api.backend
	_, err := backend.GetBalanceAt("addr", block)
	return err
}
`

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", sourceFile, err)
	}
	offenders := jsonRPCBackendAliasAssignments(fset, sourceFile, file)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "backend receiver assigned to alias") {
		t.Fatalf("offenders = %+v, want backend receiver alias rejected", offenders)
	}
}

func TestJSONRPCArchiveBackendAuditRejectsBackendReceiverArgument(t *testing.T) {
	const sourceFile = "fixture.go"
	const source = `package jsonrpc

type API struct{ backend *backend }
type backend struct{}

func useBackend(*backend, uint64) error { return nil }

func (api *API) ethGetBalance(block uint64) error {
	return useBackend(api.backend, block)
}
`

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", sourceFile, err)
	}
	offenders := jsonRPCBackendReceiverEscapes(fset, sourceFile, file)
	if len(offenders) != 1 || !strings.Contains(offenders[0], "backend receiver referenced outside a method selector") {
		t.Fatalf("offenders = %+v, want backend receiver argument rejected", offenders)
	}
}

func parseJSONRPCSource(t *testing.T, path string) *ast.File {
	t.Helper()
	_, file := parseJSONRPCSourceWithFileSet(t, path)
	return file
}

func parseJSONRPCSourceWithFileSet(t *testing.T, path string) (*token.FileSet, *ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return fset, file
}

func isJSONRPCReceiverMethod(fn *ast.FuncDecl, receiver string) bool {
	if fn == nil || fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	return jsonRPCExprTypeName(fn.Recv.List[0].Type) == receiver
}

func hasStarStringParam(fields *ast.FieldList) bool {
	if fields == nil {
		return false
	}
	for _, field := range fields.List {
		if jsonRPCExprTypeName(field.Type) == "*string" {
			return true
		}
	}
	return false
}

func jsonRPCExprTypeName(expr ast.Expr) string {
	switch typ := expr.(type) {
	case *ast.Ident:
		return typ.Name
	case *ast.StarExpr:
		return "*" + jsonRPCExprTypeName(typ.X)
	default:
		return ""
	}
}

func archiveBackendCalls(body *ast.BlockStmt) []string {
	seen := make(map[string]struct{})
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if !isBackendSelector(sel.X) {
			return true
		}
		if _, ok := jsonRPCArchiveBackendMethods()[sel.Sel.Name]; ok {
			seen[sel.Sel.Name] = struct{}{}
		}
		return true
	})
	return sortedJSONRPCAuditKeys(seen)
}

func archiveBackendNonDirectReferences(fset *token.FileSet, sourceFile string, file *ast.File) []string {
	var offenders []string
	stack := make([]ast.Node, 0, 16)
	ast.Inspect(file, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		var parent ast.Node
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		sel, ok := node.(*ast.SelectorExpr)
		if ok && isBackendSelector(sel.X) {
			if _, watched := jsonRPCArchiveBackendMethods()[sel.Sel.Name]; watched && !isJSONRPCDirectCallFun(parent, sel) {
				offenders = append(offenders, fmt.Sprintf("%s:%d: %s referenced outside a direct call",
					sourceFile, fset.Position(sel.Pos()).Line, sel.Sel.Name))
			}
		}
		stack = append(stack, node)
		return true
	})
	return offenders
}

func jsonRPCBackendAliasAssignments(fset *token.FileSet, sourceFile string, file *ast.File) []string {
	var offenders []string
	ast.Inspect(file, func(node ast.Node) bool {
		switch stmt := node.(type) {
		case *ast.AssignStmt:
			for _, rhs := range stmt.Rhs {
				if isBackendSelector(rhs) {
					offenders = append(offenders, fmt.Sprintf("%s:%d: backend receiver assigned to alias",
						sourceFile, fset.Position(rhs.Pos()).Line))
				}
			}
		case *ast.ValueSpec:
			for _, value := range stmt.Values {
				if isBackendSelector(value) {
					offenders = append(offenders, fmt.Sprintf("%s:%d: backend receiver assigned to alias",
						sourceFile, fset.Position(value.Pos()).Line))
				}
			}
		}
		return true
	})
	return offenders
}

func jsonRPCBackendReceiverEscapes(fset *token.FileSet, sourceFile string, file *ast.File) []string {
	var offenders []string
	stack := make([]ast.Node, 0, 16)
	ast.Inspect(file, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		var parent ast.Node
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		sel, ok := node.(*ast.SelectorExpr)
		if ok && isBackendSelector(sel) &&
			!isJSONRPCBackendMethodReceiver(parent, sel) &&
			!isJSONRPCAllowedBackendHelperArgument(parent, sel) {
			offenders = append(offenders, fmt.Sprintf("%s:%d: backend receiver referenced outside a method selector",
				sourceFile, fset.Position(sel.Pos()).Line))
		}
		stack = append(stack, node)
		return true
	})
	return offenders
}

func jsonRPCArchiveBackendMethods() map[string]struct{} {
	return map[string]struct{}{
		"Call":              {},
		"CallAt":            {},
		"EstimateGas":       {},
		"EstimateGasAt":     {},
		"GetBalance":        {},
		"GetBalanceAt":      {},
		"GetCode":           {},
		"GetCodeAt":         {},
		"GetStorageAt":      {},
		"GetStorageAtBlock": {},
	}
}

func isJSONRPCDirectCallFun(parent ast.Node, expr ast.Expr) bool {
	call, ok := parent.(*ast.CallExpr)
	return ok && call.Fun == expr
}

func isJSONRPCBackendMethodReceiver(parent ast.Node, expr ast.Expr) bool {
	sel, ok := parent.(*ast.SelectorExpr)
	return ok && sel.X == expr
}

func isJSONRPCAllowedBackendHelperArgument(parent ast.Node, expr ast.Expr) bool {
	call, ok := parent.(*ast.CallExpr)
	if !ok || !jsonRPCExprIsCallArg(call, expr) {
		return false
	}
	ident, ok := call.Fun.(*ast.Ident)
	if !ok {
		return false
	}
	switch ident.Name {
	case "blockByNumberOrHash", "blockReceiptsToRPC":
		return true
	default:
		return false
	}
}

func jsonRPCExprIsCallArg(call *ast.CallExpr, expr ast.Expr) bool {
	for _, arg := range call.Args {
		if arg == expr {
			return true
		}
	}
	return false
}

func isBackendSelector(expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "backend" {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && (ident.Name == "e" || ident.Name == "api")
}

func compareArchiveBackendCalls(expected, actual map[string][]string) string {
	var lines []string
	for name, want := range expected {
		got, ok := actual[name]
		if !ok {
			lines = append(lines, fmt.Sprintf("%s: missing audited handler", name))
			continue
		}
		want = sortedJSONRPCStringSlice(want)
		got = sortedJSONRPCStringSlice(got)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			lines = append(lines, fmt.Sprintf("%s: backend calls got [%s], want [%s]",
				name, strings.Join(got, ","), strings.Join(want, ",")))
		}
	}
	for name, got := range actual {
		if _, ok := expected[name]; ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: unexpected block-tag handler with backend calls [%s]",
			name, strings.Join(got, ",")))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func sortedJSONRPCAuditKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedJSONRPCStringSlice(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
