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

func parseJSONRPCSource(t *testing.T, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return file
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
	watched := map[string]struct{}{
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
		if _, ok := watched[sel.Sel.Name]; ok {
			seen[sel.Sel.Name] = struct{}{}
		}
		return true
	})
	return sortedJSONRPCAuditKeys(seen)
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
