package grpcapi_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

func TestSolidityServerBackendCallsAreAudited(t *testing.T) {
	expected := map[string][]string{
		"EstimateEnergy":                     {"EstimateEnergyAt"},
		"GetAccount":                         {"GetAccountAt"},
		"GetAccountById":                     {"GetAccountAt", "GetAccountByIdAt"},
		"GetAssetIssueById":                  {"GetAssetIssueByIDAt"},
		"GetAssetIssueByName":                {"GetAssetIssueByNameAt"},
		"GetAssetIssueList":                  {"GetAssetIssueListAt"},
		"GetAvailableUnfreezeCount":          {"GetAvailableUnfreezeCountAt"},
		"GetBandwidthPrices":                 {"GetBandwidthPricesAt"},
		"GetBlock":                           {"GetBlockByHash"},
		"GetBlockByNum":                      {"GetBlockByNumber"},
		"GetBlockByNum2":                     {"GetBlockByNumber"},
		"GetBrokerageInfo":                   {"GetBrokerageInfoAt"},
		"GetBurnTrx":                         {"GetBurnTrxAt"},
		"GetCanDelegatedMaxSize":             {"CanDelegateResourceAt"},
		"GetCanWithdrawUnfreezeAmount":       {"GetCanWithdrawUnfreezeAmountAt"},
		"GetDelegatedResourceAccountIndexV2": {"GetDelegatedResourceAccountIndexV2At"},
		"GetDelegatedResourceV2":             {"GetDelegatedResourceV2At"},
		"GetEnergyPrices":                    {"GetEnergyPricesAt"},
		"GetExchangeById":                    {"GetExchangeByIDAt"},
		"GetMarketOrderByAccount":            {"GetMarketOrdersByAccountAt"},
		"GetMarketOrderById":                 {"GetMarketOrderByIDAt"},
		"GetMarketOrderListByPair":           {"GetMarketOrderListByPairAt"},
		"GetMarketPairList":                  {"GetMarketPairListAt"},
		"GetMarketPriceByPair":               {"GetMarketPriceByPairAt"},
		"GetNowBlock":                        {"GetBlockByNumber"},
		"GetNowBlock2":                       {"GetBlockByNumber"},
		"GetPaginatedAssetIssueList":         {"GetAssetIssueListPaginatedAt"},
		"GetRewardInfo":                      {"GetRewardAt"},
		"GetTransactionById":                 {"GetTransactionByID"},
		"GetTransactionCountByBlockNum":      {"GetBlockByNumber"},
		"GetTransactionInfoByBlockNum":       {"GetTransactionInfoByBlockNum"},
		"GetTransactionInfoById":             {"GetTransactionInfoByID"},
		"ListExchanges":                      {"ListExchangesAt"},
		"ListWitnesses":                      {"ListWitnessesAt"},
		"TriggerConstantContract":            {"TriggerConstantContractAt"},
		"getSolidBlockByNumber":              {"GetBlockByNumber"},
		"solidNum":                           {"SolidifiedBlockNum"},
		"transactionWithinSolid":             {"GetTransactionBlockNumByID"},
	}

	actual := solidityServerBackendCalls(t)
	if diff := compareSolidityServerBackendCalls(expected, actual); diff != "" {
		t.Fatalf("WalletSolidity backend call surface changed; audit solid/archive boundaries and update this test:\n%s", diff)
	}
}

func TestSolidityServerArchiveCallsUseSolidBound(t *testing.T) {
	expectedArchive := map[string][]string{
		"EstimateEnergy":                     {"EstimateEnergyAt"},
		"GetAccount":                         {"GetAccountAt"},
		"GetAccountById":                     {"GetAccountAt", "GetAccountByIdAt"},
		"GetAssetIssueById":                  {"GetAssetIssueByIDAt"},
		"GetAssetIssueByName":                {"GetAssetIssueByNameAt"},
		"GetAssetIssueList":                  {"GetAssetIssueListAt"},
		"GetAvailableUnfreezeCount":          {"GetAvailableUnfreezeCountAt"},
		"GetBandwidthPrices":                 {"GetBandwidthPricesAt"},
		"GetBrokerageInfo":                   {"GetBrokerageInfoAt"},
		"GetBurnTrx":                         {"GetBurnTrxAt"},
		"GetCanDelegatedMaxSize":             {"CanDelegateResourceAt"},
		"GetCanWithdrawUnfreezeAmount":       {"GetCanWithdrawUnfreezeAmountAt"},
		"GetDelegatedResourceAccountIndexV2": {"GetDelegatedResourceAccountIndexV2At"},
		"GetDelegatedResourceV2":             {"GetDelegatedResourceV2At"},
		"GetEnergyPrices":                    {"GetEnergyPricesAt"},
		"GetExchangeById":                    {"GetExchangeByIDAt"},
		"GetMarketOrderByAccount":            {"GetMarketOrdersByAccountAt"},
		"GetMarketOrderById":                 {"GetMarketOrderByIDAt"},
		"GetMarketOrderListByPair":           {"GetMarketOrderListByPairAt"},
		"GetMarketPairList":                  {"GetMarketPairListAt"},
		"GetMarketPriceByPair":               {"GetMarketPriceByPairAt"},
		"GetPaginatedAssetIssueList":         {"GetAssetIssueListPaginatedAt"},
		"GetRewardInfo":                      {"GetRewardAt"},
		"ListExchanges":                      {"ListExchangesAt"},
		"ListWitnesses":                      {"ListWitnessesAt"},
		"TriggerConstantContract":            {"TriggerConstantContractAt"},
	}

	file := parseGRPCAPISoliditySource(t)
	var lines []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !isGRPCAPISolidityReceiver(fn) {
			continue
		}
		wantCalls, ok := expectedArchive[fn.Name.Name]
		if !ok {
			continue
		}
		found := make(map[string]int)
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !isGRPCAPISolidityBackendSelector(sel.X) {
				return true
			}
			if !solidityAuditStringInSlice(sel.Sel.Name, wantCalls) {
				return true
			}
			found[sel.Sel.Name]++
			if !solidityCallHasSolidNumArg(call) {
				lines = append(lines, fmt.Sprintf("%s.%s: archive backend call must use s.solidNum()",
					fn.Name.Name, sel.Sel.Name))
			}
			return true
		})
		for _, want := range wantCalls {
			if found[want] == 0 {
				lines = append(lines, fmt.Sprintf("%s: missing archive backend call %s", fn.Name.Name, want))
			}
		}
	}
	for name := range expectedArchive {
		if !solidityServerMethodExists(file, name) {
			lines = append(lines, fmt.Sprintf("%s: missing WalletSolidity archive method", name))
		}
	}
	sort.Strings(lines)
	if len(lines) != 0 {
		t.Fatalf("WalletSolidity archive calls must be solid-bound:\n%s", strings.Join(lines, "\n"))
	}
}

func TestSolidityServerDoesNotAliasBackend(t *testing.T) {
	source := parseGRPCAPISolidityAuditSource(t, "solidity.go", nil)
	var offenders []string
	for _, decl := range source.file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !isGRPCAPISolidityReceiver(fn) {
			continue
		}
		offenders = append(offenders, grpcAPISolidityBackendAliasOffenders(source.fset, source.path, fn.Body)...)
		offenders = append(offenders, grpcAPISolidityBackendReceiverEscapeOffenders(source.fset, source.path, fn.Body)...)
		offenders = append(offenders, grpcAPISolidityBackendMethodAliasOffenders(source.fset, source.path, fn.Body)...)
	}
	if len(offenders) > 0 {
		t.Fatalf("WalletSolidity handlers must call s.backend methods directly; aliases bypass archive-boundary audits:\n%s",
			strings.Join(offenders, "\n"))
	}
}

func TestSolidityServerBackendAliasAuditRejectsBackendReceiverAlias(t *testing.T) {
	source := parseGRPCAPISolidityAuditSource(t, "fixture.go", `package grpcapi

type SolidityServer struct{ backend *backend }
type backend struct{}

func (b *backend) GetAccountAt(string, uint64) error { return nil }

func (s *SolidityServer) GetAccount() error {
	backend := s.backend
	return backend.GetAccountAt("addr", s.solidNum())
}
`)
	offenders := grpcAPISolidityBackendAliasOffenders(source.fset, source.path, onlyGRPCAPISolidityFuncBody(t, source.file, "GetAccount"))
	if len(offenders) != 1 || !strings.Contains(offenders[0], "backend receiver assigned to alias") {
		t.Fatalf("offenders = %+v, want backend receiver alias rejected", offenders)
	}
}

func TestSolidityServerBackendAliasAuditRejectsBackendReceiverArgument(t *testing.T) {
	source := parseGRPCAPISolidityAuditSource(t, "fixture.go", `package grpcapi

type SolidityServer struct{ backend *backend }
type backend struct{}

func useBackend(*backend, uint64) error { return nil }

func (s *SolidityServer) GetAccount() error {
	return useBackend(s.backend, s.solidNum())
}
`)
	offenders := grpcAPISolidityBackendReceiverEscapeOffenders(source.fset, source.path, onlyGRPCAPISolidityFuncBody(t, source.file, "GetAccount"))
	if len(offenders) != 1 || !strings.Contains(offenders[0], "backend receiver referenced outside a method selector") {
		t.Fatalf("offenders = %+v, want backend receiver argument rejected", offenders)
	}
}

func TestSolidityServerBackendAliasAuditRejectsBackendMethodAlias(t *testing.T) {
	source := parseGRPCAPISolidityAuditSource(t, "fixture.go", `package grpcapi

type SolidityServer struct{ backend *backend }
type backend struct{}

func (b *backend) GetAccountAt(string, uint64) error { return nil }

func (s *SolidityServer) GetAccount() error {
	read := s.backend.GetAccountAt
	return read("addr", s.solidNum())
}
`)
	offenders := grpcAPISolidityBackendMethodAliasOffenders(source.fset, source.path, onlyGRPCAPISolidityFuncBody(t, source.file, "GetAccount"))
	if len(offenders) != 1 || !strings.Contains(offenders[0], "backend method referenced outside a direct call") {
		t.Fatalf("offenders = %+v, want backend method alias rejected", offenders)
	}
}

func solidityServerBackendCalls(t *testing.T) map[string][]string {
	t.Helper()
	out := make(map[string][]string)
	file := parseGRPCAPISoliditySource(t)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !isGRPCAPISolidityReceiver(fn) {
			continue
		}
		calls := make(map[string]struct{})
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !isGRPCAPISolidityBackendSelector(sel.X) {
				return true
			}
			calls[sel.Sel.Name] = struct{}{}
			return true
		})
		if len(calls) != 0 {
			out[fn.Name.Name] = sortedSolidityAuditKeys(calls)
		}
	}
	return out
}

type grpcAPISolidityAuditSource struct {
	path string
	fset *token.FileSet
	file *ast.File
}

func parseGRPCAPISoliditySource(t *testing.T) *ast.File {
	t.Helper()
	return parseGRPCAPISolidityAuditSource(t, "solidity.go", nil).file
}

func parseGRPCAPISolidityAuditSource(t *testing.T, path string, source any) grpcAPISolidityAuditSource {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return grpcAPISolidityAuditSource{path: path, fset: fset, file: file}
}

func onlyGRPCAPISolidityFuncBody(t *testing.T, file *ast.File, name string) *ast.BlockStmt {
	t.Helper()
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == name && fn.Body != nil {
			return fn.Body
		}
	}
	t.Fatalf("function %s not found", name)
	return nil
}

func isGRPCAPISolidityReceiver(fn *ast.FuncDecl) bool {
	if fn == nil || fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	return solidityAuditExprTypeName(fn.Recv.List[0].Type) == "*SolidityServer"
}

func solidityAuditExprTypeName(expr ast.Expr) string {
	switch typ := expr.(type) {
	case *ast.Ident:
		return typ.Name
	case *ast.StarExpr:
		return "*" + solidityAuditExprTypeName(typ.X)
	default:
		return ""
	}
}

func isGRPCAPISolidityBackendSelector(expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "backend" {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == "s"
}

func grpcAPISolidityBackendAliasOffenders(fset *token.FileSet, path string, body *ast.BlockStmt) []string {
	var offenders []string
	ast.Inspect(body, func(node ast.Node) bool {
		switch stmt := node.(type) {
		case *ast.AssignStmt:
			for _, rhs := range stmt.Rhs {
				if isGRPCAPISolidityBackendSelector(rhs) {
					offenders = append(offenders, fmt.Sprintf("%s:%d: backend receiver assigned to alias",
						path, fset.Position(rhs.Pos()).Line))
				}
			}
		case *ast.ValueSpec:
			for _, value := range stmt.Values {
				if isGRPCAPISolidityBackendSelector(value) {
					offenders = append(offenders, fmt.Sprintf("%s:%d: backend receiver assigned to alias",
						path, fset.Position(value.Pos()).Line))
				}
			}
		}
		return true
	})
	return offenders
}

func grpcAPISolidityBackendReceiverEscapeOffenders(fset *token.FileSet, path string, body *ast.BlockStmt) []string {
	var offenders []string
	stack := make([]ast.Node, 0, 16)
	ast.Inspect(body, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		var parent ast.Node
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		sel, ok := node.(*ast.SelectorExpr)
		if ok && isGRPCAPISolidityBackendSelector(sel) && !grpcAPISolidityBackendMethodReceiver(parent, sel) {
			offenders = append(offenders, fmt.Sprintf("%s:%d: backend receiver referenced outside a method selector",
				path, fset.Position(sel.Pos()).Line))
		}
		stack = append(stack, node)
		return true
	})
	return offenders
}

func grpcAPISolidityBackendMethodAliasOffenders(fset *token.FileSet, path string, body *ast.BlockStmt) []string {
	var offenders []string
	stack := make([]ast.Node, 0, 16)
	ast.Inspect(body, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		var parent ast.Node
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		sel, ok := node.(*ast.SelectorExpr)
		if ok && isGRPCAPISolidityBackendSelector(sel.X) && !grpcAPISolidityDirectCallFun(parent, sel) {
			offenders = append(offenders, fmt.Sprintf("%s:%d: backend method referenced outside a direct call",
				path, fset.Position(sel.Pos()).Line))
		}
		stack = append(stack, node)
		return true
	})
	return offenders
}

func grpcAPISolidityBackendMethodReceiver(parent ast.Node, expr ast.Expr) bool {
	sel, ok := parent.(*ast.SelectorExpr)
	return ok && sel.X == expr
}

func grpcAPISolidityDirectCallFun(parent ast.Node, expr ast.Expr) bool {
	call, ok := parent.(*ast.CallExpr)
	return ok && call.Fun == expr
}

func solidityCallHasSolidNumArg(call *ast.CallExpr) bool {
	for _, arg := range call.Args {
		if solidityIsSolidNumCall(arg) {
			return true
		}
	}
	return false
}

func solidityIsSolidNumCall(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 0 {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "solidNum" {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == "s"
}

func solidityServerMethodExists(file *ast.File, name string) bool {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == name && isGRPCAPISolidityReceiver(fn) {
			return true
		}
	}
	return false
}

func compareSolidityServerBackendCalls(expected, actual map[string][]string) string {
	var lines []string
	for name, want := range expected {
		got, ok := actual[name]
		if !ok {
			lines = append(lines, fmt.Sprintf("%s: missing audited backend call site", name))
			continue
		}
		if !sameSolidityAuditStringSet(got, want) {
			lines = append(lines, fmt.Sprintf("%s: backend calls got [%s], want [%s]",
				name, strings.Join(got, ","), strings.Join(sortedSolidityAuditStringSlice(want), ",")))
		}
	}
	for name, got := range actual {
		if _, ok := expected[name]; ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: unexpected WalletSolidity backend calls [%s]",
			name, strings.Join(got, ",")))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func sameSolidityAuditStringSet(got, want []string) bool {
	got = sortedSolidityAuditStringSlice(got)
	want = sortedSolidityAuditStringSlice(want)
	return strings.Join(got, ",") == strings.Join(want, ",")
}

func solidityAuditStringInSlice(value string, values []string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func sortedSolidityAuditKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedSolidityAuditStringSlice(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
