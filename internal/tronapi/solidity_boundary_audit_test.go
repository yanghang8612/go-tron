package tronapi

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestTronAPIBoundHandlersUseArchiveBackends(t *testing.T) {
	expectedArchive := map[string][]string{
		"handleEstimateEnergy":                     {"EstimateEnergy", "EstimateEnergyAt"},
		"handleGetAccount":                         {"GetAccount", "GetAccountAt"},
		"handleGetAccountById":                     {"GetAccountById", "GetAccountByIdAt"},
		"handleGetAccountNet":                      {"GetAccountNet", "GetAccountNetAt"},
		"handleGetAccountResource":                 {"GetAccountResource", "GetAccountResourceAt"},
		"handleGetAssetIssueByID":                  {"GetAssetIssueByID", "GetAssetIssueByIDAt"},
		"handleGetAssetIssueByName":                {"GetAssetIssueByName", "GetAssetIssueByNameAt"},
		"handleGetAssetIssueList":                  {"GetAssetIssueList", "GetAssetIssueListAt"},
		"handleGetBrokerage":                       {"GetBrokerageInfo", "GetBrokerageInfoAt"},
		"handleGetChainParameters":                 {"GetChainParameters", "GetChainParametersAt"},
		"handleGetContract":                        {"GetContract", "GetContractAt"},
		"handleGetDelegatedResourceAccountIndexV2": {"GetDelegatedResourceAccountIndexV2", "GetDelegatedResourceAccountIndexV2At"},
		"handleGetDelegatedResourceV2":             {"GetDelegatedResourceV2", "GetDelegatedResourceV2At"},
		"handleGetMarketOrderByID":                 {"GetMarketOrderByID", "GetMarketOrderByIDAt"},
		"handleGetMarketOrdersFromAccount":         {"GetMarketOrdersByAccount", "GetMarketOrdersByAccountAt"},
		"handleGetMarketPriceByPair":               {"GetMarketPriceByPair", "GetMarketPriceByPairAt"},
		"handleGetNextMaintenanceTime":             {"NextMaintenanceTime", "NextMaintenanceTimeAt"},
		"handleGetPaginatedAssetIssueList":         {"GetAssetIssueListPaginated", "GetAssetIssueListPaginatedAt"},
		"handleGetPaginatedProposalList":           {"ListProposalsPaginated", "ListProposalsPaginatedAt"},
		"handleGetProposalById":                    {"GetProposalByID", "GetProposalByIDAt"},
		"handleGetReward":                          {"GetReward", "GetRewardAt"},
		"handleListExchanges":                      {"ListExchanges", "ListExchangesAt"},
		"handleListProposals":                      {"ListProposals", "ListProposalsAt"},
		"handleListWitnesses":                      {"ListWitnesses", "ListWitnessesAt"},
		"handleTriggerConstantContract":            {"TriggerConstantContract", "TriggerConstantContractAt"},
	}
	expectedBoundGate := map[string][]string{
		"handleGetBlockByIDAtBound":           nil,
		"handleGetBlockByLimitNextAtBound":    nil,
		"handleGetTransactionByIDAtBound":     nil,
		"handleGetTransactionInfoByIDAtBound": nil,
		"transactionWithinBound":              {"GetTransactionBlockNumByID"},
	}

	actual := make(map[string][]string)
	for _, file := range parseTronAPISourceFiles(t) {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isTronAPIAuditReceiverMethod(fn, "*API") {
				continue
			}
			if !hasTronAPIBoundFnParam(fn.Type.Params) {
				continue
			}
			actual[fn.Name.Name] = tronAPIBackendCalls(fn.Body)
		}
	}

	if diff := compareTronAPIBoundHandlerCalls(expectedArchive, expectedBoundGate, actual); diff != "" {
		t.Fatalf("TRON HTTP solid/PBFT archive routing changed; audit live/archive backend calls and update this test:\n%s", diff)
	}
}

func TestSolidityBoundWrappersCallExpectedHandlers(t *testing.T) {
	expected := map[string]tronAPIBoundWrapperCall{
		"estimatePbftEnergy":                      {method: "handleEstimateEnergy", bound: "pbftBoundNum"},
		"estimateSolidEnergy":                     {method: "handleEstimateEnergy", bound: "solidBoundNum"},
		"getPbftAccount":                          {method: "handleGetAccount", bound: "pbftBoundNum"},
		"getPbftAccountById":                      {method: "handleGetAccountById", bound: "pbftBoundNum"},
		"getPbftAccountNet":                       {method: "handleGetAccountNet", bound: "pbftBoundNum"},
		"getPbftAccountResource":                  {method: "handleGetAccountResource", bound: "pbftBoundNum"},
		"getPbftAssetIssueByID":                   {method: "handleGetAssetIssueByID", bound: "pbftBoundNum"},
		"getPbftAssetIssueByName":                 {method: "handleGetAssetIssueByName", bound: "pbftBoundNum"},
		"getPbftAssetIssueList":                   {method: "handleGetAssetIssueList", bound: "pbftBoundNum"},
		"getPbftBlockByID":                        {method: "handleGetBlockByIDAtBound", bound: "pbftBoundNum"},
		"getPbftBlockByLimitNext":                 {method: "handleGetBlockByLimitNextAtBound", bound: "pbftBoundNum"},
		"getPbftBrokerage":                        {method: "handleGetBrokerage", bound: "pbftBoundNum"},
		"getPbftChainParameters":                  {method: "handleGetChainParameters", bound: "pbftBoundNum"},
		"getPbftContract":                         {method: "handleGetContract", bound: "pbftBoundNum"},
		"getPbftDelegatedResourceAccountIndexV2":  {method: "handleGetDelegatedResourceAccountIndexV2", bound: "pbftBoundNum"},
		"getPbftDelegatedResourceV2":              {method: "handleGetDelegatedResourceV2", bound: "pbftBoundNum"},
		"getPbftExchanges":                        {method: "handleListExchanges", bound: "pbftBoundNum"},
		"getPbftMarketOrderByID":                  {method: "handleGetMarketOrderByID", bound: "pbftBoundNum"},
		"getPbftMarketOrdersFromAccount":          {method: "handleGetMarketOrdersFromAccount", bound: "pbftBoundNum"},
		"getPbftMarketPriceByPair":                {method: "handleGetMarketPriceByPair", bound: "pbftBoundNum"},
		"getPbftNextMaintenanceTime":              {method: "handleGetNextMaintenanceTime", bound: "pbftBoundNum"},
		"getPbftPaginatedAssetIssueList":          {method: "handleGetPaginatedAssetIssueList", bound: "pbftBoundNum"},
		"getPbftPaginatedProposalList":            {method: "handleGetPaginatedProposalList", bound: "pbftBoundNum"},
		"getPbftProposalByID":                     {method: "handleGetProposalById", bound: "pbftBoundNum"},
		"getPbftProposals":                        {method: "handleListProposals", bound: "pbftBoundNum"},
		"getPbftReward":                           {method: "handleGetReward", bound: "pbftBoundNum"},
		"getPbftTransactionByID":                  {method: "handleGetTransactionByIDAtBound", bound: "pbftBoundNum"},
		"getPbftTransactionInfoByID":              {method: "handleGetTransactionInfoByIDAtBound", bound: "pbftBoundNum"},
		"getPbftWitnesses":                        {method: "handleListWitnesses", bound: "pbftBoundNum"},
		"getSolidAccount":                         {method: "handleGetAccount", bound: "solidBoundNum"},
		"getSolidAccountById":                     {method: "handleGetAccountById", bound: "solidBoundNum"},
		"getSolidAccountNet":                      {method: "handleGetAccountNet", bound: "solidBoundNum"},
		"getSolidAccountResource":                 {method: "handleGetAccountResource", bound: "solidBoundNum"},
		"getSolidAssetIssueByID":                  {method: "handleGetAssetIssueByID", bound: "solidBoundNum"},
		"getSolidAssetIssueByName":                {method: "handleGetAssetIssueByName", bound: "solidBoundNum"},
		"getSolidAssetIssueList":                  {method: "handleGetAssetIssueList", bound: "solidBoundNum"},
		"getSolidBlockByID":                       {method: "handleGetBlockByIDAtBound", bound: "solidBoundNum"},
		"getSolidBlockByLimitNext":                {method: "handleGetBlockByLimitNextAtBound", bound: "solidBoundNum"},
		"getSolidBrokerage":                       {method: "handleGetBrokerage", bound: "solidBoundNum"},
		"getSolidChainParameters":                 {method: "handleGetChainParameters", bound: "solidBoundNum"},
		"getSolidContract":                        {method: "handleGetContract", bound: "solidBoundNum"},
		"getSolidDelegatedResourceAccountIndexV2": {method: "handleGetDelegatedResourceAccountIndexV2", bound: "solidBoundNum"},
		"getSolidDelegatedResourceV2":             {method: "handleGetDelegatedResourceV2", bound: "solidBoundNum"},
		"getSolidExchanges":                       {method: "handleListExchanges", bound: "solidBoundNum"},
		"getSolidMarketOrderByID":                 {method: "handleGetMarketOrderByID", bound: "solidBoundNum"},
		"getSolidMarketOrdersFromAccount":         {method: "handleGetMarketOrdersFromAccount", bound: "solidBoundNum"},
		"getSolidMarketPriceByPair":               {method: "handleGetMarketPriceByPair", bound: "solidBoundNum"},
		"getSolidNextMaintenanceTime":             {method: "handleGetNextMaintenanceTime", bound: "solidBoundNum"},
		"getSolidPaginatedAssetIssueList":         {method: "handleGetPaginatedAssetIssueList", bound: "solidBoundNum"},
		"getSolidPaginatedProposalList":           {method: "handleGetPaginatedProposalList", bound: "solidBoundNum"},
		"getSolidProposalByID":                    {method: "handleGetProposalById", bound: "solidBoundNum"},
		"getSolidProposals":                       {method: "handleListProposals", bound: "solidBoundNum"},
		"getSolidReward":                          {method: "handleGetReward", bound: "solidBoundNum"},
		"getSolidTransactionByID":                 {method: "handleGetTransactionByIDAtBound", bound: "solidBoundNum"},
		"getSolidTransactionInfoByID":             {method: "handleGetTransactionInfoByIDAtBound", bound: "solidBoundNum"},
		"getSolidWitnesses":                       {method: "handleListWitnesses", bound: "solidBoundNum"},
		"triggerPbftConstantContract":             {method: "handleTriggerConstantContract", bound: "pbftBoundNum"},
		"triggerSolidConstantContract":            {method: "handleTriggerConstantContract", bound: "solidBoundNum"},
	}

	actual := make(map[string][]tronAPIBoundWrapperCall)
	file := parseTronAPISource(t, "solidity.go")
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !isTronAPIAuditReceiverMethod(fn, "*API") {
			continue
		}
		if calls := tronAPIBoundWrapperCalls(fn.Body); len(calls) > 0 {
			actual[fn.Name.Name] = calls
		}
	}

	if diff := compareTronAPIBoundWrapperCalls(expected, actual); diff != "" {
		t.Fatalf("TRON HTTP solid/PBFT wrapper routing changed; audit wrapper bounds and update this test:\n%s", diff)
	}
}

func parseTronAPISourceFiles(t *testing.T) []*ast.File {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob tronapi sources: %v", err)
	}
	files := make([]*ast.File, 0, len(paths))
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		files = append(files, parseTronAPISource(t, path))
	}
	return files
}

func parseTronAPISource(t *testing.T, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return file
}

func isTronAPIAuditReceiverMethod(fn *ast.FuncDecl, receiver string) bool {
	if fn == nil || fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	return tronAPIAuditExprTypeName(fn.Recv.List[0].Type) == receiver
}

func hasTronAPIBoundFnParam(fields *ast.FieldList) bool {
	if fields == nil {
		return false
	}
	for _, field := range fields.List {
		if !isTronAPIBoundFnType(field.Type) {
			continue
		}
		for _, name := range field.Names {
			if name.Name == "boundFn" {
				return true
			}
		}
	}
	return false
}

func isTronAPIBoundFnType(expr ast.Expr) bool {
	fn, ok := expr.(*ast.FuncType)
	if !ok {
		return false
	}
	if fn.Params != nil && len(fn.Params.List) != 0 {
		return false
	}
	if fn.Results == nil || len(fn.Results.List) != 1 {
		return false
	}
	return tronAPIAuditExprTypeName(fn.Results.List[0].Type) == "uint64"
}

func tronAPIAuditExprTypeName(expr ast.Expr) string {
	switch typ := expr.(type) {
	case *ast.Ident:
		return typ.Name
	case *ast.StarExpr:
		return "*" + tronAPIAuditExprTypeName(typ.X)
	default:
		return ""
	}
}

func tronAPIBackendCalls(body *ast.BlockStmt) []string {
	seen := make(map[string]struct{})
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !isTronAPIBackendSelector(sel.X) {
			return true
		}
		seen[sel.Sel.Name] = struct{}{}
		return true
	})
	return sortedTronAPIAuditKeys(seen)
}

func isTronAPIBackendSelector(expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "backend" {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == "api"
}

func compareTronAPIBoundHandlerCalls(expectedArchive, expectedBoundGate, actual map[string][]string) string {
	var lines []string
	for name, want := range expectedArchive {
		got, ok := actual[name]
		if !ok {
			lines = append(lines, fmt.Sprintf("%s: missing archive-bound handler", name))
			continue
		}
		if !sameTronAPIStringSet(got, want) {
			lines = append(lines, fmt.Sprintf("%s: backend calls got [%s], want archive pair [%s]",
				name, strings.Join(got, ","), strings.Join(sortedTronAPIStringSlice(want), ",")))
		}
	}
	for name, want := range expectedBoundGate {
		got, ok := actual[name]
		if !ok {
			lines = append(lines, fmt.Sprintf("%s: missing non-archive bound gate", name))
			continue
		}
		if !sameTronAPIStringSet(got, want) {
			lines = append(lines, fmt.Sprintf("%s: backend calls got [%s], want bound-gate calls [%s]",
				name, strings.Join(got, ","), strings.Join(sortedTronAPIStringSlice(want), ",")))
		}
	}
	for name, got := range actual {
		if _, ok := expectedArchive[name]; ok {
			continue
		}
		if _, ok := expectedBoundGate[name]; ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: unexpected boundFn handler with backend calls [%s]",
			name, strings.Join(got, ",")))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

type tronAPIBoundWrapperCall struct {
	method string
	bound  string
}

func (c tronAPIBoundWrapperCall) String() string {
	if c.method == "" && c.bound == "" {
		return "<none>"
	}
	return fmt.Sprintf("%s(api.%s)", c.method, c.bound)
}

func tronAPIBoundWrapperCalls(body *ast.BlockStmt) []tronAPIBoundWrapperCall {
	var out []tronAPIBoundWrapperCall
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !isTronAPIAuditAPISelector(sel.X) {
			return true
		}
		for _, arg := range call.Args {
			bound, ok := tronAPIBoundSelectorName(arg)
			if !ok {
				continue
			}
			out = append(out, tronAPIBoundWrapperCall{method: sel.Sel.Name, bound: bound})
		}
		return true
	})
	return out
}

func isTronAPIAuditAPISelector(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == "api"
}

func tronAPIBoundSelectorName(expr ast.Expr) (string, bool) {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || !isTronAPIAuditAPISelector(sel.X) {
		return "", false
	}
	switch sel.Sel.Name {
	case "solidBoundNum", "pbftBoundNum":
		return sel.Sel.Name, true
	default:
		return "", false
	}
}

func compareTronAPIBoundWrapperCalls(expected map[string]tronAPIBoundWrapperCall, actual map[string][]tronAPIBoundWrapperCall) string {
	var lines []string
	for name, want := range expected {
		got, ok := actual[name]
		if !ok {
			lines = append(lines, fmt.Sprintf("%s: missing bound wrapper", name))
			continue
		}
		if len(got) != 1 || got[0] != want {
			lines = append(lines, fmt.Sprintf("%s: bound wrapper calls got [%s], want [%s]",
				name, formatTronAPIBoundWrapperCalls(got), want))
		}
	}
	for name, got := range actual {
		if _, ok := expected[name]; ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: unexpected bound wrapper calls [%s]",
			name, formatTronAPIBoundWrapperCalls(got)))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func formatTronAPIBoundWrapperCalls(calls []tronAPIBoundWrapperCall) string {
	parts := make([]string, 0, len(calls))
	for _, call := range calls {
		parts = append(parts, call.String())
	}
	return strings.Join(parts, ",")
}

func sameTronAPIStringSet(got, want []string) bool {
	got = sortedTronAPIStringSlice(got)
	want = sortedTronAPIStringSlice(want)
	return strings.Join(got, ",") == strings.Join(want, ",")
}

func sortedTronAPIAuditKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedTronAPIStringSlice(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
