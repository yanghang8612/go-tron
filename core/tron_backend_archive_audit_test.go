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
