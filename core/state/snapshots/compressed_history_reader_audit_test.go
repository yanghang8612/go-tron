package snapshots

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

func TestStateDomainHistoryRecordReadersUseCompressedOpeners(t *testing.T) {
	expected := map[string][]string{
		"checkStateDomainChangeBinarySegment":                        {"openHistorySegmentForRead"},
		"copyStateDomainChangeBinarySegmentPayload":                  {"openStateDomainChangeBinarySegmentReader"},
		"iterateStateDomainChangeBinarySegmentByAccessorFile":        {"openHistorySegmentForRead", "openStateDomainChangeBinaryAccessorReader"},
		"iterateStateDomainChangeBinarySegmentByAccessorPrefixFile":  {"openHistorySegmentForRead", "openStateDomainChangeBinaryAccessorReader"},
		"iterateStateDomainChangeBinarySegmentTxRangeByIndexFile":    {"openHistorySegmentForRead"},
		"readStateDomainChangeBinarySegment":                         {"openHistorySegmentForRead"},
		"readStateDomainChangeBinarySegmentByAccessorEntries":        {"openHistorySegmentForRead"},
		"readStateDomainChangeBinarySegmentTxRange":                  {"openHistorySegmentForRead"},
		"readStateDomainChangeBinaryTxRangeForBlockByIndexFile":      {"openHistorySegmentForRead"},
		"stateDomainChangeBinaryIndexBlockLowerBound":                nil,
		"validateCompressedHistorySegmentReadable":                   {"openHistorySegmentForRead"},
		"validateStateDomainChangeBinaryAccessorEntryAgainstSegment": nil,
		"verifyStateDomainChangeBinaryIndexCoverage":                 nil,
	}

	actual := make(map[string][]string)
	file := parseHistoryBinarySourceForAudit(t)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if !callsStateDomainRecordReader(fn.Body) {
			continue
		}
		actual[fn.Name.Name] = compressedHistoryOpeners(fn.Body)
	}

	if diff := compareCompressedHistoryRecordReaders(expected, actual); diff != "" {
		t.Fatalf("state-domain history record reader surface changed; route production readers through compressed-aware openers or update audited helper exceptions:\n%s", diff)
	}
}

func parseHistoryBinarySourceForAudit(t *testing.T) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "history_binary.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse history_binary.go: %v", err)
	}
	return file
}

func callsStateDomainRecordReader(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		if found {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		switch ident.Name {
		case "readStateDomainChangeBinaryRecordAtBoundedIndex":
			found = true
			return false
		default:
			return true
		}
	})
	return found
}

func compressedHistoryOpeners(body *ast.BlockStmt) []string {
	watched := map[string]struct{}{
		"openHistorySegmentForRead":                 {},
		"openStateDomainChangeBinaryAccessorReader": {},
		"openStateDomainChangeBinarySegmentReader":  {},
	}
	seen := make(map[string]struct{})
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		if _, ok := watched[ident.Name]; ok {
			seen[ident.Name] = struct{}{}
		}
		return true
	})
	return sortedCompressedHistoryAuditKeys(seen)
}

func compareCompressedHistoryRecordReaders(expected, actual map[string][]string) string {
	var lines []string
	for name, want := range expected {
		got, ok := actual[name]
		if !ok {
			lines = append(lines, fmt.Sprintf("%s: missing audited record-reader call site", name))
			continue
		}
		if !sameCompressedHistoryAuditStringSet(got, want) {
			lines = append(lines, fmt.Sprintf("%s: compressed-aware openers got [%s], want [%s]",
				name, strings.Join(got, ","), strings.Join(sortedCompressedHistoryAuditStringSlice(want), ",")))
		}
	}
	for name, got := range actual {
		if _, ok := expected[name]; ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: unexpected state-domain record-reader call site with openers [%s]",
			name, strings.Join(got, ",")))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func sameCompressedHistoryAuditStringSet(got, want []string) bool {
	got = sortedCompressedHistoryAuditStringSlice(got)
	want = sortedCompressedHistoryAuditStringSlice(want)
	return strings.Join(got, ",") == strings.Join(want, ",")
}

func sortedCompressedHistoryAuditKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedCompressedHistoryAuditStringSlice(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
