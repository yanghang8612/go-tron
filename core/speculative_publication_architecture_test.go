package core

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

type speculativePublicationCall struct {
	name string
	pos  token.Pos
	call *ast.CallExpr
}

func speculativePublicationCallName(call *ast.CallExpr) string {
	switch function := call.Fun.(type) {
	case *ast.Ident:
		return function.Name
	case *ast.SelectorExpr:
		if receiver, ok := function.X.(*ast.Ident); ok {
			return receiver.Name + "." + function.Sel.Name
		}
		return function.Sel.Name
	default:
		return ""
	}
}

func speculativePublicationCallsBetween(calls []speculativePublicationCall, name string, start, end token.Pos) []speculativePublicationCall {
	var matches []speculativePublicationCall
	for _, call := range calls {
		if call.name == name && call.pos > start && call.pos < end {
			matches = append(matches, call)
		}
	}
	return matches
}

// TestSpeculativeCanonicalPublicationOnlyAppliesOracleSeals is an
// architecture guard for the most dangerous regression in this subsystem: a
// new publication cohort applying worker-owned writes directly. Private
// discard workers intentionally apply their own WriteSets elsewhere; this
// assertion is scoped to processBlockWithOptions, the canonical mutation
// boundary.
func TestSpeculativeCanonicalPublicationOnlyAppliesOracleSeals(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	sourcePath := filepath.Join(filepath.Dir(testFile), "state_processor.go")
	file, err := parser.ParseFile(token.NewFileSet(), sourcePath, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	var processBlock *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, isFunction := declaration.(*ast.FuncDecl)
		if isFunction && function.Name.Name == "processBlockWithOptions" {
			processBlock = function
			break
		}
	}
	if processBlock == nil {
		t.Fatal("processBlockWithOptions not found")
	}

	var calls []speculativePublicationCall
	ast.Inspect(processBlock.Body, func(node ast.Node) bool {
		if branch, isBranch := node.(*ast.BranchStmt); isBranch {
			if branch.Tok == token.CONTINUE {
				calls = append(calls, speculativePublicationCall{name: "continue", pos: branch.Pos()})
			}
			return true
		}
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		name := speculativePublicationCallName(call)
		calls = append(calls, speculativePublicationCall{name: name, pos: call.Pos(), call: call})
		selector, isSelector := call.Fun.(*ast.SelectorExpr)
		if !isSelector || (selector.Sel.Name != "ApplyTransactionWriteSetRecorded" && selector.Sel.Name != "ApplyTransactionWriteSet") {
			return true
		}
		if selector.Sel.Name != "ApplyTransactionWriteSetRecorded" {
			t.Errorf("canonical speculative path uses unaudited %s", selector.Sel.Name)
			return true
		}
		if len(call.Args) == 0 {
			t.Error("canonical WriteSet apply call has no payload")
			return true
		}
		payload, isPayloadSelector := call.Args[0].(*ast.SelectorExpr)
		seal, isSeal := payload.X.(*ast.Ident)
		if !isPayloadSelector || !isSeal || seal.Name != "writeSeal" || payload.Sel.Name != "writes" {
			t.Error("canonical WriteSet apply call does not consume writeSeal.writes")
		}
		return true
	})
	sort.Slice(calls, func(i, j int) bool { return calls[i].pos < calls[j].pos })

	seals := speculativePublicationCallsBetween(calls, "newCanonicalPublicationWriteSeal", processBlock.Body.Pos()-1, processBlock.Body.End()+1)
	if len(seals) != 4 {
		t.Fatalf("canonical publication seals = %d, want Transfer/VM direct/retry paths = 4", len(seals))
	}
	applyCalls := speculativePublicationCallsBetween(calls, "statedb.ApplyTransactionWriteSetRecorded", processBlock.Body.Pos()-1, processBlock.Body.End()+1)
	if len(applyCalls) != 4 {
		t.Fatalf("canonical speculative WriteSet apply calls = %d, want audited Transfer/VM direct/retry paths = 4", len(applyCalls))
	}

	previousSeal := processBlock.Body.Pos() - 1
	for index, seal := range seals {
		nextSeal := processBlock.Body.End() + 1
		if index+1 < len(seals) {
			nextSeal = seals[index+1].pos
		}
		if len(seal.call.Args) == 0 {
			t.Fatalf("publication seal %d has no family", index)
		}
		familyLiteral, ok := seal.call.Args[0].(*ast.BasicLit)
		if !ok || (familyLiteral.Value != `"Transfer"` && familyLiteral.Value != `"VM"`) {
			t.Fatalf("publication seal %d has non-literal family", index)
		}
		family := familyLiteral.Value[1 : len(familyLiteral.Value)-1]
		oracle := "validateTransferResultAtCanonicalBoundary"
		if family == "VM" {
			oracle = "validateVMResultAtCanonicalBoundary"
		}
		if got := len(speculativePublicationCallsBetween(calls, oracle, previousSeal, seal.pos)); got != 1 {
			t.Errorf("%s publication path %d canonical serial oracles = %d, want 1", family, index, got)
		}
		balanceOracles := len(speculativePublicationCallsBetween(calls, "validateTransferBalancePostImages", previousSeal, seal.pos))
		if family == "Transfer" && balanceOracles != 1 {
			t.Errorf("Transfer publication path %d balance oracles = %d, want 1", index, balanceOracles)
		}
		if family == "VM" && balanceOracles != 0 {
			t.Errorf("VM publication path %d unexpectedly contains %d Transfer balance oracles", index, balanceOracles)
		}

		applies := speculativePublicationCallsBetween(calls, "statedb.ApplyTransactionWriteSetRecorded", seal.pos, nextSeal)
		if len(applies) != 1 {
			t.Errorf("%s publication path %d applies = %d, want 1", family, index, len(applies))
			previousSeal = seal.pos
			continue
		}
		apply := applies[0]
		continues := speculativePublicationCallsBetween(calls, "continue", apply.pos, nextSeal)
		if len(continues) == 0 {
			t.Errorf("%s publication path %d has no loop continue after publication", family, index)
			previousSeal = seal.pos
			continue
		}
		postEnd := continues[0].pos
		beforeSourceChecks := speculativePublicationCallsBetween(calls, "writeSeal.validateSource", seal.pos, apply.pos)
		if len(beforeSourceChecks) != 1 {
			t.Errorf("%s publication path %d pre-apply source checks = %d, want 1", family, index, len(beforeSourceChecks))
		}
		afterSourceChecks := speculativePublicationCallsBetween(calls, "writeSeal.validateSource", apply.pos, postEnd)
		matches := speculativePublicationCallsBetween(calls, "writeSeal.markMatched", apply.pos, postEnd)
		observations := speculativePublicationCallsBetween(calls, "versionedShadow.ObserveTransaction", apply.pos, postEnd)
		audits := speculativePublicationCallsBetween(calls, "validateCanonicalPublicationWriteSet", apply.pos, postEnd)
		flushes := speculativePublicationCallsBetween(calls, "flushDomainChanges", apply.pos, postEnd)
		if len(afterSourceChecks) != 1 || len(matches) != 1 || len(observations) != 1 || len(audits) != 1 || len(flushes) != 1 {
			t.Errorf("%s publication path %d post-apply chain source/match/observe/audit/flush = %d/%d/%d/%d/%d, want 1/1/1/1/1",
				family, index, len(afterSourceChecks), len(matches), len(observations), len(audits), len(flushes))
		} else if apply.pos >= afterSourceChecks[0].pos ||
			afterSourceChecks[0].pos >= matches[0].pos ||
			matches[0].pos >= observations[0].pos ||
			observations[0].pos >= audits[0].pos ||
			audits[0].pos >= flushes[0].pos {
			t.Errorf("%s publication path %d safety chain is out of order", family, index)
		}
		previousSeal = seal.pos
	}
}

// TestVMCanonicalBoundaryRequiresIndependentDualOracles prevents the VM
// admission path from silently collapsing back to one state-acquisition path.
// The direct result must remain the publication carrier: unlike the isolated
// Copy result, it does not share copyStateObjectInto with the speculative
// block-execution base.
func TestVMCanonicalBoundaryRequiresIndependentDualOracles(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	sourcePath := filepath.Join(filepath.Dir(testFile), "versioned_shadow_worker.go")
	file, err := parser.ParseFile(token.NewFileSet(), sourcePath, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	var verifyVM *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, isFunction := declaration.(*ast.FuncDecl)
		if isFunction && function.Name.Name == "verifyVMResultAtCanonicalBoundary" {
			verifyVM = function
			break
		}
	}
	if verifyVM == nil {
		t.Fatal("verifyVMResultAtCanonicalBoundary not found")
	}

	callCounts := map[string]int{}
	callPositions := map[string]token.Pos{}
	ast.Inspect(verifyVM.Body, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		name := speculativePublicationCallName(call)
		switch name {
		case "verifyResultAtCanonicalBoundary", "verifyResultDirectAtCanonicalBoundary", "compareBoundaryCanonicalResults":
			callCounts[name]++
			callPositions[name] = call.Pos()
		}
		return true
	})
	for _, name := range []string{
		"verifyResultAtCanonicalBoundary",
		"verifyResultDirectAtCanonicalBoundary",
		"compareBoundaryCanonicalResults",
	} {
		if callCounts[name] != 1 {
			t.Errorf("%s calls = %d, want 1", name, callCounts[name])
		}
	}
	if callPositions["verifyResultAtCanonicalBoundary"] >= callPositions["verifyResultDirectAtCanonicalBoundary"] ||
		callPositions["verifyResultDirectAtCanonicalBoundary"] >= callPositions["compareBoundaryCanonicalResults"] {
		t.Error("VM canonical oracles or comparison are out of order")
	}

	if len(verifyVM.Body.List) == 0 {
		t.Fatal("verifyVMResultAtCanonicalBoundary has empty body")
	}
	finalReturn, ok := verifyVM.Body.List[len(verifyVM.Body.List)-1].(*ast.ReturnStmt)
	if !ok || len(finalReturn.Results) != 1 {
		t.Fatal("VM canonical boundary does not end with one publication result")
	}
	carrier, ok := finalReturn.Results[0].(*ast.Ident)
	if !ok || carrier.Name != "direct" {
		t.Errorf("VM publication carrier = %v, want direct oracle result", finalReturn.Results[0])
	}
}

// TestCanonicalOracleRestorationGuardProtectsDirectAndIsolatedPaths prevents
// either VM oracle (or Transfer's direct oracle) from dropping the live-state
// rollback proof. In particular, the isolated path must install its guard
// before StateDB.Copy: a future shallow alias must not contaminate the baseline
// later consumed by the direct oracle.
func TestCanonicalOracleRestorationGuardProtectsDirectAndIsolatedPaths(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	sourcePath := filepath.Join(filepath.Dir(testFile), "versioned_shadow_worker.go")
	file, err := parser.ParseFile(token.NewFileSet(), sourcePath, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	functions := map[string]*ast.FuncDecl{}
	for _, declaration := range file.Decls {
		function, isFunction := declaration.(*ast.FuncDecl)
		if !isFunction {
			continue
		}
		switch function.Name.Name {
		case "beginCanonicalOracleRestorationGuard", "revert", "verifyAndRevert",
			"verifyResultAtCanonicalBoundary", "verifyResultDirectAtCanonicalBoundary":
			functions[function.Name.Name] = function
		}
	}
	for _, name := range []string{
		"beginCanonicalOracleRestorationGuard", "revert", "verifyAndRevert",
		"verifyResultAtCanonicalBoundary", "verifyResultDirectAtCanonicalBoundary",
	} {
		if functions[name] == nil {
			t.Fatalf("%s not found", name)
		}
	}

	callsIn := func(function *ast.FuncDecl) map[string][]token.Pos {
		positions := map[string][]token.Pos{}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if isCall {
				name := speculativePublicationCallName(call)
				positions[name] = append(positions[name], call.Pos())
			}
			return true
		})
		return positions
	}
	assertCalls := func(functionName string, want map[string]int) map[string][]token.Pos {
		t.Helper()
		positions := callsIn(functions[functionName])
		for name, count := range want {
			if got := len(positions[name]); got != count {
				t.Errorf("%s %s calls = %d, want %d", functionName, name, got, count)
			}
		}
		return positions
	}
	assertCalls("beginCanonicalOracleRestorationGuard", map[string]int{
		"captureCanonicalCommutativePostImages": 1,
		"statedb.DomainChangeJournalMark":       1,
		"statedb.Snapshot":                      1,
		"dynProps.Snapshot":                     1,
	})
	assertCalls("revert", map[string]int{
		"RevertToSnapshot": 2,
	})
	assertCalls("verifyAndRevert", map[string]int{
		"captureCanonicalCommutativePostImages":            1,
		"DomainChangeJournalMark":                          2,
		"SnapshotChanged":                                  1,
		"guard.revert":                                     1,
		"restoreUnjournaledCanonicalCommutativePostImages": 1,
	})
	for _, path := range []string{"verifyResultAtCanonicalBoundary", "verifyResultDirectAtCanonicalBoundary"} {
		positions := assertCalls(path, map[string]int{
			"beginCanonicalOracleRestorationGuard": 1,
			"guard.revert":                         1,
			"verifyResultOnBoundaryState":          1,
			"guard.verifyAndRevert":                1,
		})
		begin := positions["beginCanonicalOracleRestorationGuard"]
		execute := positions["verifyResultOnBoundaryState"]
		verify := positions["guard.verifyAndRevert"]
		if len(begin) == 1 && len(execute) == 1 && len(verify) == 1 &&
			(begin[0] >= execute[0] || execute[0] >= verify[0]) {
			t.Errorf("%s restoration guard does not bracket oracle execution", path)
		}
	}
	isolated := callsIn(functions["verifyResultAtCanonicalBoundary"])
	if copies := isolated["statedb.Copy"]; len(copies) != 1 || isolated["beginCanonicalOracleRestorationGuard"][0] >= copies[0] {
		t.Error("isolated restoration guard does not begin before StateDB.Copy")
	}
}

func cachedAccountSource(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	switch selector.Sel.Name {
	case "GetAccount", "AccountReference", "AccountResourceReference":
		return true
	default:
		return false
	}
}

func cachedAccountMutator(name string) bool {
	for _, prefix := range []string{"Set", "Add", "Reduce", "Remove", "Clear", "Initialize", "Invalidate"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// TestExecutionCodeDoesNotMutateCachedAccountReferences freezes the ownership
// assumption behind the direct-oracle restoration seal. GetAccount and the
// explicit reference helpers expose the live cached wrapper for efficient
// reads; mutation must go through StateDB so snapshots and access recording see
// it. Core/state is the implementation boundary and is intentionally excluded.
func TestExecutionCodeDoesNotMutateCachedAccountReferences(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	repositoryRoot := filepath.Dir(filepath.Dir(testFile))
	for _, directory := range []string{"actuator", "core", "vm"} {
		root := filepath.Join(repositoryRoot, directory)
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if path == filepath.Join(repositoryRoot, "core", "state") || path == filepath.Join(repositoryRoot, "core", "types") {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fileSet := token.NewFileSet()
			file, parseErr := parser.ParseFile(fileSet, path, nil, 0)
			if parseErr != nil {
				return parseErr
			}
			for _, declaration := range file.Decls {
				function, isFunction := declaration.(*ast.FuncDecl)
				if !isFunction || function.Body == nil {
					continue
				}
				// This is the single intentional inverse operation: after an
				// ownership violation is detected, it restores the pre-oracle
				// balance without journaling the leaked value as a pre-image.
				if function.Name.Name == "restoreUnjournaledCanonicalCommutativePostImages" {
					continue
				}
				cached := map[string]struct{}{}
				ast.Inspect(function.Body, func(node ast.Node) bool {
					switch statement := node.(type) {
					case *ast.AssignStmt:
						if len(statement.Lhs) != len(statement.Rhs) {
							return true
						}
						for index, rhs := range statement.Rhs {
							call, isCall := rhs.(*ast.CallExpr)
							identifier, isIdentifier := statement.Lhs[index].(*ast.Ident)
							if isCall && isIdentifier && cachedAccountSource(call) {
								cached[identifier.Name] = struct{}{}
							}
						}
					case *ast.ValueSpec:
						if len(statement.Names) != len(statement.Values) {
							return true
						}
						for index, rhs := range statement.Values {
							call, isCall := rhs.(*ast.CallExpr)
							if isCall && cachedAccountSource(call) {
								cached[statement.Names[index].Name] = struct{}{}
							}
						}
					}
					return true
				})
				ast.Inspect(function.Body, func(node ast.Node) bool {
					call, isCall := node.(*ast.CallExpr)
					if isCall {
						selector, isSelector := call.Fun.(*ast.SelectorExpr)
						if isSelector && cachedAccountMutator(selector.Sel.Name) {
							switch receiver := selector.X.(type) {
							case *ast.Ident:
								if _, live := cached[receiver.Name]; live {
									t.Errorf("%s:%d %s mutates cached account %s via %s", path, fileSet.Position(call.Pos()).Line, function.Name.Name, receiver.Name, selector.Sel.Name)
								}
							case *ast.CallExpr:
								if cachedAccountSource(receiver) {
									t.Errorf("%s:%d %s directly mutates cached account via %s", path, fileSet.Position(call.Pos()).Line, function.Name.Name, selector.Sel.Name)
								}
							}
						}
					}
					assignment, isAssignment := node.(*ast.AssignStmt)
					if !isAssignment {
						return true
					}
					for _, lhs := range assignment.Lhs {
						field, isField := lhs.(*ast.SelectorExpr)
						if !isField {
							continue
						}
						protoCall, isProtoCall := field.X.(*ast.CallExpr)
						if !isProtoCall {
							continue
						}
						protoSelector, isProtoSelector := protoCall.Fun.(*ast.SelectorExpr)
						if !isProtoSelector {
							continue
						}
						receiver, isReceiver := protoSelector.X.(*ast.Ident)
						if isReceiver && protoSelector.Sel.Name == "Proto" {
							if _, live := cached[receiver.Name]; live {
								t.Errorf("%s:%d %s assigns through cached account %s.Proto().%s", path, fileSet.Position(lhs.Pos()).Line, function.Name.Name, receiver.Name, field.Sel.Name)
							}
						}
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
