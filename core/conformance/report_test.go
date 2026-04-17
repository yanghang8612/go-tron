package conformance

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReport_StringPass(t *testing.T) {
	r := &Report{
		RangeName:    "smoke",
		Passed:       true,
		BlockResults: []BlockResult{{BlockNum: 100, Passed: true}, {BlockNum: 101, Passed: true}},
	}
	s := r.String()
	if !strings.Contains(s, "status          : PASS") {
		t.Fatalf("missing PASS:\n%s", s)
	}
	if !strings.Contains(s, "blocks replayed : 2") {
		t.Fatalf("missing block count:\n%s", s)
	}
	if strings.Contains(s, "first failure") {
		t.Fatalf("pass report must not show first failure:\n%s", s)
	}
}

func TestReport_StringFail(t *testing.T) {
	div := &Divergence{
		BlockNum: 101,
		FieldDiffs: []FieldDiff{
			{Field: "dp:energy_fee", Got: "100", Want: "200"},
		},
	}
	r := &Report{
		RangeName: "smoke",
		Passed:    false,
		BlockResults: []BlockResult{
			{BlockNum: 100, Passed: true},
			{BlockNum: 101, Passed: false, Divergence: div},
		},
		FirstFailure: div,
	}
	s := r.String()
	if !strings.Contains(s, "status          : FAIL") {
		t.Fatalf("expected FAIL:\n%s", s)
	}
	if !strings.Contains(s, "first failure at block 101") {
		t.Fatalf("missing failure header:\n%s", s)
	}
	if !strings.Contains(s, "dp:energy_fee") {
		t.Fatalf("missing field diff:\n%s", s)
	}
}

func TestReport_StringStale(t *testing.T) {
	r := &Report{
		RangeName: "smoke",
		Passed:    true,
		StaleAllowlistEntries: []AllowlistEntry{
			{BlockNum: 42, Field: "dp:x", Reason: "fixed?"},
		},
	}
	s := r.String()
	if !strings.Contains(s, "stale allowlist : 1") {
		t.Fatalf("missing stale count:\n%s", s)
	}
	if !strings.Contains(s, "block=42 field=dp:x") {
		t.Fatalf("missing stale detail:\n%s", s)
	}
}

func TestReport_JSONSerializable(t *testing.T) {
	div := &Divergence{
		BlockNum:   7,
		FieldDiffs: []FieldDiff{{Field: "f", Got: "g", Want: "w"}},
		GotJSON:    json.RawMessage(`{"a":1}`),
	}
	r := &Report{
		RangeName:    "x",
		Passed:       false,
		BlockResults: []BlockResult{{BlockNum: 7, Passed: false, Divergence: div}},
		FirstFailure: div,
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var back Report
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back.RangeName != "x" || back.FirstFailure == nil || back.FirstFailure.BlockNum != 7 {
		t.Fatalf("roundtrip dropped data: %+v", back)
	}
}
