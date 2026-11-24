package jsonrpc

import (
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestCallDataAliases(t *testing.T) {
	for _, tc := range []struct {
		args, data, errorField string
	}{
		{`{"input":"0x1234","data":"abcd"}`, "1234", ""},
		{`{"input":null,"data":"1"}`, "01", ""},
		{`{"input":"","data":"abcd"}`, "", ""},
		{`{"input":"0x1"}`, "", "input"},
		{`{"input":"1234"}`, "", "input"},
		{`{"input":"0x1234","data":"not hex"}`, "", "data"},
	} {
		t.Run(tc.args, func(t *testing.T) {
			var args callArgs
			if err := json.Unmarshal([]byte(tc.args), &args); err != nil {
				t.Fatal(err)
			}
			got, err := args.callData()
			if tc.errorField != "" {
				coded, ok := err.(*invalidParamsError)
				if !ok || coded.ErrorCode() != -32602 || coded.Error() != "invalid hex string for \""+tc.errorField+"\"" {
					t.Fatalf("error = %v; want invalid params for %s", err, tc.errorField)
				}
				return
			}
			if err != nil || hex.EncodeToString(got) != tc.data {
				t.Fatalf("data = %x, %v; want %s", got, err, tc.data)
			}
		})
	}
}
