package main

import (
	"flag"
	"os"
	"testing"

	"github.com/urfave/cli/v2"
)

func TestRuntimeHistoryCompressionPrecedence(t *testing.T) {
	for _, tc := range []struct {
		env, explicit, want string
		invalid             bool
	}{
		{"", "", "auto", false}, {"2", "", "2", false}, {"2", "auto", "auto", false},
		{"auto", "3", "3", false}, {"", "1", "1", false}, {"", "bad", "", true},
	} {
		t.Run(tc.env+"/"+tc.explicit, func(t *testing.T) {
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", tc.env)
			set := flag.NewFlagSet("history-compression", flag.ContinueOnError)
			// Flag instances keep IsSet/env state; use a fresh value per case.
			f := *historyCompressionFormatFlag
			if err := f.Apply(set); err != nil {
				t.Fatal(err)
			}
			if tc.explicit != "" {
				if err := set.Parse([]string{"--" + f.Name, tc.explicit}); err != nil {
					t.Fatal(err)
				}
			}
			got, err := applyRuntimeHistoryCompression(cli.NewContext(cli.NewApp(), set, nil))
			if tc.invalid {
				if err == nil || os.Getenv("GTRON_HISTORY_COMPRESSION_FORMAT") != tc.env {
					t.Fatalf("invalid mode mutated environment: %q %v", got, err)
				}
				return
			}
			if err != nil || got != tc.want || os.Getenv("GTRON_HISTORY_COMPRESSION_FORMAT") != tc.want {
				t.Fatalf("mode %q want %q: %v", got, tc.want, err)
			}
		})
	}
}
