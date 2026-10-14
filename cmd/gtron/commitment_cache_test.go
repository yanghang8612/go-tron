package main

import (
	"io"
	"strconv"
	"testing"

	"github.com/urfave/cli/v2"
)

func TestCommitmentCacheTailCLIEnvironment(t *testing.T) {
	const env = "GTRON_STATE_COMMITMENT_CACHE_TAIL"
	for _, tc := range []struct {
		name, env          string
		args               []string
		wantBase, wantTail int
		wantErr            bool
	}{
		{name: "default", wantBase: 512},
		{name: "environment", env: "512", wantBase: 512, wantTail: 512},
		{name: "explicit-cli-overrides-environment", env: "512", args: []string{"--state.commitment.cache.tail", "1024"}, wantBase: 512, wantTail: 1024},
		{name: "explicit-cli-disables-environment", env: "512", args: []string{"--state.commitment.cache.tail", "0"}, wantBase: 512},
		{name: "invalid-environment", env: "invalid", wantErr: true},
		{name: "negative-environment", env: "-1", wantErr: true},
		{name: "negative-cli", args: []string{"--state.commitment.cache.tail", "-1"}, wantErr: true},
		{name: "base-disabled", env: "512", args: []string{"--state.commitment.cache", "0"}, wantErr: true},
		{name: "all-disabled", args: []string{"--state.commitment.cache", "0"}, wantBase: 0},
		{name: "overflow", env: strconv.FormatUint(uint64(^uint(0)>>1), 10), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(env, tc.env)
			// Use the flags actually registered on the node, copying their mutable
			// cli state so one case cannot affect a later case or the global app.
			var flags []cli.Flag
			for _, flag := range app.Flags {
				if flag.Names()[0] == stateCommitmentCacheFlag.Name || flag.Names()[0] == stateCommitmentCacheTailFlag.Name {
					copy := *flag.(*cli.IntFlag)
					flags = append(flags, &copy)
				}
			}
			if len(flags) != 2 {
				t.Fatal("commitment cache flags not registered")
			}
			var base, tail int
			a := &cli.App{Writer: io.Discard, ErrWriter: io.Discard, Flags: flags, Action: func(ctx *cli.Context) error {
				var err error
				base, tail, err = commitmentCacheSizeBytes(ctx.Int(stateCommitmentCacheFlag.Name), ctx.Int(stateCommitmentCacheTailFlag.Name))
				return err
			}}
			err := a.Run(append([]string{"gtron"}, tc.args...))
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && (base != tc.wantBase<<20 || tail != tc.wantTail<<20) {
				t.Fatalf("base/tail=%d/%d want=%d/%d MiB", base, tail, tc.wantBase, tc.wantTail)
			}
		})
	}
}
