package main

import (
	"github.com/urfave/cli/v2"
	"io"
	"strings"
	"testing"
)

func TestRemovedCommitmentCacheFlagsRejected(t *testing.T) {
	for _, name := range []string{"state.commitment.cache", "state.commitment.cache.tail"} {
		t.Run(name, func(t *testing.T) {
			for _, flag := range app.Flags {
				for _, n := range flag.Names() {
					if n == name {
						t.Fatalf("obsolete flag %s still registered", name)
					}
				}
			}
			called := false
			a := &cli.App{Writer: io.Discard, ErrWriter: io.Discard, Flags: app.Flags, Action: func(*cli.Context) error { called = true; return nil }}
			err := a.Run([]string{"gtron", "--" + name, "1"})
			if err == nil || called || !strings.Contains(err.Error(), "flag provided but not defined") {
				t.Fatalf("obsolete branch cache flag accepted: %v", err)
			}
		})
	}
}
