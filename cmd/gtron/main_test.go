package main

import (
	"io"
	"testing"
)

func TestAppCommandTreeSetup(t *testing.T) {
	testApp := *app
	testApp.Writer = io.Discard
	testApp.ErrWriter = io.Discard
	if err := testApp.Run([]string{"gtron", "--version"}); err != nil {
		t.Fatalf("run gtron --version: %v", err)
	}
}
