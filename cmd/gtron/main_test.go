package main

import (
	"errors"
	"io"
	"strings"
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

func TestCloseRuntimeStoreReportsErrorsAndRecoversPanics(t *testing.T) {
	want := errors.New("close failed")
	if err := closeRuntimeStore("test", func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("close error = %v, want %v", err, want)
	}
	err := closeRuntimeStore("test", func() error { panic("boom") })
	if err == nil || !strings.Contains(err.Error(), "test close panic: boom") || !strings.Contains(err.Error(), "TestCloseRuntimeStoreReportsErrorsAndRecoversPanics") {
		t.Fatalf("panic error = %v, want named panic and stack", err)
	}
	if err := closeRuntimeStore("test", nil); err != nil {
		t.Fatalf("nil close = %v", err)
	}
}
