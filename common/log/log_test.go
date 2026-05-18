package log

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetup_VerbosityRange(t *testing.T) {
	for _, v := range []int{-1, 6, 99} {
		if err := Setup(v, "terminal", ""); err == nil {
			t.Errorf("expected error for verbosity=%d, got nil", v)
		}
	}
	for _, v := range []int{0, 1, 2, 3, 4, 5} {
		if err := Setup(v, "terminal", ""); err != nil {
			t.Errorf("unexpected error for verbosity=%d: %v", v, err)
		}
	}
}

func TestSetup_UnknownFormat(t *testing.T) {
	if err := Setup(3, "xml", ""); err == nil {
		t.Error("expected error for unknown format, got nil")
	}
}

func TestSetup_FileSinkWritesJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.log")

	if err := Setup(3, "terminal", path); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	logger := New("module", "log/test")
	logger.Info("hello", "k", "v")

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !strings.Contains(string(data), `"msg":"hello"`) {
		t.Errorf("file does not contain JSON message; got: %s", data)
	}
	if !strings.Contains(string(data), `"module":"log/test"`) {
		t.Errorf("file does not contain module tag; got: %s", data)
	}
}
