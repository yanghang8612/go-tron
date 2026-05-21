package log

import (
	"bytes"
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

func TestParseModuleLevels(t *testing.T) {
	levels, err := ParseModuleLevels([]string{"net/sync=debug,p2p=warn", "core/chain=5"})
	if err != nil {
		t.Fatalf("ParseModuleLevels: %v", err)
	}
	if got := levels["net/sync"]; got != LevelDebug {
		t.Fatalf("net/sync level=%v, want debug", got)
	}
	if got := levels["p2p"]; got != LevelWarn {
		t.Fatalf("p2p level=%v, want warn", got)
	}
	if got := levels["core/chain"]; got != LevelTrace {
		t.Fatalf("core/chain level=%v, want trace", got)
	}
}

func TestSetupWithModules_ModuleOverride(t *testing.T) {
	var buf bytes.Buffer
	prev := Root()
	defer SetDefault(prev)
	defer setLevels(LevelInfo, nil)

	// Reinstall the configured handler with a capture sink so the test can
	// assert filtering without depending on stderr.
	moduleLevels, err := ParseModuleLevels([]string{"log=error", "log/test=debug", "log/quiet=warn"})
	if err != nil {
		t.Fatal(err)
	}
	setLevels(LevelInfo, moduleLevels)
	h := moduleLevelHandler{
		next:    LogfmtHandlerWithLevel(&buf, LevelDebug),
		global:  LevelInfo,
		modules: moduleLevels,
	}
	SetDefault(NewLogger(h))

	NewModule("log/test").Debug("debug visible")
	NewModule("log/other").Debug("debug hidden")
	NewModule("log/other").Warn("prefix warn hidden")
	NewModule("log/quiet").Info("info hidden")
	NewModule("log/quiet").Warn("warn visible")

	out := buf.String()
	for _, want := range []string{"debug visible", "warn visible"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in log output:\n%s", want, out)
		}
	}
	for _, reject := range []string{"debug hidden", "prefix warn hidden", "info hidden"} {
		if strings.Contains(out, reject) {
			t.Fatalf("unexpected %q in log output:\n%s", reject, out)
		}
	}
}
