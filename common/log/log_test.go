package log

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var moduleTraceBenchmarkSink uint64

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
	t.Cleanup(func() { _ = Close() })
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
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat log file: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("log file permissions = %o, want 600", got)
	}
}

func TestSetupWithOptions_IndependentFileVerbosity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.log")
	if err := SetupWithOptions(SetupOptions{
		Verbosity:      2,
		Format:         "terminal",
		File:           path,
		FileVerbosity:  4,
		FileMaxSizeMB:  1,
		FileMaxBackups: 1,
		FileMaxAgeDays: 1,
	}); err != nil {
		t.Fatalf("SetupWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = Close() })

	NewModule("log/test").Debug("file-only-debug")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !strings.Contains(string(data), "file-only-debug") {
		t.Fatalf("debug record missing from file: %s", data)
	}
}

func TestSetupWithOptions_TerminalFileFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gtron.log")
	if err := SetupWithOptions(SetupOptions{
		Verbosity:      3,
		Format:         "terminal",
		File:           path,
		FileFormat:     "terminal",
		FileVerbosity:  -1,
		FileMaxSizeMB:  1,
		FileMaxBackups: 1,
		FileMaxAgeDays: 1,
	}); err != nil {
		t.Fatalf("SetupWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = Close() })

	NewModule("log/test").Info("terminal-file-message", "height", 42)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read terminal log file: %v", err)
	}
	if strings.HasPrefix(strings.TrimSpace(string(data)), "{") {
		t.Fatalf("terminal file unexpectedly contains JSON: %s", data)
	}
	for _, want := range []string{"INFO", "terminal-file-message", "height=42"} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("terminal file missing %q: %s", want, data)
		}
	}
}

func TestSetupWithOptions_RejectsUnknownFileFormat(t *testing.T) {
	err := SetupWithOptions(SetupOptions{
		Verbosity:      3,
		Format:         "terminal",
		File:           filepath.Join(t.TempDir(), "gtron.log"),
		FileFormat:     "xml",
		FileVerbosity:  -1,
		FileMaxSizeMB:  1,
		FileMaxBackups: 1,
		FileMaxAgeDays: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "file log format") {
		t.Fatalf("unknown file format error = %v", err)
	}
}

func TestRedactArgs(t *testing.T) {
	got := RedactArgs([]string{
		"gtron",
		"--witness.key=secret-one",
		"--snapshot.signing-key", "secret-two",
		"--datadir", "/data/gtron",
	}, "witness.key", "snapshot.signing-key")
	want := []string{
		"gtron",
		"--witness.key=<redacted>",
		"--snapshot.signing-key", "<redacted>",
		"--datadir", "/data/gtron",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("RedactArgs = %#v, want %#v", got, want)
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

func TestModuleTraceEnabledHonorsModuleLevel(t *testing.T) {
	defer setLevels(LevelInfo, nil)
	module := NewModule("log/trace-gate")
	setLevels(LevelInfo, map[string]slog.Level{"log/trace-gate": LevelInfo})
	if module.TraceEnabled() {
		t.Fatal("trace enabled at module info level")
	}
	setLevels(LevelInfo, map[string]slog.Level{"log/trace-gate": LevelTrace})
	if !module.TraceEnabled() {
		t.Fatal("trace disabled at module trace level")
	}
}

func TestModuleDebugEnabledHonorsModuleLevel(t *testing.T) {
	defer setLevels(LevelInfo, nil)
	module := NewModule("log/debug-gate")
	setLevels(LevelInfo, map[string]slog.Level{"log/debug-gate": LevelInfo})
	if module.DebugEnabled() {
		t.Fatal("debug enabled at module info level")
	}
	setLevels(LevelInfo, map[string]slog.Level{"log/debug-gate": LevelDebug})
	if !module.DebugEnabled() {
		t.Fatal("debug disabled at module debug level")
	}
}

func BenchmarkModuleDisabledTraceGuard(b *testing.B) {
	defer setLevels(LevelInfo, nil)
	module := NewModule("log/trace-benchmark")
	setLevels(LevelInfo, map[string]slog.Level{"log/trace-benchmark": LevelInfo})
	peerID := strings.Repeat("peer", 16)
	var head uint64
	b.ReportAllocs()
	for b.Loop() {
		head++
		if module.TraceEnabled() {
			module.Trace("waiting", "peer", peerID, "head", head, "tip", head+1)
		}
	}
	moduleTraceBenchmarkSink = head
}
