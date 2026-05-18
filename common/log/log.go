// Package log is gtron's structured-logging facade over go-ethereum's log
// package. Every gtron package imports this one (never go-ethereum/log
// directly) so that the backend can be swapped in one place.
//
// Typical use — package-level Module so each line carries module=<pkg>:
//
//	import gtronlog "github.com/tronprotocol/go-tron/common/log"
//	var log = gtronlog.NewModule("net/sync")
//	log.Info("Sync started", "peer", id, "head", n)
//
// One-shot CLI utilities can use the top-level functions directly:
//
//	import "github.com/tronprotocol/go-tron/common/log"
//	log.Info("Starting", "version", v)
package log

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	gethlog "github.com/ethereum/go-ethereum/log"
)

// Logger is the gtron logger interface, identical to geth's. Useful for
// callers that want to hold a Logger value (typically tests).
type Logger = gethlog.Logger

// Module is the package-scoped logger handle. Its level methods (Info, Debug,
// etc.) resolve the current global root each call and append a "module=<name>"
// attribute, so calling log.Setup at process start propagates to every
// package-level Module without needing to recreate them.
//
// Usage:
//
//	var log = gtronlog.New("module", "net/sync") // INCORRECT: captures root at init
//	var log = gtronlog.NewModule("net/sync")     // CORRECT: resolves lazily
type Module struct{ name string }

// NewModule returns a Module logger tagged with module=<name>.
func NewModule(name string) Module { return Module{name: name} }

func (m Module) with(ctx []any) []any {
	out := make([]any, 0, len(ctx)+2)
	out = append(out, "module", m.name)
	out = append(out, ctx...)
	return out
}

// enabled is a fast pre-check that lets each level method short-circuit
// before allocating the module-prepended slice. Geth's *logger.Write also
// runs an Enabled gate, but only AFTER the args slice has been built;
// inlining the check here keeps per-block Trace cost at one func call +
// one comparison on production verbosity.
func (m Module) enabled(level slog.Level) bool {
	return gethlog.Root().Enabled(context.Background(), level)
}

// Trace logs at Trace level. The module tag is automatically prepended.
func (m Module) Trace(msg string, ctx ...interface{}) {
	if !m.enabled(gethlog.LevelTrace) {
		return
	}
	gethlog.Trace(msg, m.with(ctx)...)
}

// Debug logs at Debug level.
func (m Module) Debug(msg string, ctx ...interface{}) {
	if !m.enabled(gethlog.LevelDebug) {
		return
	}
	gethlog.Debug(msg, m.with(ctx)...)
}

// Info logs at Info level.
func (m Module) Info(msg string, ctx ...interface{}) {
	if !m.enabled(gethlog.LevelInfo) {
		return
	}
	gethlog.Info(msg, m.with(ctx)...)
}

// Warn logs at Warn level.
func (m Module) Warn(msg string, ctx ...interface{}) {
	if !m.enabled(gethlog.LevelWarn) {
		return
	}
	gethlog.Warn(msg, m.with(ctx)...)
}

// Error logs at Error level.
func (m Module) Error(msg string, ctx ...interface{}) {
	if !m.enabled(gethlog.LevelError) {
		return
	}
	gethlog.Error(msg, m.with(ctx)...)
}

// Crit logs at Crit level. gethlog.Crit also calls os.Exit(1).
func (m Module) Crit(msg string, ctx ...interface{}) {
	gethlog.Crit(msg, m.with(ctx)...)
}

// Re-exports.
var (
	New        = gethlog.New
	Root       = gethlog.Root
	SetDefault = gethlog.SetDefault
	NewLogger  = gethlog.NewLogger

	Trace = gethlog.Trace
	Debug = gethlog.Debug
	Info  = gethlog.Info
	Warn  = gethlog.Warn
	Error = gethlog.Error
	Crit  = gethlog.Crit

	// Handler factories — re-exported primarily for tests that want to
	// capture log records into a buffer.
	NewTerminalHandlerWithLevel = gethlog.NewTerminalHandlerWithLevel
	JSONHandlerWithLevel        = gethlog.JSONHandlerWithLevel
	LogfmtHandlerWithLevel      = gethlog.LogfmtHandlerWithLevel
	DiscardHandler              = gethlog.DiscardHandler
)

// Level constants — re-exported under shorter names. Tests can also use the
// slog.Level constants directly.
const (
	LevelTrace = gethlog.LevelTrace
	LevelDebug = gethlog.LevelDebug
	LevelInfo  = gethlog.LevelInfo
	LevelWarn  = gethlog.LevelWarn
	LevelError = gethlog.LevelError
	LevelCrit  = gethlog.LevelCrit
)

// SetupCLI installs a sensible default logger for standalone CLI utilities
// that don't expose a --verbosity flag: terminal output on stderr at Info
// level. Must be called from main() before any log emission, otherwise the
// geth default (DiscardHandler) drops every record — including Crit, whose
// os.Exit(1) would then leave the user with no diagnostic.
//
// Tests should NOT call this. They install their own handler via SetDefault
// when they need to capture records.
func SetupCLI() {
	// Cannot fail with these arguments — Setup only errors on bad verbosity,
	// unknown format, or unopenable file.
	_ = Setup(3, "terminal", "")
}

// Setup configures the global root logger. Safe to call multiple times (each
// call replaces the previous handler).
//
//   - verbosity uses geth's legacy 0-5 scale: 0=Crit 1=Error 2=Warn 3=Info
//     4=Debug 5=Trace.
//   - format must be one of "terminal" (default), "json", "logfmt".
//   - file is optional; if non-empty, records are tee'd to that path in JSON
//     regardless of stderr format.
func Setup(verbosity int, format, file string) error {
	if verbosity < 0 || verbosity > 5 {
		return fmt.Errorf("verbosity %d out of range 0-5", verbosity)
	}
	level := gethlog.FromLegacyLevel(verbosity)

	var primary slog.Handler
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "terminal":
		primary = gethlog.NewTerminalHandlerWithLevel(os.Stderr, level, useColor(os.Stderr))
	case "json":
		primary = gethlog.JSONHandlerWithLevel(os.Stderr, level)
	case "logfmt":
		primary = gethlog.LogfmtHandlerWithLevel(os.Stderr, level)
	default:
		return fmt.Errorf("unknown log format %q (want terminal|json|logfmt)", format)
	}

	handler := primary
	if file != "" {
		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("open log file: %w", err)
		}
		handler = teeHandler{primary: primary, secondary: gethlog.JSONHandlerWithLevel(f, level)}
	}

	gethlog.SetDefault(gethlog.NewLogger(handler))
	return nil
}

func useColor(f *os.File) bool {
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// teeHandler fans each record out to two slog handlers. Both handlers see a
// cloned Record so neither can mutate state seen by the other.
type teeHandler struct {
	primary, secondary slog.Handler
}

func (t teeHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return t.primary.Enabled(ctx, lvl) || t.secondary.Enabled(ctx, lvl)
}

func (t teeHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	if t.primary.Enabled(ctx, r.Level) {
		if err := t.primary.Handle(ctx, r.Clone()); err != nil {
			errs = append(errs, err)
		}
	}
	if t.secondary.Enabled(ctx, r.Level) {
		if err := t.secondary.Handle(ctx, r.Clone()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (t teeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return teeHandler{
		primary:   t.primary.WithAttrs(attrs),
		secondary: t.secondary.WithAttrs(attrs),
	}
}

func (t teeHandler) WithGroup(name string) slog.Handler {
	return teeHandler{
		primary:   t.primary.WithGroup(name),
		secondary: t.secondary.WithGroup(name),
	}
}
