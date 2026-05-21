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
	"strconv"
	"strings"
	"sync"

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
	if lvl, ok := moduleLevel(m.name); ok {
		return level >= lvl
	}
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
	return SetupWithModules(verbosity, format, file, nil)
}

// SetupWithModules configures the global logger with optional per-module
// levels. Each module entry is "module=level", where level is trace, debug,
// info, warn, error, crit, or the legacy 0-5 verbosity number.
//
// Example:
//
//	SetupWithModules(3, "terminal", "", []string{"net/sync=debug", "p2p=warn"})
func SetupWithModules(verbosity int, format, file string, modules []string) error {
	if verbosity < 0 || verbosity > 5 {
		return fmt.Errorf("verbosity %d out of range 0-5", verbosity)
	}
	level := gethlog.FromLegacyLevel(verbosity)
	moduleLevels, err := ParseModuleLevels(modules)
	if err != nil {
		return err
	}
	handlerLevel := lowestLevel(level, moduleLevels)

	var primary slog.Handler
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "terminal":
		primary = gethlog.NewTerminalHandlerWithLevel(os.Stderr, handlerLevel, useColor(os.Stderr))
	case "json":
		primary = gethlog.JSONHandlerWithLevel(os.Stderr, handlerLevel)
	case "logfmt":
		primary = gethlog.LogfmtHandlerWithLevel(os.Stderr, handlerLevel)
	default:
		return fmt.Errorf("unknown log format %q (want terminal|json|logfmt)", format)
	}

	handler := primary
	if file != "" {
		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("open log file: %w", err)
		}
		handler = teeHandler{primary: primary, secondary: gethlog.JSONHandlerWithLevel(f, handlerLevel)}
	}

	setLevels(level, moduleLevels)
	handler = moduleLevelHandler{next: handler, global: level, modules: moduleLevels}
	gethlog.SetDefault(gethlog.NewLogger(handler))
	return nil
}

// ParseModuleLevels parses module-specific level overrides. Entries may be
// supplied either as repeated values or as comma-separated lists.
func ParseModuleLevels(specs []string) (map[string]slog.Level, error) {
	levels := make(map[string]slog.Level)
	for _, spec := range specs {
		for _, part := range strings.Split(spec, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			key, val, ok := strings.Cut(part, "=")
			if !ok {
				return nil, fmt.Errorf("invalid log module override %q (want module=level)", part)
			}
			module := strings.TrimSpace(key)
			if module == "" {
				return nil, fmt.Errorf("invalid log module override %q (empty module)", part)
			}
			level, err := ParseLevel(strings.TrimSpace(val))
			if err != nil {
				return nil, fmt.Errorf("invalid log module override %q: %w", part, err)
			}
			levels[module] = level
		}
	}
	return levels, nil
}

// ParseLevel parses either a named slog level or geth's legacy 0-5 verbosity.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace":
		return gethlog.LevelTrace, nil
	case "debug":
		return gethlog.LevelDebug, nil
	case "info":
		return gethlog.LevelInfo, nil
	case "warn", "warning":
		return gethlog.LevelWarn, nil
	case "error":
		return gethlog.LevelError, nil
	case "crit", "critical":
		return gethlog.LevelCrit, nil
	}
	v, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("unknown level %q", s)
	}
	if v < 0 || v > 5 {
		return 0, fmt.Errorf("verbosity %d out of range 0-5", v)
	}
	return gethlog.FromLegacyLevel(v), nil
}

func lowestLevel(global slog.Level, modules map[string]slog.Level) slog.Level {
	lowest := global
	for _, level := range modules {
		if level < lowest {
			lowest = level
		}
	}
	return lowest
}

var moduleLevelsState = struct {
	sync.RWMutex
	levels map[string]slog.Level
}{}

func setLevels(_ slog.Level, levels map[string]slog.Level) {
	moduleLevelsState.Lock()
	defer moduleLevelsState.Unlock()
	if len(levels) == 0 {
		moduleLevelsState.levels = nil
		return
	}
	cp := make(map[string]slog.Level, len(levels))
	for module, level := range levels {
		cp[module] = level
	}
	moduleLevelsState.levels = cp
}

func moduleLevel(module string) (slog.Level, bool) {
	moduleLevelsState.RLock()
	defer moduleLevelsState.RUnlock()
	if len(moduleLevelsState.levels) == 0 {
		return 0, false
	}
	return matchModuleLevel(moduleLevelsState.levels, module)
}

func matchModuleLevel(levels map[string]slog.Level, module string) (slog.Level, bool) {
	level, ok := levels[module]
	bestLen := 0
	if ok {
		bestLen = len(module)
	}
	for prefix, candidate := range levels {
		if len(prefix) <= bestLen {
			continue
		}
		if strings.HasPrefix(module, prefix+"/") {
			level = candidate
			ok = true
			bestLen = len(prefix)
		}
	}
	return level, ok
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

type moduleLevelHandler struct {
	next    slog.Handler
	global  slog.Level
	modules map[string]slog.Level
	attrs   []slog.Attr
}

func (h moduleLevelHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return lvl >= lowestLevel(h.global, h.modules) && h.next.Enabled(ctx, lvl)
}

func (h moduleLevelHandler) Handle(ctx context.Context, r slog.Record) error {
	module := h.moduleFromAttrs()
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "module" {
			module = a.Value.String()
			return false
		}
		return true
	})
	level := h.global
	if module != "" {
		if override, ok := matchModuleLevel(h.modules, module); ok {
			level = override
		}
	}
	if r.Level < level {
		return nil
	}
	return h.next.Handle(ctx, r)
}

func (h moduleLevelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	nextAttrs := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	nextAttrs = append(nextAttrs, h.attrs...)
	nextAttrs = append(nextAttrs, attrs...)
	return moduleLevelHandler{
		next:    h.next.WithAttrs(attrs),
		global:  h.global,
		modules: h.modules,
		attrs:   nextAttrs,
	}
}

func (h moduleLevelHandler) WithGroup(name string) slog.Handler {
	return moduleLevelHandler{
		next:    h.next.WithGroup(name),
		global:  h.global,
		modules: h.modules,
		attrs:   h.attrs,
	}
}

func (h moduleLevelHandler) moduleFromAttrs() string {
	for i := len(h.attrs) - 1; i >= 0; i-- {
		if h.attrs[i].Key == "module" {
			return h.attrs[i].Value.String()
		}
	}
	return ""
}
