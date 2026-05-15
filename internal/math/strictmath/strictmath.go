// Package strictmath provides a Go entry point for java-tron's
// `StrictMath` semantics, gated by the `allow_strict_math` proposal (#87).
//
// Java's `StrictMath.pow` is specified to match fdlibm bit-for-bit across
// all JVMs; java-tron added the gate to escape HotSpot's `Math.pow` drift.
// gtron must match the same bit pattern for any chain replay that crosses
// the activation height.
//
// This package currently delegates to Go's `math.Pow`. **That delegation
// is NOT bit-identical to fdlibm** — Go's standard-library pow uses a
// different algorithm. See `RED-3` in docs/dev/fork-audit-2026-05-15.md.
//
// TODO(red-3-port): port fdlibm `e_pow.c` to Go and validate bit-for-bit
// against a Java oracle (`java.lang.StrictMath.pow`). Until that lands,
// callers gated on this package will silently fork from java-tron for
// inputs where Go's `math.Pow` and `StrictMath.pow` disagree.
package strictmath

import "math"

// Pow returns a^b. Will be replaced with an fdlibm port once validated.
func Pow(a, b float64) float64 {
	return math.Pow(a, b)
}
