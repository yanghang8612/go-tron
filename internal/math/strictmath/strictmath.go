// Package strictmath provides a Go entry point for java-tron's
// `StrictMath` semantics, gated by the `allow_strict_math` proposal (#87).
//
// Java's `StrictMath.pow` is specified to match fdlibm bit-for-bit across
// all JVMs; java-tron added the gate to escape HotSpot's `Math.pow` drift.
// gtron must match the same bit pattern for any chain replay that crosses
// the activation height.
//
// `Pow` is implemented in `pow.go` as a direct port of OpenJDK's
// `java.lang.FdLibm.Pow.compute` (which is itself a translation of the
// fdlibm `e_pow.c` reference). It is validated bit-for-bit against a
// Java `StrictMath.pow` oracle in `pow_test.go`.
package strictmath
