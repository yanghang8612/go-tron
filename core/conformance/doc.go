// Package conformance implements the M0" conformance-replay harness.
//
// Scope: replay a recorded range of java-tron mainnet blocks through
// go-tron's core.ProcessBlock, compare the post-state of each block
// against a java-tron-sourced oracle, and surface divergences at the
// granularity of individual account / DP fields.
//
// Pipeline boundaries:
//
//   - Capture (manual, operator) runs against a live java-tron and
//     produces the fixture files under test/fixtures/mainnet-blocks/
//     <range>/ — NOT in this package.
//   - Replay (CI, hermetic) is this package. It takes those fixture
//     files and runs a pure, in-memory reproduction of core.ProcessBlock.
//   - Cross e2e (manual) lives under scripts/ — also NOT here.
//
// Public entry point: ReplayRange(ctx, ReplayConfig) → *Report.
//
// Design: docs/superpowers/specs/2026-04-17-m0-double-prime-conformance-replay-design.md
// Plan:   docs/superpowers/plans/2026-04-17-m0-double-prime-conformance-replay.md
package conformance
