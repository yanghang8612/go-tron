#!/usr/bin/env bash
#
# Repeatable harness for the opt-in ProcessBlock state-prefetch benchmarks.
#
# This is intentionally a measurement harness, not a pass/fail CI test. It
# stores raw Go benchmark output plus environment metadata so samples from
# different commits/machines can be compared later.
set -euo pipefail

BASEDIR="$(cd "$(dirname "$0")/../.." && pwd)"
PACKAGE="./core"
BENCH='BenchmarkProcessBlock_HeavyTRX_(HeavyState|ColdState)'
RUN='^$'
BENCHTIME="10x"
COUNT=5
TIMEOUT="30m"
BENCHMEM=1
OUTDIR=""
RUN_BENCHSTAT=1

usage() {
  cat <<'EOF'
Usage: scripts/dev/state_prefetch_benchmark.sh [options]

Options:
  --package PKG       Go package to benchmark (default: ./core)
  --bench REGEX       Benchmark regex (default: ProcessBlock HeavyTRX prefetch benches)
  --run REGEX         Test run regex passed to go test (default: ^$)
  --benchtime VALUE   Go benchmark benchtime (default: 10x)
  --count N           Go benchmark count (default: 5)
  --timeout VALUE     Go test timeout (default: 30m)
  --outdir DIR        Output directory (default: build/state-prefetch-bench/<utc>)
  --no-benchmem       Do not pass -benchmem
  --no-benchstat      Skip benchstat even when installed
  --short             Shortcut for --benchtime 1x --count 1
  -h, --help          Show this help

Examples:
  scripts/dev/state_prefetch_benchmark.sh
  scripts/dev/state_prefetch_benchmark.sh --short --outdir /tmp/prefetch-smoke
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --package) PACKAGE="${2:?}"; shift 2 ;;
    --bench) BENCH="${2:?}"; shift 2 ;;
    --run) RUN="${2:?}"; shift 2 ;;
    --benchtime) BENCHTIME="${2:?}"; shift 2 ;;
    --count) COUNT="${2:?}"; shift 2 ;;
    --timeout) TIMEOUT="${2:?}"; shift 2 ;;
    --outdir) OUTDIR="${2:?}"; shift 2 ;;
    --no-benchmem) BENCHMEM=0; shift ;;
    --no-benchstat) RUN_BENCHSTAT=0; shift ;;
    --short) BENCHTIME="1x"; COUNT=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

case "$COUNT" in
  ''|*[!0-9]*) die "--count must be a non-negative integer" ;;
esac
if [ "$COUNT" -lt 1 ]; then
  die "--count must be >= 1"
fi

if [ -z "$OUTDIR" ]; then
  OUTDIR="$BASEDIR/build/state-prefetch-bench/$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "$OUTDIR"

RAW="$OUTDIR/benchmark.txt"
META="$OUTDIR/metadata.txt"
BENCHSTAT_OUT="$OUTDIR/benchstat.txt"

cd "$BASEDIR"

GO_TEST=(go test "$PACKAGE" -run "$RUN" -bench "$BENCH" -benchtime "$BENCHTIME" -count "$COUNT" -timeout "$TIMEOUT")
if [ "$BENCHMEM" -eq 1 ]; then
  GO_TEST+=(-benchmem)
fi

{
  echo "time_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "repo=$BASEDIR"
  echo "branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  echo "head=$(git rev-parse --short HEAD 2>/dev/null || true)"
  if git diff --quiet --ignore-submodules -- 2>/dev/null && git diff --cached --quiet --ignore-submodules -- 2>/dev/null; then
    echo "worktree=clean"
  else
    echo "worktree=dirty"
  fi
  echo "go=$(go version)"
  echo "uname=$(uname -a)"
  printf 'command='
  printf '%q ' "${GO_TEST[@]}"
  echo
} > "$META"

echo "metadata: $META"
echo "benchmark output: $RAW"
"${GO_TEST[@]}" | tee "$RAW"

if [ "$RUN_BENCHSTAT" -eq 1 ]; then
  if command -v benchstat >/dev/null 2>&1; then
    benchstat "$RAW" > "$BENCHSTAT_OUT" || true
    echo "benchstat output: $BENCHSTAT_OUT"
  else
    echo "benchstat not found; raw benchmark output only" >&2
  fi
fi

echo "state prefetch benchmark complete: $OUTDIR"
