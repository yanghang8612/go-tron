#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' \
    'Usage: scripts/dev/archive_cold_prune_verify.sh [--race] [--benchmark [storage_benchmark args...]]' \
    '' \
    'Runs the archive cold-coverage/hot-prune safety suite. With --benchmark,' \
    'runs scripts/dev/storage_benchmark.sh after tests and forwards the rest of' \
    'the arguments verbatim.'
}

race_flag=()
run_benchmark=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --race)
      race_flag=(-race)
      shift
      ;;
    --benchmark)
      run_benchmark=1
      shift
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
repo_dir="$(CDPATH= cd -- "$script_dir/../.." && pwd)"
cd "$repo_dir"

go test "${race_flag[@]}" ./core/state/pruning ./core/state/snapshots \
  -run 'TestWorkerArchive|TestArchiveLifecycleBuildsColdHistoryBeforePruningDuplicateHotRows|TestColdBuilderDefersHistory' \
  -count=1

go test ./core \
  -run 'TestArchiveQuery_(UsesColdStateDomainChangeSnapshots|CodeAndStorageUseColdStateDomainChangeSnapshots|ContractRecreateStorageGenerationUsesColdStateDomainChangeSnapshots)' \
  -count=1

go test ./cmd/gtron ./params \
  -run 'Test(ShouldEnableDomainStatePruner|DomainStatePrunePolicy|EnsureHistoryPruneMode|DBStorageAlertsCmdJSONReportsArchiveModePruneConflict|HistoryPruneWindow)' \
  -count=1

if [ "$run_benchmark" -eq 1 ]; then
  exec "$script_dir/storage_benchmark.sh" "$@"
fi
