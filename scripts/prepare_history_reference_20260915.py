#!/usr/bin/env python3
"""Build/test the reference reader and private builder from exact native Git bytes.

No service operation or database migration. The existing native build harness
pins the source archive, checkout, Sapling library, compiler and test results.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import types

REPO = Path('/data/gtron/go-tron')
BASE = '61708c9de685efa2be727167f8d2b3e3970a0099'
BUILDER_PATH = 'scripts/prepare_history_parallel_20260915.py'
BUILDER_SHA = '43e1c06ea0065f6cf42a5b7cb60c34d17cba99eddf019e4dd51e2c2fcfe02ef1'
SCRIPT_PATH = 'scripts/prepare_history_reference_20260915.py'
RELEASE = Path('/data/gtron/releases/20260915-history-reference')


def load_builder():
    data = subprocess.check_output(['git', 'show', BASE + ':' + BUILDER_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=60)
    if hashlib.sha256(data).hexdigest() != BUILDER_SHA:
        raise RuntimeError('native builder Git bytes differ')
    module = types.ModuleType('history_reference_native_builder')
    module.__file__ = str(Path(__file__).resolve())
    exec(compile(data, BUILDER_PATH, 'exec'), module.__dict__)
    module.REPO, module.BASE = REPO, BASE
    module.RELEASE, module.SOURCE = RELEASE, RELEASE / 'source'
    module.BINARY = RELEASE / 'gtron-inspect'
    module.SCRIPT_PATH = SCRIPT_PATH
    module.ALLOWED_SOURCE_CHANGES = {
        'cmd/gtron/db_history_cold_benchmark.go',
        'cmd/gtron/db_history_reference_benchmark_test.go',
        'core/rawdb/state_history_span.go',
        'core/rawdb/state_history_span_test.go',
        'core/state/snapshots/compactor_budget.go',
        'core/state/snapshots/history_binary.go',
        'core/state/snapshots/history_business_sample.go',
        'core/state/snapshots/history_diagnostic.go',
        'core/state/snapshots/history_migrate_v7.go',
        'core/state/snapshots/history_space_inspect.go',
        'core/state/snapshots/history_reference_build.go',
        'core/state/snapshots/history_reference_build_test.go',
        'core/state/snapshots/history_reference_container.go',
        'core/state/snapshots/history_reference_container_reader.go',
        'core/state/snapshots/history_reference_container_writer.go',
        'core/state/snapshots/history_reference_container_test.go',
        SCRIPT_PATH,
    }
    module.REQUIRED_SOURCE_CHANGES = module.ALLOWED_SOURCE_CHANGES - {SCRIPT_PATH}
    module.TEST_PATTERN += '|Test.*(HistoryReference|StateHistorySpan)'
    module.REQUIRED_TESTS += (
        ('core/rawdb', 'TestStateHistorySpanColdOrderOracle'),
        ('core/rawdb', 'TestStateHistorySpanCorruptionBeforeAnyBlockCallback'),
        ('core/state/snapshots', 'TestHistoryReferenceBuildIntegrationOracle'),
        ('core/state/snapshots', 'TestHistoryReferenceContainerRandomAccessAndOwnedOutput'),
        ('core/state/snapshots', 'TestHistoryReferenceContainerCorruptionAndTruncation'),
        ('cmd/gtron', 'TestDBHistoryReferenceBenchmarkPrivateComplete'),
        ('cmd/gtron', 'TestDBHistoryReferenceBenchmarkCLIAndInvalidTopology'),
    )
    module.HELP_CONTRACTS = dict(module.HELP_CONTRACTS)
    module.HELP_CONTRACTS['benchmark-history-cold'] += ('--reference-container', '--shared-chunk-cache')
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--script-revision', required=True)
    parser.add_argument('--rust-library-sha256', required=True)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise RuntimeError('explicit root execution required for private native build')
    builder = load_builder()
    for sig in builder.SIGNALS:
        signal.signal(sig, builder.interrupted)
    print(json.dumps(builder.prepare(args), sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(json.dumps({'prepared': False, 'error': repr(error)}), file=sys.stderr, flush=True)
        sys.exit(1)
