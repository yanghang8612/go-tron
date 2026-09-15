#!/usr/bin/env python3
"""Build a fresh offline worker-count diagnostic using the pinned pipeline builder.

The pinned builder has no deployment or service operations. Only its explicit
source scope, fresh output directory and test/help contracts differ here.
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
BASE = 'e913c72d274d3f76f07b61f1d44ae1df986cbee9'
BUILDER_PATH = 'scripts/prepare_history_pipeline_20260915.py'
BUILDER_SHA = '9f5680ed1aaf5782617dc1113ca706aeb81b1cc86a1060e38f615d426ee48038'
SCRIPT_PATH = 'scripts/prepare_history_pipeline_workers_20260915.py'
RELEASE = Path('/data/gtron/releases/20260915-history-pipeline-workers')


def load_builder():
    data = subprocess.check_output(['git', 'show', BASE + ':' + BUILDER_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=60)
    if hashlib.sha256(data).hexdigest() != BUILDER_SHA:
        raise RuntimeError('reviewed native builder Git bytes differ')
    module = types.ModuleType('history_pipeline_native_builder')
    # The pinned file is solely imports, constants, definitions and a guarded
    # __main__. Loading it does not run prepare or any child command.
    module.__file__ = str(Path(__file__).resolve())
    exec(compile(data, BUILDER_PATH, 'exec'), module.__dict__)
    module = module.load_builder()
    module.REPO, module.BASE = REPO, BASE
    module.RELEASE, module.SOURCE = RELEASE, RELEASE / 'source'
    module.BINARY = RELEASE / 'gtron-inspect'
    module.SCRIPT_PATH = SCRIPT_PATH
    module.ALLOWED_SOURCE_CHANGES = {
        'core/rawdb/state_history_pipeline.go',
        'core/rawdb/state_history_pipeline_test.go',
        'core/rawdb/state_history_pipeline_cold_bench_test.go',
        'core/rawdb/state_history_pipeline_workers_test.go',
        'core/state/snapshots/history_diagnostic.go',
        'cmd/gtron/db_history_cold_benchmark.go',
        'cmd/gtron/db_history_cold_benchmark_test.go',
        'cmd/gtron/db_history_pipeline_benchmark_test.go',
        SCRIPT_PATH, 'scripts/tests/test_prepare_history_pipeline_workers_20260915.py',
        'scripts/benchmark_history_pipeline_workers_20260915.py',
        'scripts/tests/test_benchmark_history_pipeline_workers_20260915.py',
    }
    module.REQUIRED_SOURCE_CHANGES = {
        'core/rawdb/state_history_pipeline.go',
        'core/state/snapshots/history_diagnostic.go',
        'cmd/gtron/db_history_cold_benchmark.go',
    }
    module.HELP_CONTRACTS = dict(module.HELP_CONTRACTS)
    module.HELP_CONTRACTS['benchmark-history-cold'] += ('--shared-read-workers',)
    module.REQUIRED_TESTS += (
        ('core/rawdb', 'TestStateHistoryPipelineWorkersFrozenOracle'),
        ('core/rawdb', 'TestStateHistoryPipelineWorkersBoundedQueueAndCancelJoin'),
        ('core/rawdb', 'TestStateHistoryPipelineWorkersFutureErrorOrder'),
        ('core/rawdb', 'TestStateHistoryPipelineWorkersBudgetAndValidation'),
        ('cmd/gtron', 'TestDBHistorySharedPipelineWorkersCLIAndBounds'),
    )
    # Package tests include the focused rawdb pipeline suites, full snapshots
    # and cmd/gtron, the unchanged serial oracle, and native Sapling evidence.
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--script-revision', required=True)
    parser.add_argument('--rust-library-sha256', required=True)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise RuntimeError('explicit root execution required for the fresh diagnostic release')
    builder = load_builder()
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
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
