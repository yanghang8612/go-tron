#!/usr/bin/env python3
"""Build a fresh CDC digest-reuse candidate from exact Git source and native inputs.

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
BASE = '8ddc4d8a38eeb7d351bb9d4a4db03592a71ad1e8'
BUILDER_PATH = 'scripts/prepare_history_parallel_20260915.py'
BUILDER_SHA = '43e1c06ea0065f6cf42a5b7cb60c34d17cba99eddf019e4dd51e2c2fcfe02ef1'
SCRIPT_PATH = 'scripts/prepare_history_cdc_reuse_20260915.py'
RELEASE = Path('/data/gtron/releases/20260915-history-cdc-reuse')


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
    module.REPO, module.BASE = REPO, BASE
    module.RELEASE, module.SOURCE = RELEASE, RELEASE / 'source'
    module.BINARY = RELEASE / 'gtron-inspect'
    module.SCRIPT_PATH = SCRIPT_PATH
    module.ALLOWED_SOURCE_CHANGES = {
        'core/state/snapshots/compressed_block_cdc_writer.go',
        'core/state/snapshots/compressed_block_cdc_digest_reuse_test.go',
        'core/state/snapshots/compressed_block_cdc_digest_reuse_oracle_test.go',
        SCRIPT_PATH, 'scripts/tests/test_prepare_history_cdc_reuse_20260915.py',
        'scripts/benchmark_history_cdc_reuse_20260915.py',
        'scripts/tests/test_benchmark_history_cdc_reuse_20260915.py',
    }
    module.REQUIRED_SOURCE_CHANGES = {
        'core/state/snapshots/compressed_block_cdc_writer.go',
        'core/state/snapshots/compressed_block_cdc_digest_reuse_test.go',
        'core/state/snapshots/compressed_block_cdc_digest_reuse_oracle_test.go',
    }
    module.TEST_PATTERN += '|Test.*(HistoryPipeline|HistorySharedPipeline|CDCDigestReuse)'
    module.REQUIRED_TESTS += (
        ('core/state/snapshots', 'TestCDCDigestReuseFrozenFullFileOracle'),
        ('core/state/snapshots', 'TestCDCDigestReuseCollisionLengthAndOwnedBytes'),
        ('core/state/snapshots', 'TestCDCDigestReuseBoundedEvictionAndReentry'),
        ('core/state/snapshots', 'TestCDCDigestReuseLiteralReferenceDistanceOracle'),
        ('core/state/snapshots', 'TestCDCDigestReuseResetAndCanceledFinishReleaseIndex'),
    )
    module.HELP_CONTRACTS = dict(module.HELP_CONTRACTS)
    module.HELP_CONTRACTS['benchmark-history-cold'] += ('--shared-read-pipeline', '--shared-read-workers')
    # Require independent frozen CDC oracle, full snapshots/cmd and native Sapling.
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
