#!/usr/bin/env python3
"""Prepare a fresh native GC-accounting candidate without touching the service.

The source scope remains disabled until the exact business paths/test names are
frozen. The previous reviewed native builder is reused, never a deployment chain.
"""
import argparse
import hashlib
import json
from pathlib import Path
import os
import signal
import subprocess
import sys
import types

REPO = Path('/data/gtron/go-tron')
BASE = '9d4262fcfa78bf7de8a03391e18eb0896293652e'
PARENT_PATH = 'scripts/prepare_history_shared_read_20260915.py'
PARENT_SHA = '8f7806ca43bce83b98382968d63e87b10011c53bd6c03974e04be32ff0c22877'
SCRIPT_PATH = 'scripts/prepare_history_gc_accounting_20260915.py'
RELEASE = Path('/data/gtron/releases/20260915-history-gc-accounting')
SOURCE_REVISION = '197b73a920a826f21b460b3a85cc0e7ba1727d99'
SCOPE_FROZEN = True
ALLOWED_FILES = {
    'core/state/pruning/worker.go',
    'core/state/pruning/lifecycle.go',
    'core/state/pruning/history_gc_density_test.go',
    'core/state/snapshots/cold_builder.go',
    'core/state/snapshots/history_load.go',
    'core/state/snapshots/history_work_cost.go',
    'core/state/snapshots/history_gc_density_test.go',
    'scripts/activate_history_shared_read_20260915.py',
    'scripts/tests/test_activate_history_shared_read_20260915.py',
}
REQUIRED_FILES = set(ALLOWED_FILES)
SNAPSHOT_TESTS = tuple(('core/state/snapshots', name) for name in (
    'TestHistoryIndependentGCDoesNotShrinkRowDensity',
    'TestHistoryIndependentGCTimingBoundsAndLegacy',
    'TestHistoryIndependentGCCompleteMaintenanceStillCharged'))
PRUNING_TESTS = tuple(('core/state/pruning', name) for name in (
    'TestWorkerIndependentGCTimerIncludesScanProofAndGuard',
    'TestLifecycleIndependentGCTimingReachesDensity'))
OLD_OPS = {
    'scripts/activate_history_shared_read_20260915.py': 'a032a79b96a4fbc5c0c27f87d4b68d8281e3bebb84ba297761931df0f5319137',
    'scripts/tests/test_activate_history_shared_read_20260915.py': 'dc0d78abeb530c2984544b01c0cc52a142732d03095fa970f084ec063c947a36',
}


def load_builder():
    blob = subprocess.check_output(['git', 'show', BASE + ':' + PARENT_PATH], cwd=str(REPO),
                                   stdin=subprocess.DEVNULL, timeout=60)
    if hashlib.sha256(blob).hexdigest() != PARENT_SHA:
        raise RuntimeError('reviewed parent builder bytes differ')
    parent = types.ModuleType('gc_accounting_parent_builder')
    parent.__file__ = str(Path(__file__).resolve())
    exec(compile(blob, PARENT_PATH, 'exec'), parent.__dict__)
    parent.REPO, parent.BASE = REPO, BASE
    builder = parent.load_builder()
    builder.BASE, builder.RELEASE, builder.SOURCE = BASE, RELEASE, RELEASE / 'source'
    builder.BINARY, builder.SCRIPT_PATH = RELEASE / 'gtron-inspect', SCRIPT_PATH
    builder.ALLOWED_SOURCE_CHANGES = set(ALLOWED_FILES)
    builder.REQUIRED_SOURCE_CHANGES = set(REQUIRED_FILES)
    builder.TEST_PATTERN += '|Test.*(History.*(Density|Work|Maintenance|Recovery|GC)|Cold.*GC)'
    builder.REQUIRED_TESTS += SNAPSHOT_TESTS
    return builder


def prepare(builder, args):
    builder.require(SCOPE_FROZEN and args.source_revision == SOURCE_REVISION,
                    'GC accounting source scope is not frozen or differs')
    for path, checksum in OLD_OPS.items():
        builder.require(hashlib.sha256(builder.git('show', SOURCE_REVISION + ':' + path)).hexdigest() == checksum,
                        'previous frozen ops bytes differ')
    result = builder.prepare(args)
    # The old builder intentionally keeps its original package list. A separate
    # proof is mandatory for activation; base prepared.json alone is insufficient.
    helper, commands = builder.pure_helpers(), []
    env = builder.build_environment()
    prepared = json.loads(helper.read_regular(RELEASE / 'prepared.json'))
    try:
        builder.require(env == prepared['build_environment'], 'native pruning environment differs')
        common = [builder.GO, 'test', '-json', '-p', '2', '-tags', 'sapling', './core/state/pruning']
        pattern = '^(' + '|'.join(name for unused, name in PRUNING_TESTS) + ')$'
        commands.append(builder.run_logged(common + ['-run', pattern, '-count=1', '-timeout=300s'],
                                           'native-gc-pruning-focused.jsonl', env, timeout=660))
        focused = builder.verify_test_log(helper, commands[-1]['log'], ('core/state/pruning',), PRUNING_TESTS)
        commands.append(builder.run_logged(common + ['-count=1', '-timeout=300s'],
                                           'native-gc-pruning-full.jsonl', env, timeout=660))
        full = builder.verify_test_log(helper, commands[-1]['log'], ('core/state/pruning',), PRUNING_TESTS)
        helper.verify_source(None, prepared['manifest'])
        builder.require(builder.repository_state() == prepared['checkout_after'], 'production checkout changed')
        for path, checksum in ((builder.GO, prepared['go_binary_sha256']),
                               (prepared['rust_library'], prepared['rust_library_sha256']),
                               (builder.BINARY, prepared['binary_sha256']),
                               (RELEASE / 'prepared.json', result['prepared_sha256']),
                               (__file__, prepared['script_sha256'])):
            builder.require(builder.file_sha(path) == checksum, 'native input changed during pruning tests')
        proof = dict(complete=True, source_commit=SOURCE_REVISION, binary=str(builder.BINARY),
                     binary_sha256=result['binary_sha256'], prepared_sha256=result['prepared_sha256'],
                     go_version=prepared['go_version'], build_environment=env, native_commands=commands,
                     native_focused_tests=focused, native_full_tests=full)
        # umask 077 keeps source/evidence private; only the executable's release
        # directory becomes traversable by the existing service user.
        directory = os.open(str(RELEASE), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fchmod(directory, 0o755)
            os.fsync(directory)
        finally:
            os.close(directory)
        builder.write_json('gc-accounting-native.json', proof)
        result.update(gc_accounting_complete=True, native_sha256=builder.file_sha(RELEASE / 'gc-accounting-native.json'))
        return result
    except BaseException as error:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, builder.SIGNALS)
        try:
            builder.write_json('gc-accounting-native-failure.json',
                               dict(complete=False, source_commit=SOURCE_REVISION, error=repr(error), native_commands=commands))
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('source-revision', 'script-revision', 'rust-library-sha256'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise RuntimeError('native root execution required')
    if not SCOPE_FROZEN or not SOURCE_REVISION:
        raise RuntimeError('GC accounting source/test scope is not frozen')
    builder = load_builder()
    os.umask(0o077)
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, builder.interrupted)
    print(json.dumps(prepare(builder, args), sort_keys=True), flush=True)


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(json.dumps({'prepared': False, 'error': repr(error)}), file=sys.stderr)
        sys.exit(1)
