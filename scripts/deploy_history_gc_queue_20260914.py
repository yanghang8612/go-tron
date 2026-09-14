#!/usr/bin/env python3
"""Reuse the reviewed compatible-reader transaction, advancing from 8058 only."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260914-history-gc-queue')
SCRIPT_PATH = 'scripts/deploy_history_gc_queue_20260914.py'
PREVIOUS_REVISION = '6d970e0b256182f1517d7e957180c781f19c45c4'
PREVIOUS_PATH = 'scripts/deploy_history_sharing_cpu_20260913.py'
PREVIOUS_SHA = '37d2056681e515b50d4edf8de63b7651aa0debcbe4da36e1e3e615f4d3831bd3'
PREVIOUS_NATIVE_SCRIPT = '/tmp/deploy-history-sharing-cpu-6d97.py'
CURRENT_SOURCE = '8058e99d4e080c3329a587a2feef16133e676e84'
CURRENT_PREPARED_SHA = '8b00e50505ef3c91e4f71c6366285c9a9c352b7b57579eb040c086295d7089cc'
ANCESTOR_PREPARED_SHA = 'ef7f4c0ff67e323b56df3a7868a34a4bc096e6c7f08330047ec366016c627fad'
CURRENT_PID, CURRENT_TICKS = 25805, 4504138768
CURRENT_START = 1789306894172214942
SOURCE_REVISION = 'af98975346ef1d306bfe1f054bc8019b447877de'
ALLOWED_FILES = (
    'core/state/pruning/history_shared_chunk_gc.go',
    'core/state/pruning/history_shared_chunk_gc_test.go',
    'core/state/pruning/worker.go',
    'core/state_domain_change_prune_guard.go',
    'scripts/deploy_history_sharing_cpu_20260913.py',
    'scripts/tests/test_deploy_history_sharing_cpu_20260913.py',
)  # Exact non-doc 8058..af989 scope; this adapter is pinned separately by ops.
APPROVED = True
QUEUE_METRICS = ('queued_attempts', 'queued_admitted', 'queued_busy', 'queued_errors',
                 'queued_work_total_ns', 'queued_work_max_ns')


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def previous_module(filename):
    blob = subprocess.check_output(['git', 'show', PREVIOUS_REVISION + ':' + PREVIOUS_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)
    require(hashlib.sha256(blob).hexdigest() == PREVIOUS_SHA, 'compatible engine blob changed')
    module = types.ModuleType('history_gc_compatible_engine')
    module.__file__ = filename
    exec(compile(blob, filename, 'exec'), module.__dict__)
    return module


def validate_queue_metrics(metrics):
    for key in QUEUE_METRICS:
        value = metrics.get('state/history/shared/gc/' + key, {}).get('value')
        require(type(value) is int and value >= 0, 'candidate GC metric missing or invalid: ' + key)


def configure(engine):
    """Keep candidate and predecessor modules entirely separate."""
    require(APPROVED and ALLOWED_FILES and len(SOURCE_REVISION) == 40,
            'GC upgrade source review not frozen')
    engine.REPO, engine.RELEASE, engine.BINARY = REPO, RELEASE, RELEASE / 'gtron'
    engine.SCRIPT_PATH = SCRIPT_PATH
    engine.CURRENT_SOURCE, engine.SOURCE_REVISION = CURRENT_SOURCE, SOURCE_REVISION
    engine.CURRENT_PID, engine.CURRENT_TICKS, engine.CURRENT_START = CURRENT_PID, CURRENT_TICKS, CURRENT_START
    engine.ALLOWED_FILES, engine.APPROVED = tuple(ALLOWED_FILES), APPROVED

    def load(args):
        require(args.old_prepared_sha == CURRENT_PREPARED_SHA, '8058 predecessor attestation required')
        previous = previous_module(PREVIOUS_NATIVE_SCRIPT)
        previous_args = types.SimpleNamespace(mode='activate', source_revision=CURRENT_SOURCE,
            script_revision=PREVIOUS_REVISION, old_prepared_sha=ANCESTOR_PREPARED_SHA,
            prepared_sha=CURRENT_PREPARED_SHA, rollback_writer='on')
        previous_bundle = previous.load(previous_args)
        predecessor = previous.Deployment(previous_bundle, previous_args)
        # Construction verifies immutable native evidence; live comparison is
        # reserved for prepare/admit, so recovery can load a partial transaction.
        def compare(mode):
            require(mode == 'on', 'unreviewed predecessor mode')
            return predecessor.check('candidate')
        old = types.SimpleNamespace(record=predecessor.record, compare=compare)
        original = types.SimpleNamespace(BINARY=previous.BINARY,
            MARKER=previous_bundle.original.MARKER,
            ARMED=previous.RELEASE / 'reader-required.json',
            DROPIN=previous_bundle.original.DROPIN, GUARD=previous_bundle.original.GUARD,
            GC_METRICS=previous_bundle.original.GC_METRICS)
        guard = previous_bundle.guard
        i = engine.pinned_module(engine.BUILDER_REVISION, engine.BUILDER_PATH, engine.BUILDER_SHA, __file__)
        h = i.load_helper()
        i.REPO, i.RELEASE, i.SOURCE, i.BINARY = REPO, RELEASE, RELEASE / 'source', engine.BINARY
        i.CURRENT_SOURCE, i.CURRENT_EXE, i.CURRENT_SHA = CURRENT_SOURCE, str(previous.BINARY), predecessor.record['binary_sha256']
        i.CURRENT_PID, i.CURRENT_TICKS = CURRENT_PID, CURRENT_TICKS
        i.SCRIPT_PATH, i.INSPECT_APPROVED, i.ALLOWED_FILES = SCRIPT_PATH, APPROVED, tuple(ALLOWED_FILES)
        i.NATIVE_TEST_PATTERN = 'Test.*(History|Shared|StateChange|StateDomainChangePrune|AsOf|Unwind|ResetMutableState)'
        i.normalize_exec_start = guard.parse_commands
        h.RELEASE, h.BINARY, h.OLD_EXE, h.OLD_SHA = RELEASE, engine.BINARY, i.CURRENT_EXE, i.CURRENT_SHA
        h.OLD_PID, h.OLD_START_TICKS = CURRENT_PID, CURRENT_TICKS
        def enabled(exe):
            require(exe in (i.CURRENT_EXE, str(engine.BINARY)), 'unapproved compatible reader')
            return '1'
        h.expected_history_range_environment = h.expected_history_queue_environment = enabled
        wrapper = engine.pinned_module(engine.WRAPPER_REVISION, engine.WRAPPER_PATH, engine.WRAPPER_SHA, engine.WRAPPER_PATH)
        wrapper.install_show(h, guard)
        return types.SimpleNamespace(original=original, old=old, g=previous_bundle.g, i=i, h=h,
            guard=guard, ancestor_marker=predecessor.record['old_armed'])

    base = engine.Deployment
    class Deployment(base):
        def check(self, *args, **kwargs):
            # The generic engine preserves the immediate 8058 ARMED marker.
            # Also retain its D656 ancestor unchanged through every transition.
            item = self.b.ancestor_marker
            require(self.b.h.saved_file(item['path']) == item, 'ancestor permanent reader marker changed')
            return super().check(*args, **kwargs)

        def healthy_mode(self, mode):
            result = super().healthy_mode(mode)
            if mode == 'candidate':
                metrics = self.b.g.read_local_metrics()
                require(metrics.get('process/start/unix_nano', {}).get('value') ==
                        result['observer']['process_start_unix_nano'], 'GC metric process changed')
                validate_queue_metrics(metrics)
            return result

    engine.load, engine.Deployment = load, Deployment
    return engine


def main():
    require(APPROVED, 'GC upgrade review not frozen')
    return configure(previous_module(__file__)).main()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(repr(error), file=sys.stderr, flush=True)
        sys.exit(1)
