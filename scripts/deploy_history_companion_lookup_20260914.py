#!/usr/bin/env python3
"""Advance the pinned GC-capable reader without changing storage policy."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260914-history-companion-lookup')
SCRIPT_PATH = 'scripts/deploy_history_companion_lookup_20260914.py'
PREVIOUS_REVISION = 'b1822994cc51194a07a58ea16a38f45b7f5cd6bf'
PREVIOUS_PATH = 'scripts/deploy_history_gc_queue_20260914.py'
PREVIOUS_SHA = '84caa5a282f5bcc5c56dfbc767f5b1780725dc50603738de26e1471f9204f550'
PREVIOUS_NATIVE_SCRIPT = '/tmp/deploy-history-gc-queue-b182.py'
CURRENT_SOURCE = 'af98975346ef1d306bfe1f054bc8019b447877de'
CURRENT_PREPARED_SHA = '58e659ce777824afa69d8de33fcd31700ff8247a026ce8141f44f795718349a1'
PREDECESSOR_PARENT_SHA = '8b00e50505ef3c91e4f71c6366285c9a9c352b7b57579eb040c086295d7089cc'
CURRENT_PID, CURRENT_TICKS = 20814, 4509100338
CURRENT_START = 1789356509868677122
SOURCE_REVISION = '8a0681d3b62be2375dbcb0b449c4efe7b8ad5279'
ALLOWED_FILES = (
    'core/state/pruning/history_companion_view_bench_test.go',
    'core/state/pruning/history_companion_view_test.go',
    'core/state/pruning/verification_cache.go',
    'core/state/pruning/worker.go',
    'core/state/snapshots/history_companion_view.go',
    'core/state/snapshots/history_companion_view_test.go',
    'scripts/deploy_history_gc_queue_20260914.py',
    'scripts/tests/test_deploy_history_gc_queue_20260914.py',
)
APPROVED = True


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def previous_adapter():
    blob = subprocess.check_output(['git', 'show', PREVIOUS_REVISION + ':' + PREVIOUS_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)
    require(hashlib.sha256(blob).hexdigest() == PREVIOUS_SHA, 'GC predecessor adapter changed')
    module = types.ModuleType('history_lookup_gc_predecessor')
    module.__file__ = PREVIOUS_NATIVE_SCRIPT
    exec(compile(blob, module.__file__, 'exec'), module.__dict__)
    return module


def configure(engine):
    require(APPROVED and ALLOWED_FILES and len(SOURCE_REVISION) == 40,
            'companion lookup source review not frozen')
    engine.REPO, engine.RELEASE, engine.BINARY = REPO, RELEASE, RELEASE / 'gtron'
    engine.SCRIPT_PATH = SCRIPT_PATH
    engine.CURRENT_SOURCE, engine.SOURCE_REVISION = CURRENT_SOURCE, SOURCE_REVISION
    engine.CURRENT_PID, engine.CURRENT_TICKS, engine.CURRENT_START = CURRENT_PID, CURRENT_TICKS, CURRENT_START
    engine.ALLOWED_FILES, engine.APPROVED = tuple(ALLOWED_FILES), APPROVED

    def load(args):
        require(args.old_prepared_sha == CURRENT_PREPARED_SHA, 'af989 predecessor attestation required')
        adapter = previous_adapter()
        previous = adapter.configure(adapter.previous_module(PREVIOUS_NATIVE_SCRIPT))
        previous_args = types.SimpleNamespace(mode='activate', source_revision=CURRENT_SOURCE,
            script_revision=PREVIOUS_REVISION, old_prepared_sha=PREDECESSOR_PARENT_SHA,
            prepared_sha=CURRENT_PREPARED_SHA, rollback_writer='on')
        previous_bundle = previous.load(previous_args)
        predecessor = previous.Deployment(previous_bundle, previous_args)
        # Immutable attestation must load even during partial recovery. Only
        # prepare/admit compares the live predecessor configuration.
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
        i.NATIVE_TEST_PATTERN = 'Test.*(History|Shared|StateChange|StateDomainChangePrune|AsOf|Unwind|ResetMutableState|Snapshot.*Coverage|Manifest|Companion)'
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
            guard=guard, queue_adapter=adapter,
            ancestor_markers=(predecessor.record['old_armed'], previous_bundle.ancestor_marker))

    base = engine.Deployment
    class Deployment(base):
        def check(self, *args, **kwargs):
            # The generic transaction retains the immediate af989 marker;
            # retain its 8058 and D656 ancestors as well in every transition.
            for item in self.b.ancestor_markers:
                require(self.b.h.saved_file(item['path']) == item, 'ancestor permanent reader marker changed')
            return super().check(*args, **kwargs)

        def healthy_mode(self, mode):
            result = super().healthy_mode(mode)
            metrics = self.b.g.read_local_metrics()
            require(metrics.get('process/start/unix_nano', {}).get('value') ==
                    result['observer']['process_start_unix_nano'], 'GC metric process changed')
            # Both the candidate and the compatible af989 recovery target
            # must retain the six queue metrics already deployed today.
            self.b.queue_adapter.validate_queue_metrics(metrics)
            return result

    engine.load, engine.Deployment = load, Deployment
    return engine


def main():
    require(APPROVED, 'companion lookup upgrade review not frozen')
    return configure(previous_adapter().previous_module(__file__)).main()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(repr(error), file=sys.stderr, flush=True)
        sys.exit(1)
