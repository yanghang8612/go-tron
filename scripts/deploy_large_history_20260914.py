#!/usr/bin/env python3
"""Advance the pinned compatible reader with large-history CPU changes only."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260914-large-history')
SCRIPT_PATH = 'scripts/deploy_large_history_20260914.py'
PREVIOUS_REVISION = '7af1212cd56e60c28ac153c6998e78ac32ccd3af'
PREVIOUS_PATH = 'scripts/deploy_cold_metadata_20260914.py'
PREVIOUS_SHA = '825082bf23b30c53b82a21d5df347b3c3166d82543184dfe034a2c739481d45d'
PREVIOUS_NATIVE_SCRIPT = '/tmp/deploy-cold-metadata-7af1212c.py'
CURRENT_SOURCE = '64ed654f582a58f8c03578ba38cd78f7ba4d385e'
CURRENT_PREPARED_SHA = '7ed649e1f863ae363de7edaab164bd3062c4d45458c420fda783bf3ab4a615e8'
PREDECESSOR_PARENT_SHA = '3cd5417ec7dd09474597cd4d34c4a5b0c5ad2ae09b9c063fc623e78950360fcb'
CURRENT_PID, CURRENT_TICKS = 18282, 4509893088
CURRENT_START = 1789364437368890714
SOURCE_REVISION = 'cda09766dfc6cce85253484430f4297d8751ea92'
ALLOWED_FILES = (
    'core/rawdb/state_changeset_shared.go',
    'core/rawdb/state_changeset_shared_authenticated_bench_test.go',
    'core/rawdb/state_changeset_shared_authenticated_oracle_test.go',
    'core/rawdb/state_changeset_shared_authenticated_test.go',
    'core/state/delegation_cache.go',
    'core/state/delegation_membership_arena_test.go',
    'core/state/delegation_membership_bench_test.go',
    'core/state/delegation_membership_oracle_test.go',
    'core/state/delegation_performance_test.go',
    'scripts/deploy_cold_metadata_20260914.py',
    'scripts/tests/test_deploy_cold_metadata_20260914.py',
)
APPROVED = True


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def previous_adapter():
    blob = subprocess.check_output(['git', 'show', PREVIOUS_REVISION + ':' + PREVIOUS_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)
    require(hashlib.sha256(blob).hexdigest() == PREVIOUS_SHA, 'cold metadata predecessor adapter changed')
    module = types.ModuleType('large_history_cold_metadata_predecessor')
    module.__file__ = PREVIOUS_NATIVE_SCRIPT
    exec(compile(blob, module.__file__, 'exec'), module.__dict__)
    return module


def configure(engine):
    require(APPROVED and ALLOWED_FILES and len(SOURCE_REVISION) == 40,
            'large history source review not frozen')
    engine.REPO, engine.RELEASE, engine.BINARY = REPO, RELEASE, RELEASE / 'gtron'
    engine.SCRIPT_PATH = SCRIPT_PATH
    engine.CURRENT_SOURCE, engine.SOURCE_REVISION = CURRENT_SOURCE, SOURCE_REVISION
    engine.CURRENT_PID, engine.CURRENT_TICKS, engine.CURRENT_START = CURRENT_PID, CURRENT_TICKS, CURRENT_START
    engine.ALLOWED_FILES, engine.APPROVED = tuple(ALLOWED_FILES), APPROVED

    def load(args):
        require(args.old_prepared_sha == CURRENT_PREPARED_SHA, '64ed654f predecessor attestation required')
        adapter = previous_adapter()
        previous = adapter.configure(adapter.previous_adapter().previous_adapter().previous_module(PREVIOUS_NATIVE_SCRIPT))
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
        i.NATIVE_TEST_PATTERN = 'Test.*(History|Shared|StateChange|StateDomainChangePrune|AsOf|Unwind|ResetMutableState|Snapshot.*Coverage|Manifest|Companion|Trusted|Delegation|Delegated|DrAccountIndex|Commitment)'
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
            guard=guard, queue_adapter=previous_bundle.queue_adapter,
            ancestor_markers=(predecessor.record['old_armed'],) + previous_bundle.ancestor_markers)

    base = engine.Deployment
    class Deployment(base):
        def check(self, *args, **kwargs):
            # The generic transaction retains the immediate 64ed654f marker;
            # retain 8a0681d3, af989, 8058 and D656 as well in every transition.
            for item in self.b.ancestor_markers:
                require(self.b.h.saved_file(item['path']) == item, 'ancestor permanent reader marker changed')
            return super().check(*args, **kwargs)

        def healthy_mode(self, mode):
            result = super().healthy_mode(mode)
            metrics = self.b.g.read_local_metrics()
            require(metrics.get('process/start/unix_nano', {}).get('value') ==
                    result['observer']['process_start_unix_nano'], 'GC metric process changed')
            # Both the candidate and the compatible 64ed654f recovery target
            # must retain the six queue metrics already deployed today.
            self.b.queue_adapter.validate_queue_metrics(metrics)
            return result

    engine.load, engine.Deployment = load, Deployment
    return engine


def main():
    require(APPROVED, 'large history upgrade review not frozen')
    return configure(previous_adapter().previous_adapter().previous_adapter().previous_module(__file__)).main()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(repr(error), file=sys.stderr, flush=True)
        sys.exit(1)
