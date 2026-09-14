#!/usr/bin/env python3
"""Advance the pinned compatible reader with bounded cold recovery observation."""
import hashlib
import math
from pathlib import Path
import subprocess
import sys
import types

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260914-cold-recovery')
SCRIPT_PATH = 'scripts/deploy_cold_recovery_20260914.py'
PREVIOUS_REVISION = '8f76ff8884301026cdf80cabe7b52f6199993111'
PREVIOUS_PATH = 'scripts/deploy_commitment_prefetch_20260914.py'
PREVIOUS_SHA = 'b4773f899a54f51db75ee592623d3b3f82ef61011847bc6d5b3867485945f986'
PREVIOUS_NATIVE_SCRIPT = '/tmp/deploy-commitment-prefetch-8f76ff88.py'
CURRENT_SOURCE = 'c329bf129bc08d891f45575dd1a1f8c7ec8da0e5'
CURRENT_PREPARED_SHA = 'ccdf989a515e8606257a4904c0d3ec27b8c07aeeb2f5c812e5cec3f244fd39c3'
PREDECESSOR_PARENT_SHA = 'd28500787b3992e85d18843529558198491393b376e51ff2efced7412a0efeef'
CURRENT_PID, CURRENT_TICKS = 8982, 4511224735
CURRENT_START = 1789377753834965736
SOURCE_REVISION = '7b2b374ae0d66d19971b72269f3fc14aeb1a8e53'
ALLOWED_FILES = (
    'cmd/gtron/main.go',
    'core/state/pruning/lifecycle.go',
    'core/state/pruning/lifecycle_recovery_observation_test.go',
    'core/state/pruning/lifecycle_resource_observation_test.go',
    'core/state/snapshots/codec_synctest_test.go',
    'core/state/snapshots/cold_builder.go',
    'core/state/snapshots/history_busy_resource.go',
    'core/state/snapshots/history_load.go',
    'core/state/snapshots/history_recovery_observation.go',
    'core/state/snapshots/history_recovery_observation_test.go',
    'scripts/deploy_commitment_prefetch_20260914.py',
    'scripts/tests/test_deploy_commitment_prefetch_20260914.py',
    'scripts/tests/test_verify_commitment_prefetch_20260914.py',
    'scripts/verify_commitment_prefetch_20260914.py',
)
APPROVED = True
RECOVERY_METRICS = (('checks', 'count'), ('accepted_samples', 'count'), ('last_duration', 'value'))


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def validate_recovery_metrics(metrics):
    for name, field in RECOVERY_METRICS:
        path = 'state/snapshot/cold/history/recovery_observation/' + name
        item = metrics.get(path)
        value = item.get(field) if isinstance(item, dict) else None
        require(type(value) in (int, float) and value >= 0 and
                (type(value) is int or math.isfinite(value)), 'invalid recovery metric ' + path + '/' + field)


def previous_adapter():
    blob = subprocess.check_output(['git', 'show', PREVIOUS_REVISION + ':' + PREVIOUS_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)
    require(hashlib.sha256(blob).hexdigest() == PREVIOUS_SHA, 'commitment prefetch predecessor adapter changed')
    module = types.ModuleType('cold_recovery_commitment_prefetch_predecessor')
    module.__file__ = PREVIOUS_NATIVE_SCRIPT
    exec(compile(blob, module.__file__, 'exec'), module.__dict__)
    return module


def configure(engine):
    require(APPROVED and ALLOWED_FILES and len(SOURCE_REVISION) == 40,
            'cold recovery source review not frozen')
    engine.REPO, engine.RELEASE, engine.BINARY = REPO, RELEASE, RELEASE / 'gtron'
    engine.SCRIPT_PATH = SCRIPT_PATH
    engine.CURRENT_SOURCE, engine.SOURCE_REVISION = CURRENT_SOURCE, SOURCE_REVISION
    engine.CURRENT_PID, engine.CURRENT_TICKS, engine.CURRENT_START = CURRENT_PID, CURRENT_TICKS, CURRENT_START
    engine.ALLOWED_FILES, engine.APPROVED = tuple(ALLOWED_FILES), APPROVED

    def load(args):
        require(args.old_prepared_sha == CURRENT_PREPARED_SHA, 'c329bf12 predecessor attestation required')
        adapter = previous_adapter()
        previous = adapter.configure(adapter.previous_adapter().previous_adapter().previous_adapter().previous_adapter().previous_adapter().previous_module(PREVIOUS_NATIVE_SCRIPT))
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
        i.NATIVE_TEST_PATTERN = 'Test.*(History|Shared|StateChange|StateDomainChangePrune|AsOf|Unwind|ResetMutableState|Snapshot.*Coverage|Manifest|Companion|Trusted|Delegation|Delegated|DrAccountIndex|Commitment|Prefetch|BaseReadCache|EventFrontier)'
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
            # The generic transaction retains the immediate c329bf12 marker;
            # retain 833e4a0b, cda09766, 64ed654f, 8a0681d3, af989, 8058 and D656 as well.
            for item in self.b.ancestor_markers:
                require(self.b.h.saved_file(item['path']) == item, 'ancestor permanent reader marker changed')
            return super().check(*args, **kwargs)

        def healthy_mode(self, mode):
            result = super().healthy_mode(mode)
            metrics = self.b.g.read_local_metrics()
            require(metrics.get('process/start/unix_nano', {}).get('value') ==
                    result['observer']['process_start_unix_nano'], 'GC metric process changed')
            # Both the candidate and the compatible c329bf12 recovery target
            # must retain the six queue metrics already deployed today.
            self.b.queue_adapter.validate_queue_metrics(metrics)
            if mode == 'candidate':
                # Startup zeroes are valid. The c329 rollback target predates
                # these metrics and must not be required to export them.
                validate_recovery_metrics(metrics)
            return result

    engine.load, engine.Deployment = load, Deployment
    return engine


def main():
    require(APPROVED, 'cold recovery upgrade review not frozen')
    return configure(previous_adapter().previous_adapter().previous_adapter().previous_adapter().previous_adapter().previous_adapter().previous_module(__file__)).main()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(repr(error), file=sys.stderr, flush=True)
        sys.exit(1)
