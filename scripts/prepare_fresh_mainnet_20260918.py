#!/usr/bin/env python3
"""Build the exact fresh-mainnet gtron candidate; never touches a service or DB."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import types


REPO = Path('/data/gtron/go-tron')
BASE = 'cdbd49f865cc98b868675d78721a2c06d16c9dec'
BUILDER_PATH = 'scripts/prepare_history_parallel_20260915.py'
BUILDER_SHA = '43e1c06ea0065f6cf42a5b7cb60c34d17cba99eddf019e4dd51e2c2fcfe02ef1'
SCRIPT_PATH = 'scripts/prepare_fresh_mainnet_20260918.py'
TEST_PATH = 'scripts/tests/test_prepare_fresh_mainnet_20260918.py'
ACCEPT_PATH = 'scripts/accept_fresh_mainnet_20260918.py'
ACCEPT_TEST_PATH = 'scripts/tests/test_accept_fresh_mainnet_20260918.py'
CUTOVER_PATH = 'scripts/cutover_fresh_mainnet_20260918.py'
CUTOVER_TEST_PATH = 'scripts/tests/test_cutover_fresh_mainnet_20260918.py'
RELEASE = Path('/data/gtron/releases/20260918-fresh-mainnet')
REWARD_GOLDEN = 'TestOldRewardSum_JavaCompoundAssignmentGolden'


def load_builder():
    data = subprocess.check_output(['git', 'show', BASE + ':' + BUILDER_PATH], cwd=str(REPO),
                                   stdin=subprocess.DEVNULL, timeout=60)
    if hashlib.sha256(data).hexdigest() != BUILDER_SHA:
        raise RuntimeError('pinned native builder differs')
    builder = types.ModuleType('fresh_mainnet_builder')
    builder.__file__ = str(Path(__file__).resolve())
    exec(compile(data, BUILDER_PATH, 'exec'), builder.__dict__)
    builder.REPO, builder.BASE = REPO, BASE
    builder.RELEASE, builder.SOURCE = RELEASE, RELEASE / 'source'
    builder.BINARY, builder.SCRIPT_PATH = RELEASE / 'gtron', SCRIPT_PATH
    production = {
        'core/state/snapshots/cold_builder.go',
        'core/state/snapshots/retired_metadata_gc.go',
        'core/state/snapshots/retired_metadata_gc_test.go',
        'core/reward/voter_reward.go',
        'core/reward/voter_reward_test.go',
    }
    build_ops = {SCRIPT_PATH, TEST_PATH}
    later_ops = {ACCEPT_PATH, ACCEPT_TEST_PATH, CUTOVER_PATH, CUTOVER_TEST_PATH}
    # The current master may retain the read-only diagnostic. It is archived but
    # neither built nor tested as a deployment prerequisite.
    diagnostic = {
        'cmd/reward-trace/main.go', 'cmd/reward-trace/historical_withdraw.go',
        'cmd/reward-trace/historical_withdraw_test.go',
        'scripts/prepare_reward_trace_20260918.py',
        'scripts/tests/test_prepare_reward_trace_20260918.py',
    }
    builder.ALLOWED_SOURCE_CHANGES = production | build_ops | later_ops | diagnostic
    builder.REQUIRED_SOURCE_CHANGES = production | build_ops
    builder.TEST_PATTERN = 'Test.*(RetiredMetadata|PrepareRetiredMetadata|ManifestCache|ManifestPublication|StateHistorySpan)'
    builder.REQUIRED_TESTS = (
        ('core/state/snapshots', 'TestPrepareRetiredMetadataSweepOnlyForgetsConfirmedMissing'),
        ('core/state/snapshots', 'TestPrepareRetiredMetadataSweepFailsClosed'),
        ('core/state/snapshots', 'TestRetiredMetadataSweepCursorBoundedAndMutationSafe'),
        ('core/state/snapshots', 'TestRunnerRetiredMetadataCursorCommitsOnlyAfterIntegration'),
        ('core/state/snapshots', 'TestRunnerRetiredMetadataSweepCancellationAndGuardRace'),
        ('core/state/snapshots', 'TestManifestCacheAuthenticatesCurrentBytes'),
        ('core/state/snapshots', 'TestManifestPublicationSeedsExactDecodedView'),
        ('core/rawdb', 'TestStateHistorySpanColdOrderOracle'),
    )
    original_environment = builder.build_environment
    builder.build_environment = lambda: dict(original_environment(), GOAMD64='v1')
    if hasattr(builder, 'CHECKOUT'):
        delattr(builder, 'CHECKOUT')
    return builder


def rename_evidence(builder, source, target):
    source_path, target_path = builder.RELEASE / source, builder.RELEASE / target
    builder.require(not os.path.lexists(target_path), 'evidence already exists: ' + target)
    os.rename(str(source_path), str(target_path))
    directory = os.open(str(builder.RELEASE), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def prepare(builder, args):
    base_result = builder.prepare(args)
    prepared_path = builder.RELEASE / 'prepared.json'
    base_path = builder.RELEASE / 'base-prepared.json'
    rename_evidence(builder, 'prepared.json', 'base-prepared.json')
    try:
        env = builder.build_environment()
        evidence = builder.run_logged(
            [builder.GO, 'test', '-json', '-p', '2', './core/reward',
             '-count=1', '-timeout=300s'], 'native-reward-tests.jsonl', env)
        helper = builder.pure_helpers()
        reward = builder.verify_test_log(
            helper, 'native-reward-tests.jsonl', ('core/reward',),
            (('core/reward', REWARD_GOLDEN),))
        go_env_evidence = builder.run_logged(
            [builder.GO, 'env', 'GOOS', 'GOARCH', 'GOAMD64', 'CGO_ENABLED', 'GOVERSION', 'GOTOOLCHAIN'],
            'go-env.txt', env, timeout=60)
        go_env = helper.read_regular(builder.RELEASE / 'go-env.txt').decode().splitlines()
        builder.require(go_env == ['linux', 'amd64', 'v1', '1', 'go1.25.5', 'local'],
                        'native Go target differs')
        prepared = json.loads(helper.read_regular(base_path))
        # The appended package test can take minutes. Recheck every mutable
        # input that the pinned base builder authenticated before publishing
        # the final completion record.
        builder.require(builder.resolve(args.source_revision) == args.source_revision and
                        builder.resolve(args.script_revision) == args.script_revision and
                        builder.resolve('refs/remotes/origin/master') == args.script_revision,
                        'source or script Git identity changed after reward tests')
        helper.verify_source(None, prepared['manifest'])
        builder.require(builder.repository_state() == prepared['checkout_before'],
                        'production checkout changed after reward tests')
        library = builder.REPO / 'third_party/librustzcash/target/release/librustzcash.a'
        builder.require(builder.file_sha(library) == args.rust_library_sha256,
                        'native Sapling library changed after reward tests')
        builder.require(builder.file_sha(builder.GO) == prepared['go_binary_sha256'] and
                        builder.file_sha(builder.RELEASE / 'source.tar') == prepared['archive_sha256'],
                        'Go tool or source archive changed after reward tests')
        builder.require(builder.file_sha(__file__, 256 << 10) == prepared['script_sha256'],
                        'executing fresh builder changed after reward tests')
        prepared['native_commands'].extend([evidence, go_env_evidence])
        prepared.update(candidate_scope=['fresh-mainnet', 'retired-metadata-gc',
                                         'java-voter-reward-parity', 'r1-shared-history-writer'],
                        native_reward_tests=reward, go_environment=go_env,
                        base_prepared_sha256=builder.file_sha(base_path))
        builder.write_json('prepared.json', prepared)
        return dict(base_result, prepared_sha256=builder.file_sha(prepared_path),
                    candidate_scope=prepared['candidate_scope'])
    except BaseException as error:
        if not os.path.lexists(builder.RELEASE / 'fresh-preparation-failure.json'):
            builder.write_json('fresh-preparation-failure.json', {
                'prepared': False, 'phase': 'post-base-reward-gate', 'error': repr(error),
                'source_commit': args.source_revision, 'script_commit': args.script_revision,
                'base_prepared_sha256': builder.file_sha(base_path),
            })
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--script-revision', required=True)
    parser.add_argument('--rust-library-sha256', required=True)
    parser.add_argument('--expected-checkout', required=True)
    args = parser.parse_args()
    if os.geteuid() != 0 or sys.platform != 'linux' or not re.fullmatch('[0-9a-f]{40}', args.expected_checkout):
        raise RuntimeError('root Linux and exact live checkout identity required')
    builder = load_builder(); builder.CHECKOUT = args.expected_checkout
    for sig in builder.SIGNALS:
        signal.signal(sig, builder.interrupted)
    print(json.dumps(prepare(builder, args), sort_keys=True), flush=True)


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(json.dumps({'prepared': False, 'error': repr(error)}), file=sys.stderr, flush=True)
        sys.exit(1)
