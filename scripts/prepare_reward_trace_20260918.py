#!/usr/bin/env python3
"""Build one exact reward-trace commit in an isolated native release.

This preparation step reads Git objects from the production checkout but never
checks files out there, opens the production database, or manages a service.
It archives the reviewed source into a fresh release directory, runs the reward
arithmetic and exporter tests with the pinned Linux toolchain, and builds only
the standalone read-only reward-trace diagnostic.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import types


REPO = Path('/data/gtron/go-tron')
BASE = '52fcca0af68d88ecd189f26b6a4f28aff9970d9f'
BUILDER_COMMIT = 'cdbd49f865cc98b868675d78721a2c06d16c9dec'
BUILDER_PATH = 'scripts/prepare_history_parallel_20260915.py'
BUILDER_SHA = '43e1c06ea0065f6cf42a5b7cb60c34d17cba99eddf019e4dd51e2c2fcfe02ef1'
SCRIPT_PATH = 'scripts/prepare_reward_trace_20260918.py'
TEST_PATH = 'scripts/tests/test_prepare_reward_trace_20260918.py'
RELEASE = Path('/data/gtron/releases/20260918-reward-trace')
BINARY_NAME = 'reward-trace'

REWARD_GOLDEN = 'TestOldRewardSum_JavaCompoundAssignmentGolden'
EXPORTER_TESTS = (
    'TestPlanHistoricalWithdrawRewardSegments',
    'TestCalculateHistoricalReward_JavaCompoundAssignment',
    'TestHistoricalWithdrawExportLimits',
)
HELP_FLAG_NAMES = (
    'withdraw-prestate-block',
    'withdraw-txid',
    'export-output',
    'expect-balance',
    'export-timeout',
)


def load_builder():
    data = subprocess.check_output(['git', 'show', BUILDER_COMMIT + ':' + BUILDER_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=60)
    if hashlib.sha256(data).hexdigest() != BUILDER_SHA:
        raise RuntimeError('isolated native builder Git bytes differ')
    module = types.ModuleType('reward_trace_native_builder')
    module.__file__ = str(Path(__file__).resolve())
    exec(compile(data, BUILDER_PATH, 'exec'), module.__dict__)
    module.REPO, module.BASE = REPO, BASE
    module.RELEASE, module.SOURCE = RELEASE, RELEASE / 'source'
    module.BINARY = RELEASE / BINARY_NAME
    module.SCRIPT_PATH = SCRIPT_PATH
    module.ALLOWED_SOURCE_CHANGES = {
        'core/reward/voter_reward.go',
        'core/reward/voter_reward_test.go',
        'cmd/reward-trace/main.go',
        'cmd/reward-trace/historical_withdraw.go',
        'cmd/reward-trace/historical_withdraw_test.go',
        SCRIPT_PATH,
        TEST_PATH,
    }
    module.REQUIRED_SOURCE_CHANGES = {
        'cmd/reward-trace/main.go',
        'cmd/reward-trace/historical_withdraw.go',
        'cmd/reward-trace/historical_withdraw_test.go',
        SCRIPT_PATH,
        TEST_PATH,
    }
    module.TEST_PATTERN = '^(?:' + '|'.join((REWARD_GOLDEN,) + EXPORTER_TESTS) + ')$'
    module.REQUIRED_TESTS = (
        ('core/reward', REWARD_GOLDEN),
        *(('cmd/reward-trace', name) for name in EXPORTER_TESTS),
    )
    if hasattr(module, 'CHECKOUT'):
        delattr(module, 'CHECKOUT')
    return module


def reward_admission(builder, args, helper):
    source = builder.full_hex(args.source_revision, 40)
    ops = builder.full_hex(args.script_revision, 40)
    builder.require(builder.resolve(source) == source and builder.resolve(ops) == ops,
                    'source/script Git commit unavailable')
    builder.require(builder.resolve('refs/remotes/origin/master') == ops,
                    'fetched master tip differs from script revision')
    builder.require(builder.git('merge-base', builder.BASE, source).decode().strip() == builder.BASE,
                    'source does not descend from reviewed reward fix')
    before = builder.repository_state()
    script = builder.git('show', ops + ':' + SCRIPT_PATH)
    builder.require(script == helper.read_regular(__file__, 256 << 10, root=True),
                    'executing script differs from Git pin')
    changed = []
    for row in builder.git('diff', '--name-status', '--no-renames', builder.BASE, source, '--').decode().splitlines():
        fields = row.split('\t')
        builder.require(len(fields) == 2 and fields[0] in ('A', 'M'),
                        'unexpected source change kind')
        name = fields[1]
        builder.require(name in builder.ALLOWED_SOURCE_CHANGES or
                        name.startswith('docs/') and name.endswith('.md'),
                        'source change outside reviewed reward-trace scope: ' + name)
        changed.append(name)
    builder.require(builder.REQUIRED_SOURCE_CHANGES <= set(changed),
                    'reward-trace source changes missing')
    manifest, gitlinks = helper.source_tree_manifest(source)
    builder.require(SCRIPT_PATH in manifest and
                    builder.git('show', source + ':' + SCRIPT_PATH) == script,
                    'source archive contains a different builder script')
    return {
        'source_commit': source,
        'script_commit': ops,
        'script_sha256': hashlib.sha256(script).hexdigest(),
        'helper_commit': builder.BASE,
        'helper_sha256': builder.HELPER_SHA,
        'helper_functions': list(builder.PURE_FUNCTIONS),
        'builder_commit': BUILDER_COMMIT,
        'builder_sha256': BUILDER_SHA,
        'base_commit': builder.BASE,
        'changed_files': sorted(changed),
        'checkout_before': before,
        'manifest': manifest,
        'gitlinks': gitlinks,
        'source_file_count': len(manifest),
    }


def build_environment(builder):
    env = builder.build_environment()
    env['GOAMD64'] = 'v1'
    return env


def verify_native_identity(builder, version, go_env):
    builder.require(version == 'go version go1.25.5 linux/amd64',
                    'native Go version differs')
    builder.require(go_env == ['linux', 'amd64', 'v1', '1', 'go1.25.5', 'local'],
                    'native Go environment differs')


def verify_focused_test_logs(builder, helper):
    reward_result = builder.verify_test_log(
        helper, 'native-reward-golden.jsonl', ('core/reward',),
        (('core/reward', REWARD_GOLDEN),))
    exporter_result = builder.verify_test_log(
        helper, 'native-exporter-focused.jsonl', ('cmd/reward-trace',),
        tuple(('cmd/reward-trace', name) for name in EXPORTER_TESTS))
    return reward_result, exporter_result


def verify_help_contract(builder, help_text):
    missing = [name for name in HELP_FLAG_NAMES
               if re.search(r'(?m)^\s+-' + re.escape(name) + r'(?:\s|$)', help_text) is None]
    builder.require(not missing,
                    'reward-trace historical export CLI definitions missing: ' + ','.join(missing))


def native_commands(builder):
    go = builder.GO
    exporter_pattern = '^(?:' + '|'.join(EXPORTER_TESTS) + ')$'
    return {
        'reward-golden': [go, 'test', '-json', '-p', '2', './core/reward',
                          '-run', '^' + REWARD_GOLDEN + '$', '-count=1', '-timeout=300s'],
        'exporter-focused': [go, 'test', '-json', '-p', '2', './cmd/reward-trace',
                             '-run', exporter_pattern, '-count=1', '-timeout=300s'],
        'full-packages': [go, 'test', '-json', '-p', '2', './core/reward', './cmd/reward-trace',
                          '-count=1', '-timeout=300s'],
        'import-graph': [go, 'list', '-deps', './cmd/reward-trace'],
        'build': [go, 'build', '-p', '2', '-trimpath', '-buildvcs=false',
                  '-o', str(builder.BINARY), './cmd/reward-trace'],
        'help': [str(builder.BINARY), '-h'],
    }


def prepare(builder, args):
    helper = builder.pure_helpers()
    record = reward_admission(builder, args, helper)
    builder.require(builder.RELEASE.parent.is_dir() and not builder.RELEASE.parent.is_symlink() and
                    builder.RELEASE.parent.resolve() == builder.RELEASE.parent,
                    'release parent must be an existing real directory')
    builder.require(not os.path.lexists(builder.RELEASE),
                    'fresh reward-trace release required; retain any old release for review')
    go_sha = builder.file_sha(builder.GO)
    builder.RELEASE.mkdir(mode=0o755)
    try:
        builder.write_json('admission.json', record)
        archive = builder.RELEASE / 'source.tar'
        builder.git('archive', '--format=tar', '--output=' + str(archive), args.source_revision)
        archive_sha = builder.file_sha(archive)
        helper.extract_private_source(archive, record['manifest'])
        helper.verify_source(None, record['manifest'])

        env = build_environment(builder)
        evidence = []
        evidence.append(builder.run_logged([builder.GO, 'version'], 'go-version.txt', env, timeout=60))
        version = helper.read_regular(builder.RELEASE / 'go-version.txt').decode().strip()
        evidence.append(builder.run_logged(
            [builder.GO, 'env', 'GOOS', 'GOARCH', 'GOAMD64', 'CGO_ENABLED', 'GOVERSION', 'GOTOOLCHAIN'],
            'go-env.txt', env, timeout=60))
        go_env = helper.read_regular(builder.RELEASE / 'go-env.txt').decode().splitlines()
        verify_native_identity(builder, version, go_env)

        commands = native_commands(builder)
        evidence.append(builder.run_logged(commands['reward-golden'],
                                           'native-reward-golden.jsonl', env))
        evidence.append(builder.run_logged(commands['exporter-focused'],
                                           'native-exporter-focused.jsonl', env))
        reward_result, exporter_result = verify_focused_test_logs(builder, helper)
        evidence.append(builder.run_logged(commands['full-packages'],
                                           'native-full-tests.jsonl', env))
        full_result = builder.verify_test_log(
            helper, 'native-full-tests.jsonl', ('core/reward', 'cmd/reward-trace'))

        evidence.append(builder.run_logged(commands['import-graph'], 'import-graph.txt', env, timeout=300))
        imports = helper.read_regular(builder.RELEASE / 'import-graph.txt').decode().splitlines()
        builder.require('github.com/tronprotocol/go-tron/core/zksnark' not in imports,
                        'reward-trace unexpectedly depends on Sapling')

        evidence.append(builder.run_logged(commands['build'], 'build.log', env))
        builder.require(builder.BINARY.is_file() and not builder.BINARY.is_symlink(),
                        'reward-trace binary missing')
        os.chmod(str(builder.BINARY), 0o755)
        evidence.append(builder.run_logged([builder.GO, 'version', '-m', str(builder.BINARY)],
                                           'build-info.txt', env, timeout=60))
        info = helper.read_regular(builder.RELEASE / 'build-info.txt').decode()
        builder.require(all(item in info for item in
                            ('go1.25.5', 'CGO_ENABLED=1', 'GOARCH=amd64',
                             'GOOS=linux', 'GOAMD64=v1')),
                        'native reward-trace build settings differ')
        evidence.append(builder.run_logged(commands['help'],
                                           'reward-trace-help.txt', env, timeout=60))
        help_text = helper.read_regular(builder.RELEASE / 'reward-trace-help.txt').decode()
        verify_help_contract(builder, help_text)

        helper.verify_source(None, record['manifest'])
        builder.require(builder.repository_state() == record['checkout_before'],
                        'production checkout state changed during build')
        builder.require(builder.file_sha(builder.GO) == go_sha and
                        builder.file_sha(archive) == archive_sha,
                        'Go tool or source archive changed')
        builder.require(builder.file_sha(__file__, 256 << 10) == record['script_sha256'],
                        'executing builder script changed')
        record.update(
            prepared=True,
            prepared_at=time.time(),
            binary=str(builder.BINARY),
            binary_sha256=builder.file_sha(builder.BINARY),
            archive_sha256=archive_sha,
            go_version=version,
            go_binary_sha256=go_sha,
            build_environment=env,
            native_commands=evidence,
            native_reward_golden=reward_result,
            native_exporter_focused=exporter_result,
            native_full_tests=full_result,
            help_flags=list(HELP_FLAG_NAMES),
            checkout_after=builder.repository_state(),
        )
        builder.write_json('prepared.json', record)
        return {
            'prepared': True,
            'source_commit': args.source_revision,
            'binary_sha256': record['binary_sha256'],
            'prepared_sha256': builder.file_sha(builder.RELEASE / 'prepared.json'),
            'source_file_count': len(record['manifest']),
        }
    except BaseException as error:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, builder.SIGNALS)
        try:
            if not os.path.lexists(builder.RELEASE / 'prepared.json'):
                builder.write_json('failure.json', {
                    'prepared': False,
                    'failed_at': time.time(),
                    'error': repr(error),
                    'source_commit': args.source_revision,
                    'script_commit': args.script_revision,
                })
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--script-revision', required=True)
    parser.add_argument('--expected-checkout', required=True,
                        help='exact live checkout HEAD to attest without resetting or modifying it')
    args = parser.parse_args()
    if os.geteuid() != 0 or sys.platform != 'linux':
        raise RuntimeError('explicit native root Linux build required')
    if not re.fullmatch('[0-9a-f]{40}', args.expected_checkout):
        raise RuntimeError('exact lowercase checkout identity required')
    builder = load_builder()
    builder.CHECKOUT = args.expected_checkout
    for sig in builder.SIGNALS:
        signal.signal(sig, builder.interrupted)
    print(json.dumps(prepare(builder, args), sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(json.dumps({'prepared': False, 'error': repr(error)}), file=sys.stderr, flush=True)
        sys.exit(1)
