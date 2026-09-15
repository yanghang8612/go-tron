#!/usr/bin/env python3
"""Python 3.6+ isolated native diagnostic build; never installs or stops a node.

The exact source and executing script must already exist as fetched Git commits.
The production checkout and native Rust library are read-only build inputs.
Only a fresh diagnostic release and normal Go caches receive writes. A failed
release is retained for review; this script never repairs or replaces it.
"""
import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import signal
import stat
import subprocess
import sys
import tarfile
import time
import types

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260915-history-parallel')
SOURCE = RELEASE / 'source'
BINARY = RELEASE / 'gtron-inspect'
GO = '/data/go/bin/go'
CHECKOUT = '19eda11f44424f673a40b06051edcbf629b50846'
BASE = '279dcef84bd225d6a60bee50080bd2feefb11248'
SCRIPT_PATH = 'scripts/prepare_history_parallel_20260915.py'
HELPER_PATH = 'scripts/inspect_history_range_20260915.py'
HELPER_SHA = '7dcbe033f5425484f9a071f1eda1ab962f4a9280ecb41a2666e82854623d6e49'
PURE_FUNCTIONS = ('require', 'sha', 'full_commit', 'read_regular', 'git',
                  'source_tree_manifest', 'extract_private_source', 'verify_source')
ALLOWED_SOURCE_CHANGES = {
    'cmd/gtron/db_cmd.go', 'cmd/gtron/db_history_cold_benchmark.go',
    'cmd/gtron/db_history_parallel_benchmark.go', 'cmd/gtron/db_history_parallel_benchmark_test.go',
    'core/state/snapshots/history_diagnostic.go',
    'core/state/snapshots/chain_freezer_verification_cache.go',
    'core/state/snapshots/chain_freezer_verification_cache_encoding_test.go',
    SCRIPT_PATH, 'scripts/tests/test_prepare_history_parallel_20260915.py',
    'scripts/benchmark_history_parallel_20260915.py',
    'scripts/tests/test_benchmark_history_parallel_20260915.py',
}
REQUIRED_SOURCE_CHANGES = {'cmd/gtron/db_cmd.go', 'cmd/gtron/db_history_parallel_benchmark.go',
                           'cmd/gtron/db_history_parallel_benchmark_test.go'}
TEST_PATTERN = 'Test.*(HistoryParallel|HistoryColdBenchmark|HistoryRangeExport|StateHistoryRangeExport|Owned|VerificationCache)'
REQUIRED_TESTS = (
    ('cmd/gtron', 'TestDBHistoryParallelCompletePartitionsReadOnly'),
    ('cmd/gtron', 'TestDBHistoryParallelCancellationJoinsWorkers'),
    ('cmd/gtron', 'TestDBHistoryParallelSharedOpenCloseAndFailures'),
    ('cmd/gtron', 'TestDBHistoryParallelCLI'),
    ('cmd/gtron', 'TestDBHistoryColdBenchmarkCompleteModesAndReadOnly'),
    ('core/state/snapshots', 'TestChainFreezerVerificationCacheEncodingCompatibility'),
    ('core/state/snapshots', 'TestChainFreezerVerificationCacheEncodingStrictLoad'),
    ('core/rawdb', 'TestStateHistoryRangeExportPhysicalCopyReopensWithRepairsAndEmptyBlock'),
    ('core/rawdb', 'TestReadPresentValueOwnedOracleAndErrors'),
    ('core/rawdb/pebbledb', 'TestKeyValueSnapshotGetOwnedBytes'),
)
MODULE = 'github.com/tronprotocol/go-tron/'
SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
HELP_CONTRACTS = {
    'benchmark-history-parallel': ('--input-dir', '--output-dir', '--workers', '--segments', '--iterations', '--max-duration', '--cpu-profile'),
    'benchmark-history-cold': ('--input-dir', '--output-dir', '--copy-mode', '--compression-format', '--max-duration'),
    'export-history-range': ('--output-dir', '--blocks', '--max-bytes', '--max-decoded-bytes', '--max-rows', '--max-duration'),
}
PROBE_SOURCE = ('package main\nimport("fmt";"os";"github.com/tronprotocol/go-tron/core/zksnark")\n'
                'func main(){if !zksnark.Available(){os.Exit(2)};v,e:=zksnark.Uncommitted();'
                'if e!=nil{panic(e)};fmt.Printf("nativeSapling=true uncommitted=%x\\n",v)}\n')


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def full_hex(value, length):
    require(isinstance(value, str) and re.fullmatch('[0-9a-f]{' + str(length) + '}', value),
            'exact lowercase {0}-hex identity required'.format(length))
    return value


def git(*args):
    env = dict(os.environ, GIT_OPTIONAL_LOCKS='0')
    return subprocess.check_output(['git'] + list(args), cwd=str(REPO), env=env,
                                   stdin=subprocess.DEVNULL, timeout=60)


def resolve(revision):
    return git('rev-parse', '--verify', revision + '^{commit}').decode().strip()


def pure_helpers():
    require(resolve(BASE) == BASE, 'pinned helper Git object unavailable')
    blob = git('show', BASE + ':' + HELPER_PATH)
    require(hashlib.sha256(blob).hexdigest() == HELPER_SHA, 'pure helper Git bytes differ')
    # Compile only named pure definitions. The old prepare/inspect/entrypoint
    # and its nested deployment-module loader are not present in this module.
    tree = ast.parse(blob, filename=HELPER_PATH)
    definitions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in PURE_FUNCTIONS]
    require({node.name for node in definitions} == set(PURE_FUNCTIONS), 'pure helper contract differs')
    tree.body = definitions
    module = types.ModuleType('history_parallel_pure')
    module.__dict__.update(Path=Path, hashlib=hashlib, os=os, stat=stat, subprocess=subprocess,
                           tarfile=tarfile, re=re, REPO=REPO, SOURCE=SOURCE)
    exec(compile(tree, HELPER_PATH, 'exec'), module.__dict__)
    module.git = git
    return module


def file_sha(path, limit=1 << 30):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    digest = hashlib.sha256()
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and 0 <= info.st_size <= limit, 'unsafe bounded file: ' + str(path))
        count = 0
        for data in iter(lambda: stream.read(1 << 20), b''):
            count += len(data)
            require(count <= limit, 'file grew beyond limit')
            digest.update(data)
        require(count == info.st_size, 'file size changed while hashing')
    return digest.hexdigest()


def repository_state():
    require(resolve('HEAD') == CHECKOUT, 'production checkout HEAD changed')
    raw = git('status', '--porcelain', '--untracked-files=no').decode()
    require(all(row[3:] == 'third_party/librustzcash' for row in raw.splitlines()),
            'unexpected tracked production checkout changes')
    # Untracked local material is neither copied into the archive nor removed.
    return {'head': CHECKOUT, 'tracked_status': raw}


def admission(args, helper):
    source, ops = full_hex(args.source_revision, 40), full_hex(args.script_revision, 40)
    full_hex(args.rust_library_sha256, 64)
    require(resolve(source) == source and resolve(ops) == ops, 'source/script Git commit unavailable')
    require(resolve('refs/remotes/origin/master') == ops, 'fetched master tip differs from script revision')
    require(git('merge-base', BASE, source).decode().strip() == BASE, 'source does not descend from reviewed base')
    before = repository_state()
    script = git('show', ops + ':' + SCRIPT_PATH)
    require(script == helper.read_regular(__file__, 256 << 10, root=True), 'executing script differs from Git pin')
    changed = []
    for row in git('diff', '--name-status', '--no-renames', BASE, source, '--').decode().splitlines():
        fields = row.split('\t')
        require(len(fields) == 2 and fields[0] in ('A', 'M'), 'unexpected source change kind')
        name = fields[1]
        require(name in ALLOWED_SOURCE_CHANGES or name.startswith('docs/') and name.endswith('.md'),
                'source change outside reviewed scope: ' + name)
        changed.append(name)
    require(REQUIRED_SOURCE_CHANGES <= set(changed), 'parallel diagnostic source changes missing')
    manifest, gitlinks = helper.source_tree_manifest(source)
    if SCRIPT_PATH in manifest:
        require(git('show', source + ':' + SCRIPT_PATH) == script, 'source archive contains a different builder script')
    return {'source_commit': source, 'script_commit': ops, 'script_sha256': hashlib.sha256(script).hexdigest(),
            'helper_commit': BASE, 'helper_sha256': HELPER_SHA, 'helper_functions': list(PURE_FUNCTIONS),
            'base_commit': BASE, 'changed_files': sorted(changed), 'checkout_before': before,
            'manifest': manifest, 'gitlinks': gitlinks, 'source_file_count': len(manifest)}


def build_environment():
    # Avoid user GOENV/workspace/CGO overrides and test-only GTRON fixture paths.
    # Shared Go caches may be populated; production checkout/config is not.
    return {'PATH': '/data/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
            'HOME': pwd.getpwuid(os.geteuid()).pw_dir, 'LANG': 'C', 'LC_ALL': 'C',
            'CGO_ENABLED': '1', 'GOMAXPROCS': '2', 'GOFLAGS': '-mod=readonly',
            'GOENV': 'off', 'GOWORK': 'off', 'GOTOOLCHAIN': 'local', 'GIT_OPTIONAL_LOCKS': '0'}


def stop_child(child):
    # Only the process group created by run_logged is signalled.
    if child is None:
        return
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
    try:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        finally:
            # The leader may already be reaped while a compiler/test child
            # remains in its private process group.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=10)
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def run_logged(argv, name, env, timeout=1800):
    require(Path(name).name == name, 'log name must be local')
    path, started, child = RELEASE / name, time.time(), None
    with open(str(path), 'xb') as output:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            try:
                child = subprocess.Popen([str(arg) for arg in argv], cwd=str(SOURCE), env=env,
                                         stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                                         start_new_session=True,
                                         preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_SETMASK, previous))
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous)
            code = child.wait(timeout=timeout)
        finally:
            stop_child(child)
            output.flush()
            os.fsync(output.fileno())
    require(code == 0, 'command failed ({0}); see {1}'.format(code, path))
    return {'argv': [str(arg) for arg in argv], 'cwd': str(SOURCE), 'returncode': code,
            'started_at': started, 'finished_at': time.time(), 'log': name, 'log_sha256': file_sha(path)}


def verify_test_log(helper, name, packages, required=()):
    package_passes, test_passes, diagnostic_lines = set(), set(), 0
    for row in helper.read_regular(RELEASE / name, 64 << 20).splitlines():
        # Go download/compiler diagnostics can precede its JSON event stream.
        # Preserve them in the hashed log; they cannot satisfy any PASS gate.
        try:
            event = json.loads(row)
        except ValueError:
            require(not row.lstrip().startswith(b'{'), 'malformed native test JSON event')
            diagnostic_lines += 1
            continue
        require(isinstance(event, dict), 'invalid native test JSON event')
        require(event.get('Action') != 'fail', 'native tests reported failure')
        if event.get('Action') == 'pass':
            pair = (event.get('Package'), event.get('Test'))
            if pair[1]:
                test_passes.add(pair)
            else:
                package_passes.add(pair[0])
    require({MODULE + p for p in packages} <= package_passes, 'native package PASS evidence missing')
    require({(MODULE + p, test) for p, test in required} <= test_passes, 'required native test PASS evidence missing')
    return {'packages_passed': sorted(package_passes), 'test_pass_count': len(test_passes),
            'required_tests': [list(pair) for pair in required], 'diagnostic_lines': diagnostic_lines}


def write_json(name, value):
    target, temporary = RELEASE / name, RELEASE / ('.' + name + '.tmp')
    require(not os.path.lexists(target), 'evidence already exists: ' + name)
    with open(str(temporary), 'xb') as output:
        os.fchmod(output.fileno(), 0o600)
        output.write((json.dumps(value, sort_keys=True, indent=2) + '\n').encode())
        output.flush()
        os.fsync(output.fileno())
    os.rename(str(temporary), str(target))
    fd = os.open(str(RELEASE), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def prepare(args):
    helper = pure_helpers()
    record = admission(args, helper)
    require(RELEASE.parent.is_dir() and not RELEASE.parent.is_symlink() and RELEASE.parent.resolve() == RELEASE.parent,
            'release parent must be an existing real directory')
    require(not os.path.lexists(RELEASE), 'fresh release required; retain any old release for review')
    rust = REPO / 'third_party/librustzcash'
    library = rust / 'target/release/librustzcash.a'
    require(file_sha(library) == args.rust_library_sha256, 'reviewed native Sapling SHA differs')
    go_sha = file_sha(GO)
    RELEASE.mkdir(mode=0o755)
    try:
        write_json('admission.json', record)
        archive = RELEASE / 'source.tar'
        git('archive', '--format=tar', '--output=' + str(archive), args.source_revision)
        archive_sha = file_sha(archive)
        helper.extract_private_source(archive, record['manifest'])
        helper.verify_source(None, record['manifest'])
        link = SOURCE / 'third_party/librustzcash'
        link.parent.mkdir(parents=True, exist_ok=True)
        if link.exists():
            require(link.is_dir() and not any(link.iterdir()), 'archived Rust directory is nonempty')
            link.rmdir()
        link.symlink_to(rust, target_is_directory=True)
        env, evidence = build_environment(), []
        evidence.append(run_logged([GO, 'version'], 'go-version.txt', env, timeout=60))
        version = helper.read_regular(RELEASE / 'go-version.txt').decode().strip()
        require(version == 'go version go1.25.5 linux/amd64', 'native Go version differs')
        packages = ('cmd/gtron', 'core/rawdb', 'core/rawdb/pebbledb', 'core/state/snapshots')
        evidence.append(run_logged([GO, 'test', '-json', '-p', '2', '-tags', 'sapling'] +
                                   ['./' + p for p in packages] + ['-run', TEST_PATTERN, '-count=1', '-timeout=300s'],
                                   'native-focused-tests.jsonl', env))
        focused = verify_test_log(helper, 'native-focused-tests.jsonl', packages, REQUIRED_TESTS)
        full_packages = ('core/state/snapshots', 'cmd/gtron')
        evidence.append(run_logged([GO, 'test', '-json', '-p', '2', '-tags', 'sapling'] +
                                   ['./' + p for p in full_packages] + ['-count=1', '-timeout=300s'],
                                   'native-full-tests.jsonl', env))
        full = verify_test_log(helper, 'native-full-tests.jsonl', full_packages)
        probe = RELEASE / 'native-sapling-probe.go'
        with open(str(probe), 'x') as output:
            output.write(PROBE_SOURCE)
        evidence.append(run_logged([GO, 'run', '-p', '2', '-tags', 'sapling', str(probe)],
                                   'native-sapling-probe.log', env, timeout=600))
        require(re.search(r'^nativeSapling=true uncommitted=[0-9a-f]{64}$',
                          helper.read_regular(RELEASE / 'native-sapling-probe.log').decode().strip()),
                'native Sapling availability evidence missing')
        evidence.append(run_logged([GO, 'build', '-p', '2', '-tags', 'sapling', '-buildvcs=false',
                                   '-o', str(BINARY), './cmd/gtron'], 'build.log', env))
        require(BINARY.is_file() and not BINARY.is_symlink(), 'diagnostic binary missing')
        os.chmod(str(BINARY), 0o755)
        evidence.append(run_logged([GO, 'version', '-m', str(BINARY)], 'build-info.txt', env, timeout=60))
        info = helper.read_regular(RELEASE / 'build-info.txt').decode()
        require(all(s in info for s in ('go1.25.5', 'CGO_ENABLED=1', '-tags=sapling', 'GOARCH=amd64', 'GOOS=linux')),
                'native diagnostic build settings differ')
        for command, flags in sorted(HELP_CONTRACTS.items()):
            name = command + '-help.txt'
            evidence.append(run_logged([str(BINARY), 'db', command, '--help'], name, env, timeout=60))
            text = helper.read_regular(RELEASE / name).decode()
            require(command in text and all(flag in text for flag in flags), 'registered CLI contract missing: ' + command)
        helper.verify_source(None, record['manifest'])
        require(repository_state() == record['checkout_before'], 'production checkout state changed during build')
        require(file_sha(library) == args.rust_library_sha256, 'native Sapling library changed during build')
        require(file_sha(GO) == go_sha and file_sha(archive) == archive_sha, 'Go tool or source archive changed')
        require(file_sha(__file__, 256 << 10) == record['script_sha256'], 'executing builder script changed')
        record.update(prepared=True, prepared_at=time.time(), binary=str(BINARY), binary_sha256=file_sha(BINARY),
                      archive_sha256=archive_sha, rust_library=str(library), rust_library_sha256=args.rust_library_sha256,
                      go_version=version, go_binary_sha256=go_sha, build_environment=env,
                      native_sapling_probe_sha256=file_sha(probe), native_commands=evidence,
                      native_focused_tests=focused, native_full_tests=full,
                      help_contracts=HELP_CONTRACTS, checkout_after=repository_state())
        write_json('prepared.json', record)
        return {'prepared': True, 'source_commit': args.source_revision, 'binary_sha256': record['binary_sha256'],
                'prepared_sha256': file_sha(RELEASE / 'prepared.json'), 'source_file_count': len(record['manifest'])}
    except BaseException as error:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            if not os.path.lexists(RELEASE / 'prepared.json'):
                write_json('failure.json', {'prepared': False, 'failed_at': time.time(), 'error': repr(error),
                                           'source_commit': args.source_revision, 'script_commit': args.script_revision})
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        raise


def interrupted(signum, unused_frame):
    raise KeyboardInterrupt('diagnostic build interrupted by signal ' + str(signum))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--script-revision', required=True)
    parser.add_argument('--rust-library-sha256', required=True)
    args = parser.parse_args()
    require(os.geteuid() == 0, 'explicit root execution required for the private diagnostic release')
    for sig in SIGNALS:
        signal.signal(sig, interrupted)
    print(json.dumps(prepare(args), sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(json.dumps({'prepared': False, 'error': repr(error)}), file=sys.stderr, flush=True)
        sys.exit(1)
