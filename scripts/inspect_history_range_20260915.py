#!/usr/bin/env python3
"""Python 3.6+ native diagnostic preparation and one bounded physical capture.

Never installs a binary or changes a unit, hold, reader marker or checkout.
prepare builds an isolated native Sapling diagnostic while the old service runs.
inspect stops that same service once, exports 16 blocks read-only into a new
private directory, reaps the child and restores the old service in finally.
No benchmark runs while production is stopped. No automatic capture retry.
Use a durable operator session: SIGKILL/host failure cannot execute finally.
"""
import argparse
import base64
import copy
import fcntl
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
RELEASE = Path('/data/gtron/releases/20260915-history-range')
SOURCE, BINARY = RELEASE / 'source', RELEASE / 'gtron-inspect'
RESULT, COPY = RELEASE / 'result', RELEASE / 'result' / 'capture'
SCRIPT_PATH = 'scripts/inspect_history_range_20260915.py'
CURRENT_RELEASE = Path('/data/gtron/releases/20260914-cold-recovery')
CURRENT_EXE = str(CURRENT_RELEASE / 'gtron')
CURRENT_SOURCE = '7b2b374ae0d66d19971b72269f3fc14aeb1a8e53'
SOURCE_REVISION = 'e3a7bf49525ec293d25a9403d0ce0372b93dd961'
CURRENT_SHA = '409b08c1e916cc9953a5f66e91ecae897e265c43471227af2bf1b208b566d533'
CURRENT_PREPARED_SHA = 'd48b20ced22d5040a094ce2cc4cf2d16eec03a947f8e0403e537b2ff35ad1966'
CURRENT_PID, CURRENT_TICKS = 24857, 4511636088
CURRENT_START = 1789381867367216192
CHECKOUT = '19eda11f44424f673a40b06051edcbf629b50846'
DATADIR = '/data/gtron/main/datadir'
MARKER = '/data/gtron/main/HISTORY_SHARED_CHUNKS_REQUIRES_READER_V3.json'
READER_GUARD = '/usr/local/libexec/gtron-history-shared-reader-guard.py'
READER_DROPIN = '/etc/systemd/system/gtron.service.d/zz-history-shared-reader.conf'
BLOCKS, MAX_BYTES, MAX_DECODED, MAX_ROWS = 16, 512 << 20, 1 << 30, 262144
PROBE_TIMEOUT = 120
# Independent immutable modules; no deployment-predecessor code is executed.
MODULES = {
    'builder': ('8f3b8c7d48a6e8076c81b8e49d83751945715f3d', 'scripts/inspect_history_prev_20260913.py',
                '75d2e44cd78ad26149a69614e0ac6c5557db07f7491b97eb84e0c1e55b4da4ae'),
    'helper': ('024a3b1bbc08c186e0232517f56598e2a413e57c', 'scripts/deploy_range_scheduling_20260913.py',
               'a0fcf33a7ccf09ad45dab5d8bc5f101955d36e8193d022854bf2f0568c6ff70b'),
    'systemd': ('593c7de3dd1b324c08fe6a2085691c5a71e7b86d', 'scripts/deploy_history_sharing_systemd_20260913.py',
                '3d7044409ff96e58a85313812a02f811209b72bc0f4595ffc1b41c7e6afaac90'),
    'guard': ('593c7de3dd1b324c08fe6a2085691c5a71e7b86d', 'scripts/history_shared_reader_guard.py',
              '036eb75572646f81ffdb2da1d0f270b4f3ff74ea1b573bdcff7e1d6114c52103'),
}
# Frozen review scope from the current production source to the diagnostic
# source. Documentation may be added/modified separately. Every source file,
# including unchanged files, is bound to its exact Git blob and SHA256 below.
ALLOWED_FILES = (
    'cmd/gtron/db_cmd.go', 'cmd/gtron/db_history_range.go', 'cmd/gtron/db_history_range_test.go',
    'cmd/gtron/db_history_cold_benchmark.go', 'cmd/gtron/db_history_cold_benchmark_test.go',
    'cmd/gtron/history_backlog.go', 'cmd/gtron/history_backlog_test.go', 'cmd/gtron/main.go',
    'core/history_backlog.go', 'core/history_backlog_test.go', 'core/pointread/owned.go',
    'core/rawdb/accessors_read.go', 'core/rawdb/accessors_read_ownership_cold_bench_test.go',
    'core/rawdb/accessors_read_ownership_fixture_test.go', 'core/rawdb/accessors_read_ownership_oracle_test.go',
    'core/rawdb/accessors_read_ownership_test.go', 'core/rawdb/history_range_export.go',
    'core/rawdb/history_range_export_test.go', 'core/rawdb/pebbledb/pebble.go',
    'core/rawdb/pebbledb/pebble_snapshot_owned_test.go', 'core/state/snapshots/history_stream_build.go',
    'core/state/snapshots/history_diagnostic.go', 'internal/tronapi/backend.go',
    'net/sync.go', 'net/sync_history_backlog.go', 'net/sync_history_backlog_test.go',
    'scripts/deploy_cold_recovery_20260914.py', 'scripts/verify_cold_recovery_20260914.py',
    'scripts/tests/test_deploy_cold_recovery_20260914.py', 'scripts/tests/test_verify_cold_recovery_20260914.py',
)


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def full_commit(value):
    require(isinstance(value, str) and re.fullmatch('[0-9a-f]{40}', value), 'exact 40-hex Git commit required')
    return value


def read_regular(path, limit=16 << 20, root=False):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size <= limit, 'unsafe bounded file: ' + str(path))
        if root:
            require(info.st_uid == 0 and not info.st_mode & 0o022, 'unsafe root evidence: ' + str(path))
        data = stream.read(limit + 1)
    require(len(data) <= limit, 'file exceeded limit: ' + str(path))
    return data


def git(*args):
    return subprocess.check_output(['git'] + list(args), cwd=str(REPO),
                                   stdin=subprocess.DEVNULL, timeout=60)


def resolve(revision):
    return git('rev-parse', '--verify', revision + '^{commit}').decode().strip()


def pinned_module(name):
    revision, path, expected = MODULES[name]
    require(resolve(full_commit(revision)) == revision, 'helper Git object differs')
    blob = git('show', revision + ':' + path)
    require(sha(blob) == expected, 'pinned helper bytes differ: ' + name)
    module = types.ModuleType('history_range_' + name)
    module.__file__ = __file__ if name == 'builder' else path
    exec(compile(blob, module.__file__, 'exec'), module.__dict__)
    return module


def validate_revision(h, source, ops):
    full_commit(source)
    full_commit(ops)
    require(source == SOURCE_REVISION, 'reviewed diagnostic source commit required')
    require(resolve(source) == source and resolve(ops) == ops, 'source/ops Git object differs')
    require(resolve('refs/remotes/origin/master') == ops, 'fetched master tip differs')
    require(resolve('HEAD') == CHECKOUT, 'production checkout changed')
    for base, tip in ((CURRENT_SOURCE, source), (source, ops)):
        require(h.run(['git', 'merge-base', '--is-ancestor', base, tip], cwd=REPO, check=False)[1] == 0,
                'unexpected diagnostic Git ancestry')
    dirty = git('status', '--porcelain', '--untracked-files=no').decode().splitlines()
    require(all(row[3:] == 'third_party/librustzcash' for row in dirty), 'unexpected tracked repository changes')
    # Local untracked files never enter git archive and are never removed.
    changed = set()
    for row in git('diff', '--name-status', '--no-renames', CURRENT_SOURCE, source, '--').decode().splitlines():
        fields = row.split('\t')
        require(len(fields) == 2 and fields[0] in ('A', 'M'), 'unexpected source change kind')
        name = fields[1]
        if name.startswith('docs/') and name.endswith('.md'):
            continue
        changed.add(name)
    require(changed == set(ALLOWED_FILES), 'source differs from reviewed file scope: ' + str(sorted(changed ^ set(ALLOWED_FILES))))
    for required in ('cmd/gtron/db_history_range.go', 'core/rawdb/history_range_export.go'):
        require(required in changed, 'diagnostic source does not include range capture')
    script = git('show', ops + ':' + SCRIPT_PATH)
    require(script == read_regular(__file__, 256 << 10, root=True), 'executing ops differs from pinned Git script')
    manifest, gitlinks = source_tree_manifest(source)
    return {'source_commit': source, 'script_commit': ops, 'script_sha256': sha(script),
            'base_commit': CURRENT_SOURCE, 'changed_files': sorted(changed), 'checkout': CHECKOUT,
            'manifest': manifest, 'gitlinks': gitlinks, 'modules': MODULES,
            'production_prepared_sha256': CURRENT_PREPARED_SHA}


def source_tree_manifest(source):
    full_commit(source)
    manifest, gitlinks = {}, {}
    for row in git('ls-tree', '-rz', source).split(b'\0'):
        if not row:
            continue
        metadata, name = row.split(b'\t', 1)
        mode, kind, oid = metadata.decode().split(' ')
        name = name.decode('utf-8')
        require(not Path(name).is_absolute() and '..' not in Path(name).parts, 'unsafe source tree path')
        if mode == '160000':
            require(kind == 'commit' and name == 'third_party/librustzcash', 'unreviewed submodule')
            gitlinks[name] = oid
        else:
            require(kind == 'blob' and mode in ('100644', '100755'), 'unsupported source tree entry')
            manifest[name] = {'git_blob': oid, 'mode': int(mode, 8) & 0o777}
    require(manifest and len(manifest) <= 20000 and len(gitlinks) == 1, 'unexpected source tree size/submodules')
    return manifest, gitlinks


def extract_private_source(archive, manifest):
    """Create a new private tree using Git's executable bit, not tar.umask.

    git archive commonly emits 0664/0775 for Git 100644/100755. Normalize only
    during this fresh extraction; existing trees and verifier rules stay intact.
    Every member's bytes and final mode remain bound to the pinned Git manifest.
    """
    require(not os.path.lexists(SOURCE), 'private extraction requires a new source directory')
    SOURCE.mkdir()
    seen, files = set(), set()
    with tarfile.open(str(archive)) as bundle:
        for item in bundle:
            rel = Path(item.name)
            require(not rel.is_absolute() and '..' not in rel.parts and str(rel) not in ('', '.'), 'unsafe archive path')
            name = str(rel)
            require(name not in seen, 'duplicate archive member')
            seen.add(name)
            require(len(seen) <= 40000, 'too many source archive members')
            target = SOURCE / rel
            if item.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            require(item.isfile() and name in manifest and item.size <= 64 << 20, 'unexpected source archive member: ' + name)
            entry = manifest[name]
            require(entry['mode'] in (0o644, 0o755), 'unexpected pinned Git file mode')
            with bundle.extractfile(item) as stream:
                data = stream.read((64 << 20) + 1)
            oid = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
            require(len(data) == item.size and oid == entry['git_blob'], 'source archive Git blob differs: ' + name)
            target.parent.mkdir(parents=True, exist_ok=True)
            with open(str(target), 'xb') as stream:
                stream.write(data)
                os.fchmod(stream.fileno(), entry['mode'])
            files.add(name)
    require(files == set(manifest), 'source archive omits pinned files')


def verify_source(h, manifest):
    require(isinstance(manifest, dict) and 0 < len(manifest) <= 20000, 'invalid source manifest')
    for name, entry in manifest.items():
        rel = Path(name)
        require(not rel.is_absolute() and '..' not in rel.parts and re.fullmatch('[0-9a-f]{40}', entry['git_blob']), 'invalid source manifest entry')
        target = SOURCE / rel
        require(SOURCE.resolve() in target.resolve().parents, 'source escapes private tree')
        data = read_regular(target, 64 << 20)
        oid = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        require(oid == entry['git_blob'] and stat.S_IMODE(target.stat().st_mode) == entry['mode'], 'extracted Git blob/mode differs: ' + name)
        digest = sha(data)
        require(entry.get('sha256', digest) == digest, 'prepared source SHA differs: ' + name)
        entry['sha256'] = digest
    actual = set()
    for directory, subdirs, files in os.walk(str(SOURCE), followlinks=False):
        for name in list(subdirs):
            target = Path(directory) / name
            if target.is_symlink():
                require(target == SOURCE / 'third_party/librustzcash' and target.resolve() == (REPO / 'third_party/librustzcash').resolve(), 'unexpected source symlink')
                subdirs.remove(name)
        actual.update(str((Path(directory) / name).relative_to(SOURCE)) for name in files)
    require(actual == set(manifest), 'untracked/omitted file in isolated build source')


def changed_saved(item, data, path=None):
    result = copy.deepcopy(item)
    result.update(data_b64=base64.b64encode(data).decode(), sha256=sha(data))
    if path is not None:
        result['path'] = str(path)
    return result


def saved_content(item):
    data = base64.b64decode(item['data_b64'], validate=True)
    require(sha(data) == item['sha256'], 'saved file hash differs')
    return data


def production_record(h, guard):
    """Verify a bounded attestation DATA chain, without predecessor Python code."""
    path, expected = CURRENT_RELEASE / 'upgrade-prepared.json', CURRENT_PREPARED_SHA
    top, seen = None, set()
    for unused in range(16):
        require(path not in seen and path.parent.parent == Path('/data/gtron/releases'), 'invalid predecessor attestation path')
        seen.add(path)
        data = read_regular(path, root=True)
        require(sha(data) == expected, 'production prepare attestation changed: ' + str(path))
        record = json.loads(data.decode())
        require(record.get('prepared') is True, 'predecessor was not prepared')
        source, checksum = full_commit(record.get('source_commit')), record.get('binary_sha256', '')
        require(re.fullmatch('[0-9a-f]{64}', checksum), 'bad attested binary SHA')
        marker = guard.reader_marker(path.parent / 'gtron', checksum, source)
        permanent = h.saved_file(path.parent / 'reader-required.json')
        require(json.loads(saved_content(permanent)) == marker, 'permanent reader identity changed')
        require(permanent['uid'] == 0 and not permanent['mode'] & 0o022, 'unsafe permanent reader marker')
        if top is None:
            top = record
            require(source == CURRENT_SOURCE and checksum == CURRENT_SHA and record.get('upgrade_prepared') is True, 'current production identity differs')
            require(h.file_sha(CURRENT_RELEASE / 'prepared.json') == record['builder_record_sha256'], 'production builder record changed')
            require(h.file_sha(CURRENT_RELEASE / 'native-compatible-history-tests.log') == record['history_tests_sha256'], 'production native test evidence changed')
            require(read_regular(CURRENT_RELEASE / 'source-commit').decode() == CURRENT_SOURCE + '\n' and
                    read_regular(CURRENT_RELEASE / 'SHA256SUMS').decode() == CURRENT_SHA + '  gtron\n', 'production release markers changed')
            global_marker = h.saved_file(MARKER)
            require(global_marker == changed_saved(record['old_marker'], (json.dumps(marker, sort_keys=True) + '\n').encode()), 'current data reader marker changed')
            require(permanent == changed_saved(record['old_marker'], saved_content(global_marker), permanent['path']), 'current permanent reader metadata changed')
            require(h.saved_file(READER_GUARD) == record['reader_guard'], 'production reader guard changed')
        if record.get('shared_prepared') is True:
            require(source == 'd656be3c43eb237fac4d46a8847e5fe551bc6a78', 'unexpected attestation root')
            return top
        require(record.get('upgrade_prepared') is True, 'unknown reader attestation format')
        old = record['old_armed']
        require(h.saved_file(old['path']) == old, 'ancestor permanent reader marker changed')
        parent = Path(old['path'])
        require(parent.name == 'reader-required.json' and parent.parent.parent == Path('/data/gtron/releases'), 'unexpected ancestor marker path')
        identity = json.loads(saved_content(old))
        filename = 'shared-prepared.json' if identity['source_commit'] == 'd656be3c43eb237fac4d46a8847e5fe551bc6a78' else 'upgrade-prepared.json'
        path, expected = parent.parent / filename, record['old_prepared_sha256']
        require(re.fullmatch('[0-9a-f]{64}', expected), 'invalid ancestor record SHA')
    raise RuntimeError('production attestation depth exceeds bound')


def production_configuration(record):
    """Derive the already-deployed candidate bytes from the pinned prepare data."""
    result = copy.deepcopy(record['current'])
    old = json.loads(saved_content(record['old_marker']))
    old_exe, old_sha, old_source = old['binary'], old['binary_sha256'], old['source_commit']
    require(result['process']['exe'] == old_exe, 'old production config identity differs')
    replacements = ((old_exe, CURRENT_EXE), (old_sha, CURRENT_SHA), (old_source, CURRENT_SOURCE))
    main_edits, guard_edits = 0, 0
    for index, item in enumerate(result['main']['files']):
        data = saved_content(item)
        if item['path'] == READER_DROPIN:
            for before, after in replacements:
                require(data.count(before.encode()) == 1, 'ambiguous reader guard identity')
                data = data.replace(before.encode(), after.encode())
            guard_edits += 1
        elif old_exe.encode() in data:
            require(data.count(old_exe.encode()) == 1, 'ambiguous service executable')
            data = data.replace(old_exe.encode(), CURRENT_EXE.encode())
            main_edits += 1
        result['main']['files'][index] = changed_saved(item, data)
    require(main_edits == 1 and guard_edits == 1, 'missing exact production config edits')
    props = result['main']['properties']
    require(props['ExecStart'].count(old_exe) == 2, 'unexpected production ExecStart shape')
    props['ExecStart'] = props['ExecStart'].replace(old_exe, CURRENT_EXE)
    for before, after in replacements:
        require(props['ExecStartPre'].count(before) == 1, 'ambiguous effective reader guard')
        props['ExecStartPre'] = props['ExecStartPre'].replace(before, after)
    return result


def probe_argv(args):
    require(args.from_block is None or type(args.from_block) is int and 0 <= args.from_block <= (1 << 64) - BLOCKS, 'invalid capture start height')
    argv = [str(BINARY), 'db', 'export-history-range', '--datadir', DATADIR,
            '--db.cache', '64', '--db.handles', '128', '--blocks', str(BLOCKS),
            '--max-bytes', str(MAX_BYTES), '--max-decoded-bytes', str(MAX_DECODED),
            '--max-rows', str(MAX_ROWS), '--max-duration', '60s', '--output-dir', str(COPY)]
    if args.from_block is not None:
        argv.extend(['--from-block', str(args.from_block)])
    return argv


def load_modules():
    i, h, wrapper, guard = (pinned_module(name) for name in ('builder', 'helper', 'systemd', 'guard'))
    i.REPO, i.RELEASE, i.SOURCE, i.BINARY = REPO, RELEASE, SOURCE, BINARY
    i.RESULT, i.PACKS = RESULT, COPY
    i.CURRENT_SOURCE, i.CURRENT_EXE, i.CURRENT_SHA = CURRENT_SOURCE, CURRENT_EXE, CURRENT_SHA
    i.CURRENT_PID, i.CURRENT_TICKS = CURRENT_PID, CURRENT_TICKS
    i.SCRIPT_PATH, i.CHECKOUT, i.ALLOWED_FILES = SCRIPT_PATH, CHECKOUT, ALLOWED_FILES
    i.PROBE_TIMEOUT, i.probe_argv = PROBE_TIMEOUT, probe_argv
    i.NATIVE_TEST_PATTERN = 'Test.*(HistoryRangeExport|StateHistoryRangeExport|HistoryColdBenchmark|HistoryDiagnostic|Owned)'
    i.validate_revision, i.verify_source = validate_revision, verify_source
    i.extract = lambda archive: extract_private_source(archive, source_tree_manifest(SOURCE_REVISION)[0])
    i.normalize_exec_start = guard.parse_commands
    h.REPO, h.RELEASE, h.BINARY = REPO, RELEASE, Path(CURRENT_EXE)
    h.OLD_EXE, h.OLD_SHA, h.OLD_PID, h.OLD_START_TICKS = CURRENT_EXE, CURRENT_SHA, CURRENT_PID, CURRENT_TICKS
    def enabled(exe):
        require(exe == CURRENT_EXE, 'unreviewed service binary')
        return '1'
    h.expected_history_range_environment = h.expected_history_queue_environment = enabled
    wrapper.install_show(h, guard)
    old_snapshot, old_check, old_verify = i.snapshot, i.check_configuration, i.verify_prepared
    def check_production(now):
        record = production_record(h, guard)
        i.same_configuration(h, production_configuration(record), now)
        marker = guard.reader_marker(CURRENT_EXE, CURRENT_SHA, CURRENT_SOURCE)
        require(guard.check_command(now['main']['properties']['ExecStart'], CURRENT_EXE, CURRENT_SHA, CURRENT_SOURCE, marker), 'shared writer unexpectedly disabled')
    def snapshot(helper, initial=True):
        value = old_snapshot(helper, initial)
        check_production(value)
        runtime = h.verify_observer_runtime(value['process']['pid'], CURRENT_EXE)
        if initial:
            require(runtime['process_start_unix_nano'] == CURRENT_START, 'pinned process metric identity changed')
        value['runtime'] = runtime
        return value
    def check(helper, before):
        now = old_check(helper, before)
        check_production(now)
        return now
    def verify(helper, record):
        old_verify(helper, record)
        require(record.get('production_prepared_sha256') == CURRENT_PREPARED_SHA and record.get('modules') == json.loads(json.dumps(MODULES)), 'diagnostic helper/production attestation differs')
        require(resolve(full_commit(record['source_commit'])) == record['source_commit'] and
                resolve(full_commit(record['script_commit'])) == record['script_commit'], 'prepared Git objects unavailable')
        require(sha(git('show', record['script_commit'] + ':' + SCRIPT_PATH)) == record['script_sha256'], 'prepared ops Git blob differs')
        require(h.file_sha(SOURCE / 'third_party/librustzcash/target/release/librustzcash.a') == record['rust_sha256'], 'prepared native Sapling library changed')
    i.snapshot, i.check_configuration, i.verify_prepared = snapshot, check, verify
    return i, h


def prepare_locked(i, h, args):
    result = i.prepare(h, types.SimpleNamespace(revision=args.source_revision, script_revision=args.script_revision))
    if (RELEASE / 'range-prepared.json').exists():
        record = json.loads(read_regular(RELEASE / 'range-prepared.json', root=True))
        verify_range_prepared(i, h, record, args)
        return dict(result, already_prepared=True, prepared_sha256=h.file_sha(RELEASE / 'range-prepared.json'))
    env = dict(os.environ, CGO_ENABLED='1', GOMAXPROCS='2', GOFLAGS='', GOTOOLCHAIN='local')
    env.pop('GOROOT', None)
    for key in (h.CACHE_OBSERVER_ENV, h.OBSERVER_ENV, h.PRUNE_ENV, h.HISTORY_RANGE_ENV, h.HISTORY_QUEUE_ENV):
        env.pop(key, None)
    # Full rawdb and Pebble packages ran in the reused builder. These two full
    # packages add native replay/equivalence/context and CLI coverage.
    h.run([i.GO, 'test', '-p', '2', '-tags', 'sapling', './core/state/snapshots', './cmd/gtron',
           '-count=1', '-timeout=300s'], cwd=SOURCE, env=env, timeout=1800, log='native-replay-tests.log')
    # The original builder also checks the older diagnostics; require this new
    # CLI explicitly before an operator can obtain a capture-admission SHA.
    text, _ = h.run([str(BINARY), 'db', 'export-history-range', '--help'], log='range-help.txt')
    for flag in ('--output-dir', '--blocks', '--max-bytes', '--max-decoded-bytes', '--max-rows', '--max-duration'):
        require(flag in text, 'physical export CLI missing: ' + flag)
    text, _ = h.run([str(BINARY), 'db', 'benchmark-history-cold', '--help'], log='cold-benchmark-help.txt')
    for flag in ('--input-dir', '--output-dir', '--compression-format', '--copy-mode', '--max-duration'):
        require(flag in text, 'cold replay CLI missing: ' + flag)
    record = h.load_json('prepared.json')
    i.verify_prepared(h, record)
    record['range_cli_verified'] = True
    record['range_builder_record_sha256'] = h.file_sha(RELEASE / 'prepared.json')
    record['native_replay_tests_sha256'] = h.file_sha(RELEASE / 'native-replay-tests.log')
    # Recheck identity and all configuration after the additional full tests.
    after = i.snapshot(h)
    i.same_configuration(h, record['preflight'], after)
    verify_source(h, record['manifest'])
    h.save_json('range-prepared.json', record)
    return dict(result, prepared_sha256=h.file_sha(RELEASE / 'range-prepared.json'))


def prepare(i, h, args):
    validate_revision(h, args.source_revision, args.script_revision)
    require(not RELEASE.is_symlink(), 'diagnostic release is a symlink')
    RELEASE.mkdir(parents=True, exist_ok=True, mode=0o755)
    with open(str(RELEASE / '.range-prepare.lock'), 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return prepare_locked(i, h, args)


def verify_range_prepared(i, h, record, args):
    require(record.get('range_cli_verified') is True and record['source_commit'] == args.source_revision and record['script_commit'] == args.script_revision, 'range prepare/source identity differs')
    i.verify_prepared(h, record)
    require(h.file_sha(RELEASE / 'prepared.json') == record['range_builder_record_sha256'], 'range builder record changed')
    require(h.file_sha(RELEASE / 'native-replay-tests.log') == record['native_replay_tests_sha256'], 'native replay test evidence changed')


def live_ops(i, h, args):
    class LiveOps(i.LiveOps):
        def before(self):
            validate_revision(h, args.source_revision, args.script_revision)
            require(h.file_sha(RELEASE / 'range-prepared.json') == args.prepared_sha, 'reviewed range prepare SHA differs')
            record = json.loads(read_regular(RELEASE / 'range-prepared.json', root=True))
            verify_range_prepared(i, h, record, args)
            # Base before() checks its original builder record separately.
            original_sha = args.prepared_sha
            args.prepared_sha = record['range_builder_record_sha256']
            try:
                return super().before()
            finally:
                args.prepared_sha = original_sha

        def probe(self):
            command = probe_argv(args)
            require(not os.path.lexists(COPY), 'capture directory is not fresh')
            account = pwd.getpwnam('java-tron')
            def drop_user():
                os.initgroups(account.pw_name, account.pw_gid)
                os.setgid(account.pw_gid)
                os.setuid(account.pw_uid)
                os.umask(0o077)
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            env = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'LANG': 'C', 'HOME': account.pw_dir, 'GOMAXPROCS': '2'}
            started = time.time()
            stdout_path, stderr_path = RELEASE / 'inspection-stdout.json', RELEASE / 'inspection-stderr.log'
            with os.fdopen(os.open(str(stdout_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as out, \
                    os.fdopen(os.open(str(stderr_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as err:
                previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, (signal.SIGINT, signal.SIGTERM, signal.SIGHUP))
                try:
                    self.child = subprocess.Popen(command, cwd='/data/gtron/main', env=env, stdin=subprocess.DEVNULL,
                                                  stdout=out, stderr=err, start_new_session=True, preexec_fn=drop_user)
                finally:
                    signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                try:
                    h.save_json('inspection-child.json', {'pid': self.child.pid, 'argv': command, 'started_at': started})
                    code = self.child.wait(timeout=PROBE_TIMEOUT)
                finally:
                    self.cancel_probe()
            value = json.loads(read_regular(stdout_path, 64 << 20).decode())
            detail = value.get('export', {})
            first, last = detail.get('from_block'), detail.get('to_block')
            valid_range = type(first) is int and type(last) is int and last - first == BLOCKS - 1 and first >= 0 and (args.from_block is None or first == args.from_block)
            ok = code == 0 and valid_range and detail.get('complete') is True and detail.get('stop_reason') == 'complete' and detail.get('content_verified') is False and detail.get('blocks') == BLOCKS
            return {'ok': ok, 'returncode': code, 'complete': detail.get('complete'), 'content_verified': detail.get('content_verified'),
                    'from_block': detail.get('from_block'), 'to_block': detail.get('to_block'),
                    'copy_directory': str(COPY), 'physical_manifest_sha256': detail.get('manifest_sha256'),
                    'started_at': started, 'finished_at': time.time(), 'stdout_sha256': h.file_sha(stdout_path)}
    return LiveOps(h, args)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('prepare', 'inspect'))
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--script-revision', required=True)
    parser.add_argument('--prepared-sha')
    parser.add_argument('--from-block', type=int, help='omit to select verified cold watermark + 1 at capture time')
    args = parser.parse_args()
    full_commit(args.source_revision)
    full_commit(args.script_revision)
    require(os.geteuid() == 0, 'explicit root execution required')
    args.export_packs = True  # Reused before() makes only result parent writable by service user.
    probe_argv(args)
    i, h = load_modules()
    if args.mode == 'prepare':
        result = prepare(i, h, args)
    else:
        require(re.fullmatch('[0-9a-f]{64}', args.prepared_sha or ''), 'reviewed range prepare SHA required')
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, i.interrupted)
        with open('/data/gtron/start.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = i.inspect_transaction(live_ops(i, h, args))
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result.get('prepared') or result.get('ok') else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(json.dumps({'ok': False, 'error': repr(error)}), file=sys.stderr, flush=True)
        sys.exit(1)
