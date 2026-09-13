#!/usr/bin/env python3
"""Pinned offline diagnostic, Python 3.6+: prepare, then explicit inspect.

Builds a separate native gtron; never edits a unit, production executable or hold.
Only inspect stops/starts gtron. Its finally restores the same service after a
bounded read-only child. Run under a durable operator session; SIGKILL or host
failure cannot be recovered by a Python finally. No automatic retry of inspect.
"""
import argparse
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

RELEASE = Path('/data/gtron/releases/20260913-history-prev')
SOURCE = RELEASE / 'source'
BINARY = RELEASE / 'gtron-inspect'
RESULT = RELEASE / 'result'
PACKS = RESULT / 'packs'
HELPER = Path('/tmp/deploy-range-024a3b1b.py')
HELPER_SHA = 'a0fcf33a7ccf09ad45dab5d8bc5f101955d36e8193d022854bf2f0568c6ff70b'
REPO = Path('/data/gtron/go-tron')
CURRENT_SOURCE = 'fdfe853236445ca77c2d7a9553c1d4e172ff9a43'
CURRENT_EXE = '/data/gtron/releases/20260913-range-scheduling/gtron'
CURRENT_SHA = 'f6a61e0d0f3c1426ab2d7849991878ff9639168791fbdb940d08e628432460f1'
# Native fresh preflight verified on 2026-09-13; unchanged service identity.
CURRENT_PID = 4345
CURRENT_TICKS = 4499806875
CHECKOUT = '19eda11f44424f673a40b06051edcbf629b50846'
SCRIPT_PATH = 'scripts/inspect_history_prev_20260913.py'
# Freeze scope after review; exact source/ops commits are mandatory runtime pins.
# Hash every Git blob at that source, including this script if source == ops.
ALLOWED_FILES = (
    'cmd/gtron/db_cmd.go',
    'cmd/gtron/db_history_codec_benchmark.go',
    'cmd/gtron/db_history_codec_benchmark_test.go',
    'cmd/gtron/db_history_pack_export.go',
    'cmd/gtron/db_inspect_history.go',
    'cmd/gtron/db_inspect_history_test.go',
    'core/rawdb/history_codec_benchmark.go',
    'core/rawdb/history_codec_benchmark_cpu_other.go',
    'core/rawdb/history_codec_benchmark_cpu_unix.go',
    'core/rawdb/history_codec_benchmark_test.go',
    'core/rawdb/inspect_state_history.go',
    'core/rawdb/inspect_state_history_test.go',
    'core/rawdb/pebbledb/pebble.go',
    'scripts/deploy_range_scheduling_20260913.py',
    'scripts/inspect_history_prev_20260913.py',
    'scripts/tests/test_inspect_history_prev_20260913.py',
)
INSPECT_APPROVED = True
EXPORT_APPROVED = True
DATADIR = '/data/gtron/main/datadir'
SERVICE = 'gtron.service'
SYSTEMCTL = '/bin/systemctl'
GO = '/data/go/bin/go'
STOP_TIMEOUT = 660
PROBE_TIMEOUT = 180
MAX_JSON_BYTES = 64 * 1024 * 1024
SAMPLES = 128
ENCODED_BYTES = 256 * 1024 * 1024
DECODED_BYTES = 1024 * 1024 * 1024
ROWS = 2000000
NATIVE_TEST_PATTERN = 'Test.*(HistoryPrev|InspectHistoryPrev|InspectStateHistory|HistoryCodecBenchmark)'


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def load_helper():
    fd = os.open(str(HELPER), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), 'helper is not regular')
        data = stream.read(256 * 1024)
    require(sha(data) == HELPER_SHA, 'read-only helper SHA differs')
    module = types.ModuleType('pinned_range_readonly')
    module.__file__ = str(HELPER)
    exec(compile(data, str(HELPER), 'exec'), module.__dict__)
    # Redirect only generic log output, never its BINARY/OLD_* dispatch rules.
    module.RELEASE = RELEASE
    require(str(module.BINARY) == CURRENT_EXE, 'helper current release mismatch')
    return module


def frozen():
    require(INSPECT_APPROVED is True, 'diagnostic safety review is not frozen')
    require(ALLOWED_FILES and len(set(ALLOWED_FILES)) == len(ALLOWED_FILES),
            'diagnostic allowed source paths are not frozen')
    require(CURRENT_PID > 0 and CURRENT_TICKS > 0, 'fresh process pins required')


def resolve(h, ref):
    value, _ = h.run(['git', 'rev-parse', '--verify', ref + '^{commit}'], cwd=REPO)
    value = value.strip()
    require(re.fullmatch('[0-9a-f]{40}', value) is not None, 'invalid commit: ' + ref)
    return value


def validate_revision(h, revision, ops):
    frozen()
    require(re.fullmatch('[0-9a-f]{40}', revision or '') is not None, 'exact source SHA required')
    require(re.fullmatch('[0-9a-f]{40}', ops or '') is not None, 'exact ops SHA required')
    require(resolve(h, revision) == revision and resolve(h, ops) == ops, 'commit mismatch')
    require(resolve(h, 'refs/remotes/origin/master') == ops, 'fetched master tip differs')
    for base, tip in ((CURRENT_SOURCE, revision), (revision, ops)):
        require(h.run(['git', 'merge-base', '--is-ancestor', base, tip], cwd=REPO,
                      check=False)[1] == 0, 'unexpected Git ancestry')
    require(resolve(h, 'HEAD') == CHECKOUT, 'production checkout changed')
    dirty, _ = h.run(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=REPO)
    require(all(line[3:] == 'third_party/librustzcash' for line in dirty.splitlines()),
            'unexpected tracked changes')
    changes, _ = h.run(['git', 'diff', '--name-status', '--no-renames',
                        CURRENT_SOURCE, revision, '--'], cwd=REPO)
    actual = set()
    for row in changes.splitlines():
        parts = row.split('\t')
        require(len(parts) == 2 and parts[0] in ('A', 'M'), 'unexpected source diff')
        if parts[1].startswith('docs/') and parts[1].endswith('.md'):
            continue
        actual.add(parts[1])
    require(actual == set(ALLOWED_FILES), 'source change list differs from allowed scope')
    manifest = {}
    for path in sorted(actual):
        blob = subprocess.check_output(['git', 'show', revision + ':' + path],
                                       cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)
        manifest[path] = sha(blob)
    script = subprocess.check_output(['git', 'show', ops + ':' + SCRIPT_PATH],
                                    cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)
    require(sha(script) == h.file_sha(__file__), 'executing ops script differs from Git')
    return {'source_commit': revision, 'script_commit': ops,
            'script_sha256': sha(script), 'base_commit': CURRENT_SOURCE,
            'changed_files': sorted(actual), 'manifest': manifest, 'checkout': CHECKOUT}


def verify_source(h, manifest):
    require(set(manifest) == set(ALLOWED_FILES), 'prepared source scope differs')
    for path, expected in manifest.items():
        require(re.fullmatch('[0-9a-f]{64}', expected) is not None, 'invalid source hash')
        target = SOURCE / path
        require(target.is_file() and not target.is_symlink(), 'source missing: ' + path)
        require(h.file_sha(target) == expected, 'extracted source differs: ' + path)


def snapshot(h, initial=True):
    out = h.preflight(CURRENT_PID if initial else None, CURRENT_EXE, CURRENT_SHA)
    if initial:
        require(out['process']['start_ticks'] == CURRENT_TICKS, 'current PID was reused')
    p = out['main']['properties']
    require(p.get('User') == 'java-tron' and p.get('WorkingDirectory') == '/data/gtron/main',
            'unexpected service user or working directory')
    require(p.get('TimeoutStopUSec') in ('10min', '10min 0s', '600000000'),
            'unexpected stop timeout; review the bounded stop budget')
    require(h.file_sha(CURRENT_EXE) == CURRENT_SHA, 'production file checksum differs')
    require((Path(CURRENT_EXE).parent / 'source-commit').read_text().strip() == CURRENT_SOURCE,
            'production release source differs')
    return out


def normalize_exec_start(value):
    """Parse the observed systemd single-command form, excluding runtime state."""
    require(isinstance(value, str) and len(value) <= 64 * 1024,
            'invalid ExecStart property')
    match = re.fullmatch(r'\{\s*(.*?)\s*\}', value)
    require(match is not None, 'unknown ExecStart serialization')
    body = match.group(1)
    require('{' not in body and '}' not in body, 'multiple or nested ExecStart commands')
    fields = {}
    for part in re.split(r'\s*;\s*', body):
        key, separator, field = part.partition('=')
        key, field = key.strip(), field.strip()
        require(separator and key not in fields and field, 'malformed or duplicate ExecStart field')
        fields[key] = field
    require(set(fields) == {'path', 'argv[]', 'ignore_errors', 'start_time',
                            'stop_time', 'pid', 'code', 'status'},
            'unknown or missing ExecStart fields')
    require(fields['path'].startswith('/') and '\n' not in fields['path'] and
            (fields['argv[]'] == fields['path'] or fields['argv[]'].startswith(fields['path'] + ' ')),
            'invalid ExecStart path/argv serialization')
    require(fields['ignore_errors'] in ('yes', 'no'), 'unknown ExecStart ignore_errors value')
    require(fields['pid'].isdigit(), 'invalid ExecStart runtime pid')
    for name in ('start_time', 'stop_time'):
        require(fields[name].startswith('[') and fields[name].endswith(']'),
                'unknown ExecStart time serialization')
    require(re.fullmatch(r'(\(null\)|[a-zA-Z_]+|[0-9]+)', fields['code']) is not None and
            re.fullmatch(r'[0-9]+(?:/[0-9]+)?', fields['status']) is not None,
            'unknown ExecStart exit serialization')
    return {name: fields[name] for name in ('path', 'argv[]', 'ignore_errors')}


def same_configuration(h, before, after):
    # Keep full unit bytes and all previous configuration checks. Only the
    # embedded process-status fields of startup commands are not configuration.
    normalized = []
    for original in (before, after):
        item = copy.deepcopy(original)
        properties = item['main']['properties']
        for name in ('ExecStart', 'ExecStartPre'):
            value = properties.get(name)
            if name == 'ExecStartPre' and value == '':
                continue  # No configured pre-start command.
            properties[name] = json.dumps(normalize_exec_start(value), sort_keys=True)
        normalized.append(item)
    h.same_configuration(normalized[0], normalized[1])


def check_configuration(h, before):
    now = {'main': h.unit_snapshot(SERVICE),
           'others': {u: h.unit_snapshot(u) for u in h.PRESERVED_UNITS},
           'holds': {p: h.hold_snapshot(p) for p in (h.GLOBAL_HOLD, h.MAIN_HOLD)},
           'guard_config': h.saved_file(h.GUARD_CONFIG), 'guard_script': h.saved_file(h.GUARD)}
    same_configuration(h, before, now)
    h.guard_check()
    require(h.file_sha(CURRENT_EXE) == CURRENT_SHA, 'production binary changed')
    return now


def extract(archive):
    SOURCE.mkdir()
    with tarfile.open(str(archive)) as bundle:
        for item in bundle:
            rel = Path(item.name)
            require(not rel.is_absolute() and '..' not in rel.parts, 'unsafe archive path')
            target = SOURCE / rel
            if item.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                require(item.isfile(), 'unsupported archive member: ' + item.name)
                target.parent.mkdir(parents=True, exist_ok=True)
                with bundle.extractfile(item) as stream:
                    target.write_bytes(stream.read())
                os.chmod(str(target), item.mode & 0o777)


def verify_prepared(h, record):
    frozen()
    require(record.get('prepared') is True and
            re.fullmatch('[0-9a-f]{40}', record.get('source_commit', '')) is not None,
            'successful pinned prepare record required')
    require(record.get('script_sha256') == h.file_sha(__file__) and
            record.get('helper_sha256') == HELPER_SHA, 'prepared script identity differs')
    require(BINARY.is_file() and not BINARY.is_symlink(), 'diagnostic binary missing')
    require(h.file_sha(BINARY) == record['binary_sha256'], 'diagnostic binary checksum differs')
    verify_source(h, record['manifest'])


def prepare(h, args):
    admission = validate_revision(h, args.revision, args.script_revision)
    require(not RELEASE.is_symlink(), 'release must not be a symlink')
    RELEASE.mkdir(mode=0o755, parents=True, exist_ok=True)
    with open(str(RELEASE / '.prepare.lock'), 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (RELEASE / 'prepared.json').exists():
            record = h.load_json('prepared.json')
            verify_prepared(h, record)
            require(record.get('script_commit') == args.script_revision and
                    record.get('source_commit') == args.revision, 'source/ops revision changed')
            snapshot(h)
            return {'prepared': True, 'already_prepared': True,
                    'prepared_sha256': h.file_sha(RELEASE / 'prepared.json')}
        require(not SOURCE.exists() and not BINARY.exists(), 'incomplete release exists; review it')
        before = snapshot(h)
        h.save_json('before-build.json', before)
        env = dict(os.environ, CGO_ENABLED='1', GOMAXPROCS='2', GOFLAGS='', GOTOOLCHAIN='local')
        env.pop('GOROOT', None)
        for key in (h.CACHE_OBSERVER_ENV, h.OBSERVER_ENV, h.PRUNE_ENV,
                    h.HISTORY_RANGE_ENV, h.HISTORY_QUEUE_ENV):
            env.pop(key, None)
        version, _ = h.run([GO, 'version'], env=env)
        require(version.strip() == 'go version go1.25.5 linux/amd64', 'unexpected native Go')
        archive = RELEASE / 'source.tar'
        h.run(['git', 'archive', '--format=tar', '--output=' + str(archive), args.revision], cwd=REPO)
        extract(archive)
        verify_source(h, admission['manifest'])
        rust = REPO / 'third_party/librustzcash'
        library = rust / 'target/release/librustzcash.a'
        rust_sha = h.file_sha(library)
        link = SOURCE / 'third_party/librustzcash'
        if link.exists():
            require(link.is_dir() and not any(link.iterdir()), 'archived rust is nonempty')
            link.rmdir()
        link.symlink_to(rust, target_is_directory=True)
        h.run([GO, 'test', '-p', '2', '-tags', 'sapling', './core/rawdb', './core/rawdb/pebbledb',
               '-count=1', '-timeout=300s'], cwd=SOURCE, env=env, timeout=1800,
              log='native-storage-tests.log')
        h.run([GO, 'test', '-p', '2', '-tags', 'sapling', './cmd/gtron', './core/rawdb',
               '-run', NATIVE_TEST_PATTERN, '-count=1', '-timeout=300s'],
              cwd=SOURCE, env=env, timeout=1200, log='native-inspection-tests.log')
        probe = RELEASE / 'native-sapling-probe.go'
        probe.write_text('package main\nimport("fmt";"os";"github.com/tronprotocol/go-tron/core/zksnark")\n'
                         'func main(){if !zksnark.Available(){os.Exit(2)};'
                         'v,e:=zksnark.Uncommitted();if e!=nil{panic(e)};'
                         'fmt.Printf("nativeSapling=true uncommitted=%x\\n",v)}\n')
        h.run([GO, 'run', '-p', '2', '-tags', 'sapling', str(probe)], cwd=SOURCE, env=env,
              timeout=600, log='native-sapling-probe.log')
        require('nativeSapling=true' in (RELEASE / 'native-sapling-probe.log').read_text(),
                'Sapling attestation missing')
        h.run([GO, 'build', '-p', '2', '-tags', 'sapling', '-o', str(BINARY), './cmd/gtron'],
              cwd=SOURCE, env=env, timeout=1800, log='build.log')
        os.chmod(str(BINARY), 0o755)
        info, _ = h.run([GO, 'version', '-m', str(BINARY)], env=env, log='build-info.txt')
        require(all(x in info for x in ('CGO_ENABLED=1', '-tags=sapling', 'GOARCH=amd64', 'GOOS=linux')),
                'diagnostic build settings mismatch')
        help_text, _ = h.run([str(BINARY), 'db', 'inspect-history-prev', '--help'], log='inspect-help.txt')
        for flag in ('--from-block', '--to-block', '--samples', '--seed', '--max-duration',
                     '--max-encoded-bytes', '--max-decoded-bytes', '--max-rows'):
            require(flag in help_text, 'inspection CLI contract missing: ' + flag)
        if EXPORT_APPROVED:
            require('--export-packs' in help_text, 'export CLI contract missing')
        benchmark_help, _ = h.run([str(BINARY), 'db', 'benchmark-history-codecs', '--help'],
                                  log='benchmark-help.txt')
        require('--export-packs' in benchmark_help and '--max-duration' in benchmark_help,
                'offline codec benchmark CLI contract missing')
        verify_source(h, admission['manifest'])
        after = snapshot(h)
        same_configuration(h, before, after)
        require(h.file_sha(library) == rust_sha, 'native Sapling library changed')
        record = dict(admission, prepared=True, prepared_at=time.time(),
                      helper_sha256=HELPER_SHA, binary_sha256=h.file_sha(BINARY),
                      archive_sha256=h.file_sha(archive), rust_sha256=rust_sha,
                      preflight=before, after_build=after)
        h.save_json('prepared.json', record)
        return {'prepared': True, 'binary_sha256': record['binary_sha256'],
                'prepared_sha256': h.file_sha(RELEASE / 'prepared.json')}


def probe_argv(args):
    require(0 <= args.from_block <= args.to_block < (1 << 64), 'invalid inclusive block interval')
    command = [str(BINARY), 'db', 'inspect-history-prev', '--datadir', DATADIR,
               '--db.cache', '64', '--db.handles', '128', '--from-block', str(args.from_block),
               '--to-block', str(args.to_block), '--seed', '20260913', '--samples', str(SAMPLES),
               '--max-encoded-bytes', str(ENCODED_BYTES), '--max-decoded-bytes', str(DECODED_BYTES),
               '--max-rows', str(ROWS), '--max-duration', '60s']
    if args.export_packs:
        require(EXPORT_APPROVED is True, 'pack export contract not frozen')
        command.extend(['--export-packs', str(PACKS)])
    return command


class LiveOps:
    def __init__(self, h, args):
        self.h, self.args, self.child = h, args, None
        self.stop_deadline = None

    def before(self):
        record = self.h.load_json('prepared.json')
        verify_prepared(self.h, record)
        require(self.h.file_sha(RELEASE / 'prepared.json') == self.args.prepared_sha,
                'prepared record differs from operator-reviewed SHA')
        before = snapshot(self.h)
        same_configuration(self.h, record['preflight'], before)
        require(not RESULT.exists(), 'inspection already attempted; do not overwrite evidence')
        RESULT.mkdir(mode=0o700)
        if self.args.export_packs:
            account = pwd.getpwnam('java-tron')
            os.chown(str(RESULT), account.pw_uid, account.pw_gid)
        self.h.save_json('inspection-before.json', before)
        return before

    def save(self, report):
        self.h.save_json('inspection-state.json', report)

    def stop(self):
        self.stop_deadline = time.monotonic() + STOP_TIMEOUT
        self.h.run([SYSTEMCTL, 'stop', SERVICE], timeout=STOP_TIMEOUT, log='inspect-stop.log')

    def assert_stopped(self):
        p = self.h.show(SERVICE)
        require(p.get('ActiveState') == 'inactive' and int(p.get('MainPID', '0')) == 0,
                'service has not fully stopped')
        require(not Path('/proc/{0}'.format(CURRENT_PID)).exists(), 'old PID still exists')

    def cancel_probe(self):
        child = self.child
        if child is None:
            return
        if child.poll() is None:
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass  # Natural exit raced with signalling; still wait/reap.
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait(timeout=10)
        self.child = None

    def probe(self):
        command = probe_argv(self.args)
        account = pwd.getpwnam('java-tron')
        def drop_user():
            os.initgroups(account.pw_name, account.pw_gid)
            os.setgid(account.pw_gid)
            os.setuid(account.pw_uid)
            os.umask(0o077)
            # The child must not inherit the brief parent fork-assignment mask.
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        env = {'PATH': '/data/go/bin:/usr/local/bin:/usr/bin:/bin', 'LANG': 'C',
               'HOME': account.pw_dir, 'GOMAXPROCS': '2'}
        started = time.time()
        stdout_path = RELEASE / 'inspection-stdout.json'
        stderr_path = RELEASE / 'inspection-stderr.log'
        with os.fdopen(os.open(str(stdout_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as out, \
                os.fdopen(os.open(str(stderr_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as err:
            # Do not lose the child handle if a signal arrives between fork and
            # assignment. Pending signals are delivered after self.child is set.
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK,
                                                   (signal.SIGINT, signal.SIGTERM, signal.SIGHUP))
            try:
                self.child = subprocess.Popen(command, cwd='/data/gtron/main', env=env,
                                              stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                              start_new_session=True, preexec_fn=drop_user)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            self.h.save_json('inspection-child.json', {'pid': self.child.pid, 'pgid': self.child.pid,
                                                       'argv': command, 'started_at': started})
            try:
                code = self.child.wait(timeout=PROBE_TIMEOUT)
            finally:
                self.cancel_probe()
        require(stdout_path.stat().st_size <= MAX_JSON_BYTES, 'diagnostic JSON too large')
        with open(str(stdout_path)) as stream:
            value = json.load(stream)
        detail = value.get('inspection', {})
        return {'ok': code == 0 and detail.get('complete') is True and detail.get('stop_reason') == 'complete',
                'returncode': code, 'complete': detail.get('complete'),
                'stop_reason': detail.get('stop_reason'), 'started_at': started,
                'finished_at': time.time(), 'stdout_sha256': self.h.file_sha(stdout_path)}

    def restore(self, before):
        check_configuration(self.h, before)
        p = self.h.show(SERVICE)
        if p.get('ActiveState') == 'deactivating':
            # An interrupted systemctl client does not cancel the stop job.
            # Use the remaining original shutdown allowance, not a fresh budget.
            deadline = self.stop_deadline or time.monotonic()
            while p.get('ActiveState') == 'deactivating' and time.monotonic() < deadline:
                time.sleep(2)
                p = self.h.show(SERVICE)
        pid = int(p.get('MainPID', '0'))
        if pid:
            identity = self.h.process_identity(pid)
            require(p.get('ActiveState') == 'active' and
                    (pid, identity['start_ticks']) == (CURRENT_PID, CURRENT_TICKS),
                    'another process appeared; refusing to override it')
        else:
            require(p.get('ActiveState') in ('inactive', 'failed'), 'stop has not settled; no concurrent start')
            self.h.run([SYSTEMCTL, 'start', SERVICE], timeout=180, log='inspect-start.log')
        healthy = self.h.wait_healthy(CURRENT_EXE, CURRENT_SHA, before['process']['argv'], seconds=240)
        after = snapshot(self.h, initial=False)
        same_configuration(self.h, before, after)
        require((healthy['process']['pid'], healthy['process']['start_ticks']) ==
                (after['process']['pid'], after['process']['start_ticks']), 'process changed after health')
        self.h.save_json('inspection-after.json', after)
        return healthy


def inspect_transaction(ops):
    """Injected operations keep stop/probe/finally recovery testable without a server."""
    report = {'ok': False, 'restored': False, 'started_at': time.time(), 'phase': 'preflight'}
    before = ops.before()  # No stop if admission or initial evidence fails.
    ops.save(report)
    attempted = False
    try:
        report['phase'] = 'stopping'
        ops.save(report)
        attempted = True  # A failed/interrupted systemctl may still have queued stop.
        ops.stop()
        ops.assert_stopped()
        report['phase'] = 'inspecting'
        ops.save(report)
        report['inspection'] = ops.probe()
        require(report['inspection'].get('ok') is True, 'diagnostic incomplete; preserve partial output')
        report['ok'] = True
    except BaseException as exc:
        report['error'] = repr(exc)
    finally:
        if attempted:
            saved_signals = {}
            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
                saved_signals[sig] = signal.signal(sig, signal.SIG_IGN)
            try:
                ops.cancel_probe()
                report['recovery'] = ops.restore(before)
                report['restored'] = True
            except BaseException as exc:
                report['ok'] = False
                report['recovery_error'] = repr(exc)
            finally:
                for sig, previous in saved_signals.items():
                    signal.signal(sig, previous)
        report['phase'] = 'complete' if report['restored'] else 'recovery-required'
        report['finished_at'] = time.time()
        ops.save(report)
    return report


def interrupted(signum, unused_frame):
    raise InterruptedError('operator signal ' + str(signum))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['prepare', 'inspect'])
    parser.add_argument('--revision')
    parser.add_argument('--script-revision')
    parser.add_argument('--prepared-sha', help='inspect: SHA256 of reviewed prepared.json')
    parser.add_argument('--from-block', type=int)
    parser.add_argument('--to-block', type=int)
    parser.add_argument('--export-packs', action='store_true')
    args = parser.parse_args()
    require(os.geteuid() == 0, 'explicit root execution is required')
    h = load_helper()
    if args.mode == 'prepare':
        result = prepare(h, args)
    else:
        frozen()
        require(re.fullmatch('[0-9a-f]{64}', args.prepared_sha or '') is not None,
                'inspect requires reviewed prepared SHA256')
        require(args.from_block is not None and args.to_block is not None, 'explicit inclusive heights required')
        probe_argv(args)
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, interrupted)
        with open('/data/gtron/start.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = inspect_transaction(LiveOps(h, args))
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result.get('prepared') or result.get('ok') else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            raise
        print(json.dumps({'ok': False, 'error': repr(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
