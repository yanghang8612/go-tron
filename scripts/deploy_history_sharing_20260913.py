#!/usr/bin/env python3
"""Pinned native prepare, reader-only bridge, then explicit shared writer enable.

After arming, recovery/disable always use the SAME v3 reader with writer=false.
There is no operator command which removes the marker or restores an old reader.
"""
import argparse
import base64
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import stat
import subprocess
import sys
import types

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260913-history-sharing')
BINARY = RELEASE / 'gtron'
CURRENT_RELEASE = Path('/data/gtron/releases/20260913-history-pack-gate')
CURRENT_EXE = str(CURRENT_RELEASE / 'gtron')
CURRENT_SOURCE = 'b1305f553b5639d90eb2f58c6a5a1b23b9b025b5'
GATE_PATH = 'scripts/deploy_history_pack_gate_20260913.py'
GATE_SHA = '69c633498a1863210803340ac5fb61f3e9dd061cc5bd7046f7946ac9cdc99134'
NATIVE_RECORD_HELPER = 'scripts/inspect_history_boundary_20260913.py'
NATIVE_RECORD_SHA = '440176cebcfb185cb2117ee5ce293059d6e713d722522c76681a8a68b07f0303'
GUARD_SOURCE = 'scripts/history_shared_reader_guard.py'
SCRIPT_PATH = 'scripts/deploy_history_sharing_20260913.py'
GUARD = Path('/usr/local/libexec/gtron-history-shared-reader-guard.py')
DROPIN = Path('/etc/systemd/system/gtron.service.d/zz-history-shared-reader.conf')
MARKER = Path('/data/gtron/main/HISTORY_SHARED_CHUNKS_REQUIRES_READER_V3.json')
ARMED = RELEASE / 'reader-required.json'
FLAG = '--history.cross-block-dedup='
APPROVED = True
ALLOWED_FILES = (
    'cmd/gtron/db_cmd.go',
    'cmd/gtron/db_history_codec_benchmark.go',
    'cmd/gtron/db_history_pack_export.go',
    'cmd/gtron/db_history_pack_export_test.go',
    'cmd/gtron/db_history_sharing_benchmark.go',
    'cmd/gtron/db_history_sharing_benchmark_test.go',
    'cmd/gtron/db_inspect_history.go',
    'cmd/gtron/history_compression.go',
    'cmd/gtron/main.go',
    'core/blockbuffer/buffer.go',
    'core/blockbuffer/history_chunk_scope.go',
    'core/blockbuffer/history_chunk_scope_test.go',
    'core/blockbuffer/history_shared_pack_test.go',
    'core/pointread/view.go',
    'core/rawdb/accessors_state_changeset.go',
    'core/rawdb/accessors_state_changeset_borrowed.go',
    'core/rawdb/chain_db.go',
    'core/rawdb/database.go',
    'core/rawdb/history_sharing_benchmark.go',
    'core/rawdb/history_sharing_benchmark_test.go',
    'core/rawdb/inspect_state_history.go',
    'core/rawdb/inspect_state_history_test.go',
    'core/rawdb/pebbledb/pebble.go',
    'core/rawdb/reset.go',
    'core/rawdb/schema.go',
    'core/rawdb/state_changeset_chunks.go',
    'core/rawdb/state_changeset_shared.go',
    'core/rawdb/state_changeset_shared_fault_test.go',
    'core/rawdb/state_changeset_shared_gc.go',
    'core/rawdb/state_changeset_shared_gc_test.go',
    'core/rawdb/state_changeset_shared_test.go',
    'core/rawdb/state_history_read_view.go',
    'core/rawdb/state_history_read_view_test.go',
    'core/state/pruning/history_shared_chunk_gc.go',
    'core/state/pruning/history_shared_chunk_gc_test.go',
    'core/state/pruning/pruner.go',
    'core/state/pruning/worker.go',
    'core/state/snapshots/history_read_view_test.go',
    'core/state/snapshots/history_segment.go',
    'core/state/snapshots/history_stream_build.go',
    'scripts/deploy_history_sharing_20260913.py',
    'scripts/history_shared_reader_guard.py',
    'scripts/inspect_history_boundary_20260913.py',
    'scripts/tests/test_deploy_history_sharing_20260913.py',
    'scripts/tests/test_inspect_history_boundary_20260913.py',
)  # Exact b1305f55..source non-doc paths; independently reviewed and tested.
GC_METRICS = tuple('state/history/shared/gc/' + name for name in
                   ('enabled', 'scanned_meta', 'candidates', 'retired', 'nonempty',
                    'deferred_coverage', 'deferred_busy', 'errors'))


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def git_blob(revision, path):
    require(re.fullmatch('[0-9a-f]{40}', revision or ''), 'exact Git commit required')
    return subprocess.check_output(['git', 'show', revision + ':' + path], cwd=str(REPO),
                                   stdin=subprocess.DEVNULL, timeout=30)


def module_from_blob(name, blob, filename):
    module = types.ModuleType(name)
    module.__file__ = filename
    exec(compile(blob, filename, 'exec'), module.__dict__)
    return module


def systemd_properties(output, parse_commands):
    """Preserve old systemctl's repeated command-property lines in order."""
    properties = {}
    for line in output.splitlines():
        if '=' not in line:
            continue
        name, value = line.split('=', 1)
        if name in ('ExecStart', 'ExecStartPre'):
            parse_commands(value)  # Reject malformed command serializations.
            if name in properties:
                require(properties[name] and value, 'ambiguous empty repeated startup property')
                value = properties[name] + ' ; ' + value
        properties[name] = value
    return properties


def load_modules(args, helper_path=None):
    require(APPROVED and ALLOWED_FILES, 'shared history deployment review not frozen')
    require(args.current_pid > 0 and args.current_start_ticks > 0, 'fresh native process pins required')
    require(re.fullmatch('[0-9a-f]{64}', args.current_record_sha or ''), 'reviewed original native record SHA required')
    blob = git_blob(CURRENT_SOURCE, GATE_PATH)
    require(sha(blob) == GATE_SHA, 'pinned gate wrapper changed')
    g = module_from_blob('pinned_gate_ops', blob, __file__)
    g.REPO, g.RELEASE, g.SOURCE, g.BINARY = REPO, RELEASE, RELEASE / 'source', BINARY
    g.SCRIPT_PATH, g.APPROVED = SCRIPT_PATH, APPROVED
    # Load the old wrappers before adapting CURRENT_EXE: their pinned helper
    # attestation intentionally identifies the earlier range release first.
    i, h = g.load_modules(helper_path)
    native_blob = git_blob(args.script_revision, NATIVE_RECORD_HELPER)
    require(sha(native_blob) == NATIVE_RECORD_SHA, 'reviewed original-native verifier changed')
    native = module_from_blob('native_record_verifier', native_blob, NATIVE_RECORD_HELPER)
    native.REPO = REPO
    old = native.verify_native_record(h, CURRENT_RELEASE, Path(CURRENT_EXE), CURRENT_SOURCE, True)
    require(old['prepared_sha256'] == args.current_record_sha, 'original native record changed since review')
    g.CURRENT_EXE, g.CURRENT_SHA = CURRENT_EXE, old['binary_sha256']
    g.CURRENT_PID, g.CURRENT_TICKS = args.current_pid, args.current_start_ticks
    i.CURRENT_EXE, i.CURRENT_SOURCE, i.CURRENT_SHA = CURRENT_EXE, CURRENT_SOURCE, g.CURRENT_SHA
    i.CURRENT_PID, i.CURRENT_TICKS = args.current_pid, args.current_start_ticks
    i.ALLOWED_FILES = tuple(ALLOWED_FILES)
    h.OLD_EXE, h.OLD_SHA = CURRENT_EXE, g.CURRENT_SHA
    h.OLD_PID, h.OLD_START_TICKS = args.current_pid, args.current_start_ticks
    guard_blob = git_blob(args.script_revision, GUARD_SOURCE)
    guard = module_from_blob('shared_reader_guard', guard_blob, GUARD_SOURCE)
    def show(unit):
        output, _ = h.run([h.SYSTEMCTL, 'show', unit, '--no-pager'])
        if unit == h.SERVICE:
            return systemd_properties(output, guard.parse_commands)
        return dict(line.split('=', 1) for line in output.splitlines() if '=' in line)
    # Old systemctl prints one ExecStartPre= line per command. The inherited
    # dict parser discarded all but the final command after adding our guard.
    h.show = show
    def normalize(value):
        values = guard.parse_commands(value)
        return values[0] if len(values) == 1 else values
    i.normalize_exec_start = normalize
    pattern = g.HISTORY_TEST_PATTERN + '|Test.*(Shared|HistorySharing|HistoryCompression|HistoryChunk|HistoryReadView|ResetMutableState)'
    g.HISTORY_TEST_PATTERN = i.NATIVE_TEST_PATTERN = pattern
    original_edit, original_expected = g.effective_exec_edit, g.candidate_configuration
    def edit(before):
        item, original, updated = original_edit(before)
        require(b'--history.cross-block-dedup' not in original, 'old unit already contains new writer option')
        return item, original, updated.replace(str(BINARY).encode(), (str(BINARY) + ' ' + FLAG + 'false').encode(), 1)
    def expected(builder, before, item, updated):
        out = original_expected(builder, before, item, updated)
        command = guard.parse_commands(out['main']['properties']['ExecStart'])[0]
        command['argv[]'] = command['argv[]'].replace(str(BINARY), str(BINARY) + ' ' + FLAG + 'false', 1)
        out['main']['properties']['ExecStart'] = serialize(command)
        return out
    g.effective_exec_edit, g.candidate_configuration = edit, expected
    return g, i, h, guard, guard_blob, old


def serialize(command):
    return ('{ path=' + command['path'] + ' ; argv[]=' + command['argv[]'] +
            ' ; ignore_errors=' + command['ignore_errors'] +
            ' ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0 }')


def file_record(path, data, mode=0o644):
    return {'path': str(path), 'sha256': sha(data), 'data_b64': base64.b64encode(data).decode(),
            'mode': mode, 'uid': 0, 'gid': 0, 'xattrs': {}}


def marker_required():
    return os.path.lexists(ARMED) or os.path.lexists(MARKER)


def guard_parent():
    # This is a fixed root-owned executable location, never a service-writable
    # release or data directory. Creating it is independent of service config.
    parent = GUARD.parent
    parent.mkdir(mode=0o755, exist_ok=True)
    for path in (parent, *parent.parents):
        info = os.lstat(path)
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022,
                'unsafe reader guard parent: ' + str(path))


def prepare(g, i, h, guard_blob, old, args):
    i.validate_revision(h, args.revision, args.script_revision)
    require(not RELEASE.is_symlink(), 'release cannot be a symlink')
    RELEASE.mkdir(parents=True, exist_ok=True, mode=0o755)
    with open(RELEASE / '.shared-prepare.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        g.prepare(i, h, args)
        record = g.verify_prepared(i, h)
        env = dict(os.environ, CGO_ENABLED='1', GOMAXPROCS='2', GOFLAGS='', GOTOOLCHAIN='local')
        env.pop('GOROOT', None)
        for name in (h.CACHE_OBSERVER_ENV, h.OBSERVER_ENV, h.PRUNE_ENV, h.HISTORY_RANGE_ENV, h.HISTORY_QUEUE_ENV):
            env[name] = '1'
        h.run([i.GO, 'test', '-p', '2', '-tags', 'sapling', './core/blockbuffer', './core/state/pruning',
               '-count=1', '-timeout=300s'], cwd=RELEASE / 'source', env=env, timeout=1200,
              log='native-shared-lifecycle-tests.log')
        text, _ = h.run([str(BINARY), 'db', 'benchmark-history-sharing', '--help'], log='shared-cli-help.txt')
        require('--export-packs' in text and '--max-duration' in text, 'shared reader/replay CLI contract missing')
        text, _ = h.run([str(BINARY), '--help'], log='shared-node-help.txt')
        require('--history.cross-block-dedup' in text, 'shared writer flag missing')
        i.same_configuration(h, record['preflight'], i.snapshot(h))
        record.update(shared_prepared=True, original_native=old, guard_sha256=sha(guard_blob),
                      gate_record_sha256=h.file_sha(RELEASE / 'gate-prepared.json'),
                      shared_tests_sha256=h.file_sha(RELEASE / 'native-shared-lifecycle-tests.log'))
        h.atomic_write(RELEASE / 'reader-guard.py', guard_blob, file_record(GUARD, guard_blob, 0o755))
        h.save_json('shared-prepared.json', record)
        return {'prepared': True, 'binary_sha256': record['binary_sha256'],
                'prepared_sha256': h.file_sha(RELEASE / 'shared-prepared.json')}


class Deployment:
    def __init__(self, g, i, h, guard, guard_blob, old, args):
        self.g, self.i, self.h, self.guard, self.guard_blob = g, i, h, guard, guard_blob
        self.old, self.args = old, args
        self.target = 'on' if args.mode == 'enable' else 'off'
        self.record = h.load_json('shared-prepared.json')
        require(h.file_sha(RELEASE / 'shared-prepared.json') == args.prepared_sha, 'reviewed shared prepare record changed')
        require(self.record.get('shared_prepared') is True and self.record.get('original_native') == old,
                'shared prepare/original identity differs')
        require(self.record.get('script_commit') == args.script_revision,
                'prepared exact ops revision differs')
        require(self.record['guard_sha256'] == sha(guard_blob) and
                h.file_sha(RELEASE / 'reader-guard.py') == sha(guard_blob), 'reader guard source changed')
        require(h.file_sha(RELEASE / 'gate-prepared.json') == self.record['gate_record_sha256'] and
                h.file_sha(RELEASE / 'native-shared-lifecycle-tests.log') == self.record['shared_tests_sha256'],
                'shared native evidence changed')
        g.verify_prepared(i, h)
        self.before = self.record['preflight']
        self.item, self.original, self.off = g.effective_exec_edit(self.before)
        require(self.off.count((FLAG + 'false').encode()) == 1, 'ambiguous reader-only unit')
        self.on = self.off.replace((FLAG + 'false').encode(), (FLAG + 'true').encode(), 1)
        self.marker = guard.reader_marker(BINARY, self.record['binary_sha256'], self.record['source_commit'])
        self.guard_argv = ['/usr/bin/python3', str(GUARD), '--binary', str(BINARY), '--sha256',
                           self.record['binary_sha256'], '--source', self.record['source_commit'], '--marker', str(MARKER)]
        self.dropin = ('[Service]\nExecStartPre=' + ' '.join(self.guard_argv) + '\n').encode()
        require(all(Path(entry['path']).name < DROPIN.name for entry in self.before['main']['files'][1:]),
                'reader guard drop-in does not sort last')
        self.configs = {'old': self.before}
        for mode, data in (('off', self.off), ('on', self.on)):
            out = g.candidate_configuration(i, self.before, self.item, data)
            if mode == 'on':
                out['main']['properties']['ExecStart'] = out['main']['properties']['ExecStart'].replace(FLAG+'false', FLAG+'true')
            pre = guard.parse_commands(out['main']['properties'].get('ExecStartPre', ''))
            pre.append({'path': '/usr/bin/python3', 'argv[]': ' '.join(self.guard_argv), 'ignore_errors': 'no'})
            out['main']['properties']['ExecStartPre'] = ' ; '.join(serialize(command) for command in pre)
            out['main']['files'].append(file_record(DROPIN, self.dropin))
            self.configs[mode] = out

    def current_mode(self):
        data = Path(self.item['path']).read_bytes()
        for mode, expected in (('old', self.original), ('off', self.off), ('on', self.on)):
            if data == expected:
                return mode
        raise RuntimeError('service unit changed outside approved writer modes')

    def verify_guard(self, required):
        for path, data, mode in ((GUARD, self.guard_blob, 0o755), (DROPIN, self.dropin, 0o644)):
            if os.path.lexists(path):
                require(self.h.saved_file(path) == file_record(path, data, mode), 'reader guard file changed: ' + str(path))
            else:
                require(not required, 'reader guard is missing: ' + str(path))
        for path in (ARMED, MARKER):
            if os.path.lexists(path):
                require(json.loads(self.guard.read_root_file(path, 4096)) == self.marker, 'permanent reader marker changed')

    def compare(self, mode, transitional=False, allow_latch=False):
        h = self.h
        # A failed reload during bridge recovery may still list our removed
        # drop-in in systemd's cached DropInPaths. Only that exact optional
        # file may be absent, and only before arming during recovery.
        props = h.show(h.SERVICE)
        paths = [props.get('FragmentPath', '')] + shlex.split(props.get('DropInPaths', ''))
        require(paths[0], 'missing unit fragment')
        files = []
        for path in paths:
            if transitional and not marker_required() and path == str(DROPIN) and not os.path.lexists(path):
                continue
            files.append(h.saved_file(path))
        now = {'main': {'properties': props, 'files': files},
               'others': {u: h.unit_snapshot(u) for u in h.PRESERVED_UNITS},
               'holds': {p: h.hold_snapshot(p) for p in (h.GLOBAL_HOLD, h.MAIN_HOLD)},
               'guard_config': h.saved_file(h.GUARD_CONFIG), 'guard_script': h.saved_file(h.GUARD)}
        expected = copy.deepcopy(self.configs[mode])
        self.verify_guard(mode != 'old' and not transitional)
        if transitional:
            allowed = ('off', 'on') if marker_required() else ('old', 'off')
            for prop in ('ExecStart', 'ExecStartPre'):
                actual = self.guard.parse_commands(now['main']['properties'].get(prop, ''))
                require(actual in [self.guard.parse_commands(self.configs[m]['main']['properties'].get(prop, '')) for m in allowed],
                        'unapproved effective startup command during reload')
                expected['main']['properties'][prop] = now['main']['properties'].get(prop, '')
            expected['main']['files'] = [entry for entry in expected['main']['files'] if entry['path'] != str(DROPIN)]
            if any(entry['path'] == str(DROPIN) for entry in now['main']['files']):
                expected['main']['files'].append(file_record(DROPIN, self.dropin))
        if allow_latch and not expected['holds'][h.MAIN_HOLD]['exists'] and now['holds'][h.MAIN_HOLD]['exists']:
            expected['holds'][h.MAIN_HOLD] = now['holds'][h.MAIN_HOLD]
        self.i.same_configuration(h, expected, now)
        require(h.file_sha(CURRENT_EXE) == self.old['binary_sha256'], 'original native binary changed')
        return now

    def admit(self):
        if self.args.mode == 'bridge':
            require(not marker_required() and not os.path.lexists(GUARD) and not os.path.lexists(DROPIN), 'bridge already armed or guard already installed')
            require(not (RELEASE / 'bridge-state.json').exists(), 'bridge already attempted; review saved state')
            current = self.i.snapshot(self.h)
            self.i.same_configuration(self.h, self.before, current)
            require(current['process']['argv'] == self.before['process']['argv'], 'original argv changed')
        else:
            require(self.current_mode() in ('off', 'on'), 'reader-only bridge must be installed first')
            self.compare(self.current_mode())
            self.healthy_mode(self.current_mode())
            if self.args.mode == 'enable':
                # These records survive every later disable/recovery. Arming
                # precedes any stop or unit change that could enable new refs.
                data = (json.dumps(self.marker, sort_keys=True) + '\n').encode()
                for path in (ARMED, MARKER):
                    self.h.atomic_write(path, data, file_record(path, data))

    def save(self, state):
        self.h.save_json(('bridge' if self.args.mode == 'bridge' else 'writer') + '-state.json', state)

    def stop(self, recovering=False):
        self.compare(self.current_mode(), transitional=recovering, allow_latch=True)
        props = self.h.show(self.h.SERVICE)
        pid = int(props.get('MainPID', '0'))
        if pid:
            process = self.h.process_identity(pid)
            allowed = {str(BINARY): self.record['binary_sha256']}
            if not marker_required():
                allowed[CURRENT_EXE] = self.old['binary_sha256']
            require(allowed.get(process['exe']) == process['exe_sha256'], 'refusing to stop an unrelated/obsolete reader')
        self.h.run([self.h.SYSTEMCTL, 'stop', self.h.SERVICE], timeout=660, log='shared-stop.log')
        props = self.h.show(self.h.SERVICE)
        require(props.get('ActiveState') in ('inactive', 'failed') and int(props.get('MainPID', '0')) == 0,
                'service did not fully stop; no forced kill')

    def install_mode(self, mode):
        self.compare(self.current_mode(), transitional=True, allow_latch=True)
        require(mode != 'old' or not marker_required(), 'old reader rollback prohibited after arming')
        if mode != 'old':
            guard_parent()
            self.h.atomic_write(GUARD, self.guard_blob, file_record(GUARD, self.guard_blob, 0o755))
            self.h.atomic_write(DROPIN, self.dropin, file_record(DROPIN, self.dropin))
        self.h.atomic_write(self.item['path'], {'old': self.original, 'off': self.off, 'on': self.on}[mode], self.item)
        if mode == 'old':
            for path in (DROPIN, GUARD):
                if os.path.lexists(path):
                    path.unlink()
                    fd = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY)
                    try: os.fsync(fd)
                    finally: os.close(fd)
        self.h.run([self.h.SYSTEMCTL, 'daemon-reload'], timeout=60, log='shared-reload.log')
        self.compare(mode, allow_latch=True)

    def install(self):
        self.install_mode(self.target)

    def start_candidate(self):
        self.h.guard_check()
        self.h.run([self.h.SYSTEMCTL, 'start', self.h.SERVICE], timeout=180, log='shared-start.log')

    def healthy_mode(self, mode):
        exe = CURRENT_EXE if mode == 'old' else str(BINARY)
        checksum = self.old['binary_sha256'] if mode == 'old' else self.record['binary_sha256']
        argv = [exe] + ([] if mode == 'old' else [FLAG + ('true' if mode == 'on' else 'false')]) + self.before['process']['argv'][1:]
        result = self.h.wait_healthy(exe, checksum, argv, seconds=240)
        after = self.h.preflight(expected_exe=exe, expected_sha=checksum)
        self.compare(mode)
        require((after['process']['pid'], after['process']['start_ticks']) ==
                (result['process']['pid'], result['process']['start_ticks']), 'process changed after health')
        if mode != 'old':
            metrics = self.g.read_local_metrics()
            require(metrics.get('process/start/unix_nano', {}).get('value') == result['observer']['process_start_unix_nano'],
                    'process changed during shared metric validation')
            values = {name: metrics.get(name, {}).get('value') for name in GC_METRICS}
            require(all(type(v) is int and v >= 0 for v in values.values()) and values[GC_METRICS[0]] == 1,
                    'shared GC metric contract missing/disabled')
            for name in ('attempts', 'selected', 'new_chunks', 'reused_raw_bytes'):
                value = metrics.get('state/history/shared/' + name, {}).get('count')
                require(type(value) is int and value >= 0, 'shared reader/writer counters missing')
            result['shared_gc'] = values
        self.h.save_json('shared-health-' + mode + '.json', after)
        return result

    def healthy(self, unused):
        return self.healthy_mode(self.target)

    def rollback(self):
        # Even a failed marker fsync is treated as armed if either durable name
        # exists. Only the original pre-writer bridge failure may restore b130.
        mode = 'old' if self.args.mode == 'bridge' and not marker_required() else 'off'
        self.stop(recovering=True)
        self.install_mode(mode)
        self.start_candidate()
        return self.healthy_mode(mode)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['prepare', 'bridge', 'enable', 'disable'])
    parser.add_argument('--revision')
    parser.add_argument('--script-revision', required=True)
    parser.add_argument('--prepared-sha')
    parser.add_argument('--current-pid', type=int, default=0)
    parser.add_argument('--current-start-ticks', type=int, default=0)
    parser.add_argument('--current-record-sha', required=True)
    args = parser.parse_args()
    require(os.geteuid() == 0, 'explicit root execution required')
    g, i, h, guard, blob, old = load_modules(args)
    if args.mode == 'prepare':
        result = prepare(g, i, h, blob, old, args)
    else:
        require(re.fullmatch('[0-9a-f]{64}', args.prepared_sha or ''), 'reviewed prepare SHA required')
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP): signal.signal(sig, g.interrupted)
        with open('/data/gtron/start.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            deployment = Deployment(g, i, h, guard, blob, old, args)
            if args.mode == 'disable':
                deployment.admit()
                result = {'disabled': True, 'result': g.protected_rollback(deployment)}
            else:
                result = g.activate_transaction(deployment)
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if any(result.get(key) for key in ('prepared', 'active', 'disabled')) else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as error:
        if isinstance(error, SystemExit): raise
        print(json.dumps({'ok': False, 'error': repr(error)}), file=sys.stderr, flush=True)
        sys.exit(1)
