#!/usr/bin/env python3
"""Thin pinned native deployment: prepare, then reviewed activate or rollback.

Reuses the committed inspection builder and static systemd comparison. Replaces
one effective ExecStart executable only; no environment, hold or checkout edits.
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
import signal
import subprocess
import sys
import time
import types
import urllib.request

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260913-history-pack-gate')
BINARY = RELEASE / 'gtron'
SOURCE = RELEASE / 'source'
CURRENT_EXE = '/data/gtron/releases/20260913-range-scheduling/gtron'
CURRENT_SHA = 'f6a61e0d0f3c1426ab2d7849991878ff9639168791fbdb940d08e628432460f1'
CURRENT_PID = 14862
CURRENT_TICKS = 4502991603
SCRIPT_PATH = 'scripts/deploy_history_pack_gate_20260913.py'
BUILDER_REVISION = '8f3b8c7d48a6e8076c81b8e49d83751945715f3d'
BUILDER_PATH = 'scripts/inspect_history_prev_20260913.py'
BUILDER_SHA = '75d2e44cd78ad26149a69614e0ac6c5557db07f7491b97eb84e0c1e55b4da4ae'
APPROVED = True
ADDITIONAL_FILES = (
    'core/rawdb/history_codec_benchmark.go',
    'core/rawdb/history_codec_benchmark_test.go',
    'core/rawdb/inspect_state_history.go',
    'core/rawdb/inspect_state_history_test.go',
    'core/rawdb/state_changeset_chunks.go',
    'core/rawdb/state_changeset_chunks_test.go',
    'scripts/deploy_history_pack_gate_20260913.py',
    'scripts/tests/test_deploy_history_pack_gate_20260913.py',
)
HISTORY_TEST_PATTERN = 'Test.*(AsOf|Unwind|StateHistory|StateDomainChange|StateChangeset|StateChangeChunks|HistoryCodec|HistoryPrev|HotColdHistory)'
SMALL_METRICS = tuple('state/history/changeset/block_pack/small_chunk/' + suffix
                      for suffix in ('attempts', 'selected', 'candidate_saved_bytes', 'work_nanos'))


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def small_metric_values(metrics):
    values = {name: metrics.get(name, {}).get('count') for name in SMALL_METRICS}
    require(all(type(value) is int and value >= 0 for value in values.values()),
            'candidate small-chunk metrics missing or invalid')
    return values  # Encoding attempts/candidates before Put, not durable savings.


def read_local_metrics():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open('http://127.0.0.1:6062/debug/metrics', timeout=10) as response:
        return json.loads(response.read(8 * 1024 * 1024).decode('utf-8'))['metrics']


def load_modules(helper_path=None):
    blob = subprocess.check_output(['git', 'show', BUILDER_REVISION + ':' + BUILDER_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)
    require(hashlib.sha256(blob).hexdigest() == BUILDER_SHA, 'pinned builder blob SHA differs')
    i = types.ModuleType('pinned_inspection_builder')
    # The reused Git admission compares the executed wrapper to its ops commit.
    i.__file__ = __file__
    exec(compile(blob, BUILDER_PATH, 'exec'), i.__dict__)
    i.RELEASE, i.SOURCE, i.BINARY = RELEASE, SOURCE, BINARY
    i.CURRENT_PID, i.CURRENT_TICKS = CURRENT_PID, CURRENT_TICKS
    i.SCRIPT_PATH = SCRIPT_PATH
    i.INSPECT_APPROVED = APPROVED
    i.ALLOWED_FILES = tuple(sorted(set(i.ALLOWED_FILES) | set(ADDITIONAL_FILES)))
    i.NATIVE_TEST_PATTERN = HISTORY_TEST_PATTERN
    if helper_path is not None:  # Local dependency injection only; no CLI override.
        i.HELPER = Path(helper_path)
    original_normalizer = i.normalize_exec_start
    def normalize(value):
        # systemd may describe abnormal exit as 11/SEGV or 1/FAILURE. This
        # remains runtime metadata; retain the strict field/command parser.
        if isinstance(value, str):
            value = re.sub(r'(;\s*status=[0-9]+)/[A-Z][A-Z0-9_]*(\s*\})$', r'\1\2', value)
        return original_normalizer(value)
    i.normalize_exec_start = normalize
    h = i.load_helper()
    h.OLD_EXE, h.OLD_SHA = CURRENT_EXE, CURRENT_SHA
    h.OLD_PID, h.OLD_START_TICKS, h.BINARY = CURRENT_PID, CURRENT_TICKS, BINARY
    def queue_environment(exe):
        require(exe in (CURRENT_EXE, str(BINARY)), 'unrecognized release environment')
        return '1'  # Both original and candidate already have all five opt-ins.
    h.expected_history_queue_environment = queue_environment
    original_runtime = h.verify_observer_runtime
    def verify_runtime(pid, exe):
        result = original_runtime(pid, exe)
        if exe == str(BINARY):
            metrics = read_local_metrics()
            require(metrics.get('process/start/unix_nano', {}).get('value') == result['process_start_unix_nano'],
                    'process changed between runtime metric checks')
            result['small_chunk_encoding_attempts'] = small_metric_values(metrics)
        return result
    h.verify_observer_runtime = verify_runtime
    return i, h


def effective_exec_edit(before):
    effective = []
    for item in before['main']['files']:
        text = base64.b64decode(item['data_b64']).decode('utf-8')
        section, logical = '', ''
        for line in text.splitlines():
            stripped = line.strip()
            if not logical and (not stripped or stripped.startswith(('#', ';'))):
                continue
            logical += stripped
            if logical.endswith('\\'):
                logical = logical[:-1] + ' '
                continue
            if logical.startswith('[') and logical.endswith(']'):
                section = logical[1:-1]
            elif section == 'Service' and logical.startswith('ExecStart='):
                value = logical[len('ExecStart='):].strip()
                effective = effective + [(item, value)] if value else []
            logical = ''
        require(not logical, 'unterminated unit continuation')
    require(len(effective) == 1, 'expected one effective ExecStart')
    item, command = effective[0]
    original = base64.b64decode(item['data_b64'])
    require(command.split()[0] == CURRENT_EXE, 'effective executable differs')
    require(original.count(CURRENT_EXE.encode()) == 1, 'old executable is not unique in unit file')
    require(item['uid'] == 0 and not item['mode'] & 0o022, 'unsafe unit ownership/mode')
    return item, original, original.replace(CURRENT_EXE.encode(), str(BINARY).encode(), 1)


def candidate_configuration(i, before, item, updated):
    expected = copy.deepcopy(before)
    for entry in expected['main']['files']:
        if entry['path'] == item['path']:
            entry['data_b64'] = base64.b64encode(updated).decode('ascii')
            entry['sha256'] = hashlib.sha256(updated).hexdigest()
    value = expected['main']['properties']['ExecStart']
    static = i.normalize_exec_start(value)
    require(static['path'] == CURRENT_EXE and static['argv[]'].count(CURRENT_EXE) == 1 and
            value.count(CURRENT_EXE) == 2, 'unexpected effective path/argv before switch')
    expected['main']['properties']['ExecStart'] = value.replace(CURRENT_EXE, str(BINARY))
    return expected


def compare_configuration(i, h, expected, allow_new_latch=False, pending_exec_values=()):
    now = {'main': h.unit_snapshot(h.SERVICE),
           'others': {u: h.unit_snapshot(u) for u in h.PRESERVED_UNITS},
           'holds': {p: h.hold_snapshot(p) for p in (h.GLOBAL_HOLD, h.MAIN_HOLD)},
           'guard_config': h.saved_file(h.GUARD_CONFIG), 'guard_script': h.saved_file(h.GUARD)}
    reference = copy.deepcopy(expected)
    if pending_exec_values:
        # A successful file replace followed by failed fsync/daemon-reload can
        # leave the manager on either approved command. Other settings and all
        # file bytes still have to match exactly before rollback may write.
        observed = i.normalize_exec_start(now['main']['properties']['ExecStart'])
        require(observed in [i.normalize_exec_start(v) for v in pending_exec_values],
                'unapproved effective command while reverting pending reload')
        reference['main']['properties']['ExecStart'] = now['main']['properties']['ExecStart']
    if allow_new_latch and not reference['holds'][h.MAIN_HOLD]['exists'] and now['holds'][h.MAIN_HOLD]['exists']:
        # A genuine disk stop may block start, never restoration of original
        # config. Do not clear or modify that latch or any other guard state.
        reference['holds'][h.MAIN_HOLD] = now['holds'][h.MAIN_HOLD]
    i.same_configuration(h, reference, now)
    require(h.file_sha(CURRENT_EXE) == CURRENT_SHA, 'original binary changed')
    return now


def verify_prepared(i, h, reviewed_sha=None):
    record = h.load_json('gate-prepared.json')
    if reviewed_sha is not None:
        require(h.file_sha(RELEASE / 'gate-prepared.json') == reviewed_sha, 'reviewed prepare record differs')
    require(record.get('gate_prepared') is True and record.get('builder_sha256') == BUILDER_SHA,
            'successful gate prepare required')
    i.verify_prepared(h, record)
    require(h.file_sha(RELEASE / 'prepared.json') == record['builder_record_sha256'], 'builder record changed')
    require(h.file_sha(RELEASE / 'native-history-tests.log') == record['history_tests_sha256'], 'history test evidence changed')
    require((RELEASE / 'source-commit').read_text().strip() == record['source_commit'], 'release source marker changed')
    require((RELEASE / 'SHA256SUMS').read_text() == record['binary_sha256'] + '  gtron\n', 'release binary marker changed')
    return record


def prepare(i, h, args):
    i.validate_revision(h, args.revision, args.script_revision)
    require(not RELEASE.is_symlink(), 'release must not be a symlink')
    RELEASE.mkdir(mode=0o755, parents=True, exist_ok=True)
    with open(str(RELEASE / '.gate-prepare.lock'), 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return prepare_locked(i, h, args)


def prepare_locked(i, h, args):
    i.prepare(h, args)  # Exact Git admission, isolated native storage tests and Sapling build.
    if (RELEASE / 'gate-prepared.json').exists():
        record = verify_prepared(i, h)
        return {'prepared': True, 'already_prepared': True,
                'prepared_sha256': h.file_sha(RELEASE / 'gate-prepared.json')}
    env = dict(os.environ, CGO_ENABLED='1', GOMAXPROCS='2', GOFLAGS='', GOTOOLCHAIN='local')
    env.pop('GOROOT', None)
    for key in (h.CACHE_OBSERVER_ENV, h.OBSERVER_ENV, h.PRUNE_ENV, h.HISTORY_RANGE_ENV, h.HISTORY_QUEUE_ENV):
        env[key] = '1'
    h.run([i.GO, 'test', '-p', '2', '-tags', 'sapling', './core/state', './core/state/snapshots', './core',
           '-run', HISTORY_TEST_PATTERN, '-count=1', '-timeout=300s'], cwd=SOURCE, env=env,
          timeout=1800, log='native-history-tests.log')
    record = h.load_json('prepared.json')
    i.verify_prepared(h, record)
    after = i.snapshot(h)
    i.same_configuration(h, record['preflight'], after)
    item, original, updated = effective_exec_edit(after)
    record.update(gate_prepared=True, builder_sha256=BUILDER_SHA,
                  builder_record_sha256=h.file_sha(RELEASE / 'prepared.json'),
                  history_tests_sha256=h.file_sha(RELEASE / 'native-history-tests.log'),
                  exec_file=item, after_gate_tests=after,
                  candidate_unit_sha256=hashlib.sha256(updated).hexdigest())
    h.atomic_write(RELEASE / 'source-commit', (record['source_commit'] + '\n').encode())
    h.atomic_write(RELEASE / 'SHA256SUMS', (record['binary_sha256'] + '  gtron\n').encode())
    h.save_json('gate-prepared.json', record)
    return {'prepared': True, 'binary_sha256': record['binary_sha256'],
            'prepared_sha256': h.file_sha(RELEASE / 'gate-prepared.json')}


class Deployment:
    def __init__(self, i, h, args):
        self.i, self.h, self.args = i, h, args
        self.record = None

    def load(self):
        self.record = verify_prepared(self.i, self.h, self.args.prepared_sha)
        self.before = self.record['preflight']
        self.item, self.original, self.updated = effective_exec_edit(self.before)
        require(self.item == self.record['exec_file'], 'saved ExecStart file differs')
        self.expected = candidate_configuration(self.i, self.before, self.item, self.updated)

    def admit(self):
        self.load()
        require(not (RELEASE / 'activation-state.json').exists(), 'activation already attempted; review or explicitly rollback')
        current = self.i.snapshot(self.h)
        self.i.same_configuration(self.h, self.before, current)
        require(current['process']['argv'] == self.before['process']['argv'], 'original argv changed')
        self.h.save_json('activation-before.json', current)

    def save(self, state):
        self.h.save_json('activation-state.json', state)

    def stop(self):
        self.h.run([self.h.SYSTEMCTL, 'stop', self.h.SERVICE], timeout=660, log='activate-stop.log')
        props = self.h.show(self.h.SERVICE)
        require(props.get('ActiveState') == 'inactive' and int(props.get('MainPID', '0')) == 0,
                'original service not fully stopped')

    def install(self):
        compare_configuration(self.i, self.h, self.before)
        self.h.guard_check()
        self.h.atomic_write(self.item['path'], self.updated, self.item)
        self.h.run([self.h.SYSTEMCTL, 'daemon-reload'], timeout=60, log='activate-reload.log')
        compare_configuration(self.i, self.h, self.expected)

    def start_candidate(self):
        self.h.guard_check()
        self.h.run([self.h.SYSTEMCTL, 'start', self.h.SERVICE], timeout=180, log='activate-start.log')

    def healthy(self, candidate):
        exe = str(BINARY) if candidate else CURRENT_EXE
        checksum = self.record['binary_sha256'] if candidate else CURRENT_SHA
        argv = [exe] + self.before['process']['argv'][1:]
        result = self.h.wait_healthy(exe, checksum, argv, seconds=240)
        after = self.h.preflight(expected_exe=exe, expected_sha=checksum)
        self.i.same_configuration(self.h, self.expected if candidate else self.before, after)
        require((after['process']['pid'], after['process']['start_ticks']) ==
                (result['process']['pid'], result['process']['start_ticks']), 'process changed after health')
        self.h.save_json('activation-after.json' if candidate else 'rollback-after.json', after)
        return result

    def rollback(self):
        if self.record is None:
            self.load()
        current = Path(self.item['path']).read_bytes()
        require(current in (self.original, self.updated), 'unit edited externally; refusing to overwrite')
        commands = (self.before['main']['properties']['ExecStart'], self.expected['main']['properties']['ExecStart'])
        state = compare_configuration(self.i, self.h, self.before if current == self.original else self.expected,
                                      allow_new_latch=True, pending_exec_values=commands)
        props = self.h.show(self.h.SERVICE)
        pid = int(props.get('MainPID', '0'))
        if pid:
            process = self.h.process_identity(pid)
            allowed = {CURRENT_EXE: CURRENT_SHA, str(BINARY): self.record['binary_sha256']}
            require(process['exe'] in allowed and process['exe_sha256'] == allowed[process['exe']],
                    'unrelated live executable; refusing to stop')
            if (props.get('ActiveState') == 'active' and current == self.original and process['exe'] == CURRENT_EXE and
                    self.i.normalize_exec_start(state['main']['properties']['ExecStart']) ==
                    self.i.normalize_exec_start(commands[0])):
                return self.healthy(False)
        self.h.run([self.h.SYSTEMCTL, 'stop', self.h.SERVICE], timeout=660, log='rollback-stop.log')
        props = self.h.show(self.h.SERVICE)
        require(props.get('ActiveState') in ('inactive', 'failed') and int(props.get('MainPID', '0')) == 0,
                'service did not stop; will not force kill')
        current = Path(self.item['path']).read_bytes()
        require(current in (self.original, self.updated), 'unit changed while stopping')
        compare_configuration(self.i, self.h, self.before if current == self.original else self.expected,
                              allow_new_latch=True, pending_exec_values=commands)
        self.h.atomic_write(self.item['path'], self.original, self.item)
        self.h.run([self.h.SYSTEMCTL, 'daemon-reload'], timeout=60, log='rollback-reload.log')
        compare_configuration(self.i, self.h, self.before, allow_new_latch=True)
        self.h.guard_check()  # A real new disk latch is preserved and blocks start.
        self.h.run([self.h.SYSTEMCTL, 'start', self.h.SERVICE], timeout=180, log='rollback-start.log')
        return self.healthy(False)


def protected_rollback(deployment):
    old = {sig: signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    try:
        return deployment.rollback()
    finally:
        for sig, handler in old.items():
            signal.signal(sig, handler)


def activate_transaction(deployment):
    deployment.admit()
    state = {'phase': 'stopping', 'active': False, 'started_at': time.time()}
    deployment.save(state)
    try:
        deployment.stop()
        deployment.install()
        deployment.start_candidate()
        state['result'] = deployment.healthy(True)
        state.update(phase='active', active=True)
    except BaseException as failure:
        state['error'] = repr(failure)
        try:
            state['rollback'] = protected_rollback(deployment)
            state['phase'] = 'rolled-back'
        except BaseException as recovery:
            state.update(phase='recovery-required', rollback_error=repr(recovery))
    state['finished_at'] = time.time()
    deployment.save(state)
    return state


def interrupted(signum, unused):
    raise InterruptedError('operator signal ' + str(signum))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['prepare', 'activate', 'rollback'])
    parser.add_argument('--revision')
    parser.add_argument('--script-revision')
    parser.add_argument('--prepared-sha')
    args = parser.parse_args()
    require(os.geteuid() == 0, 'explicit root execution required')
    require(APPROVED and ADDITIONAL_FILES, 'release scope/safety review not frozen')
    i, h = load_modules()
    if args.mode == 'prepare':
        result = prepare(i, h, args)
    else:
        require(re.fullmatch('[0-9a-f]{64}', args.prepared_sha or '') is not None, 'reviewed prepared SHA required')
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, interrupted)
        with open('/data/gtron/start.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            deployment = Deployment(i, h, args)
            if args.mode == 'activate':
                result = activate_transaction(deployment)
            else:
                result = {'rolled_back': True, 'result': protected_rollback(deployment)}
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if any(result.get(k) for k in ('prepared', 'active', 'rolled_back')) else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            raise
        print(json.dumps({'ok': False, 'error': repr(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
