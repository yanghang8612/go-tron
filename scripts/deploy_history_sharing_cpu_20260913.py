#!/usr/bin/env python3
"""Upgrade between two pinned v3 readers; never restore a pre-v3 binary."""
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
import types

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260913-history-sharing-cpu')
BINARY = RELEASE / 'gtron'
CURRENT_SOURCE = 'd656be3c43eb237fac4d46a8847e5fe551bc6a78'
SOURCE_REVISION = '8058e99d4e080c3329a587a2feef16133e676e84'
CURRENT_PID, CURRENT_TICKS = 17289, 4503924631
CURRENT_START = 1789304752801423589
WRAPPER_REVISION = '593c7de3dd1b324c08fe6a2085691c5a71e7b86d'
WRAPPER_PATH = 'scripts/deploy_history_sharing_systemd_20260913.py'
WRAPPER_SHA = '3d7044409ff96e58a85313812a02f811209b72bc0f4595ffc1b41c7e6afaac90'
BUILDER_REVISION = '8f3b8c7d48a6e8076c81b8e49d83751945715f3d'
BUILDER_PATH = 'scripts/inspect_history_prev_20260913.py'
BUILDER_SHA = '75d2e44cd78ad26149a69614e0ac6c5557db07f7491b97eb84e0c1e55b4da4ae'
SCRIPT_PATH = 'scripts/deploy_history_sharing_cpu_20260913.py'
APPROVED = True
ALLOWED_FILES = (
    'core/rawdb/accessors_state_changeset.go',
    'core/rawdb/state_changeset_chunks.go',
    'core/rawdb/state_changeset_shared.go',
    'core/rawdb/state_changeset_codec_reuse_bench_test.go',
    'core/rawdb/state_changeset_codec_reuse_test.go',
    'scripts/deploy_history_sharing_20260913.py',
    'scripts/deploy_history_sharing_systemd_20260913.py',
    'scripts/tests/test_deploy_history_sharing_20260913.py',
    'scripts/tests/test_deploy_history_sharing_systemd_20260913.py',
)  # Exact non-doc d656..8058 scope; new adapter is separately pinned by ops.
FLAG = '--history.cross-block-dedup='


def require(ok, message):
    if not ok: raise RuntimeError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def pinned_module(revision, path, checksum, filename):
    blob = subprocess.check_output(['git', 'show', revision + ':' + path], cwd=str(REPO),
                                   stdin=subprocess.DEVNULL, timeout=30)
    require(sha(blob) == checksum, 'pinned module changed: ' + path)
    module = types.ModuleType('pinned_' + Path(path).stem)
    module.__file__ = filename
    exec(compile(blob, filename, 'exec'), module.__dict__)
    return module


def load(args):
    require(APPROVED and ALLOWED_FILES, 'compatible reader upgrade review not frozen')
    wrapper = pinned_module(WRAPPER_REVISION, WRAPPER_PATH, WRAPPER_SHA, WRAPPER_PATH)
    original, g, old_i, old_h, guard, blob, old_native, old_args = wrapper.load_prepared(args.old_prepared_sha, 'disable')
    old = original.Deployment(g, old_i, old_h, guard, blob, old_native, old_args)
    # The old bundle remains untouched. A separate builder/helper instance owns
    # candidate paths, logs and dispatch rules, so old attestation cannot drift.
    i = pinned_module(BUILDER_REVISION, BUILDER_PATH, BUILDER_SHA, __file__)
    h = i.load_helper()  # Original range-helper attestation before rebinding.
    i.REPO, i.RELEASE, i.SOURCE, i.BINARY = REPO, RELEASE, RELEASE / 'source', BINARY
    i.CURRENT_SOURCE, i.CURRENT_EXE, i.CURRENT_SHA = CURRENT_SOURCE, str(original.BINARY), old.record['binary_sha256']
    i.CURRENT_PID, i.CURRENT_TICKS = CURRENT_PID, CURRENT_TICKS
    i.SCRIPT_PATH, i.INSPECT_APPROVED, i.ALLOWED_FILES = SCRIPT_PATH, APPROVED, tuple(ALLOWED_FILES)
    i.NATIVE_TEST_PATTERN = 'Test.*(History|Shared|StateChange|AsOf|Unwind|ResetMutableState)'
    i.normalize_exec_start = guard.parse_commands
    h.RELEASE, h.BINARY, h.OLD_EXE, h.OLD_SHA = RELEASE, BINARY, i.CURRENT_EXE, i.CURRENT_SHA
    h.OLD_PID, h.OLD_START_TICKS = CURRENT_PID, CURRENT_TICKS
    def enabled(exe):
        require(exe in (i.CURRENT_EXE, str(BINARY)), 'unapproved reader environment')
        return '1'
    h.expected_history_range_environment = h.expected_history_queue_environment = enabled
    wrapper.install_show(h, guard)
    return types.SimpleNamespace(original=original, old=old, g=g, i=i, h=h, guard=guard)


def changed(item, data):
    out = copy.deepcopy(item)
    out.update(data_b64=base64.b64encode(data).decode(), sha256=sha(data))
    return out


def content(item):
    return base64.b64decode(item['data_b64'])


def original_live(b):
    b.old.compare('on')
    result = b.i.snapshot(b.h)
    require(result['process']['argv'].count(FLAG + 'true') == 1 and FLAG + 'false' not in result['process']['argv'],
            'current writer mode is not the reviewed on state')
    runtime = b.h.verify_observer_runtime(CURRENT_PID, b.i.CURRENT_EXE)
    require(runtime['process_start_unix_nano'] == CURRENT_START, 'fresh exact process metric changed')
    return result


def prepare(b, args):
    h, i = b.h, b.i
    require(args.source_revision == SOURCE_REVISION, 'reviewed candidate source revision required')
    i.validate_revision(h, args.source_revision, args.script_revision)
    require(not RELEASE.is_symlink(), 'candidate release is a symlink')
    RELEASE.mkdir(parents=True, exist_ok=True, mode=0o755)
    with open(RELEASE / '.cpu-prepare.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original_live(b)
        if (RELEASE / 'upgrade-prepared.json').exists():
            return {'prepared': True, 'prepared_sha256': h.file_sha(RELEASE / 'upgrade-prepared.json'),
                    'already_prepared': bool(verify_prepared(b, args))}
        i.prepare(h, types.SimpleNamespace(revision=args.source_revision, script_revision=args.script_revision))
        env = dict(os.environ, CGO_ENABLED='1', GOMAXPROCS='2', GOTOOLCHAIN='local', GOFLAGS='')
        env.pop('GOROOT', None)
        for key in (h.CACHE_OBSERVER_ENV, h.OBSERVER_ENV, h.PRUNE_ENV, h.HISTORY_RANGE_ENV, h.HISTORY_QUEUE_ENV): env[key] = '1'
        h.run([i.GO, 'test', '-p', '2', '-tags', 'sapling', './core/blockbuffer', './core/state/pruning',
               './core/state/snapshots', './core', '-run', i.NATIVE_TEST_PATTERN, '-count=1', '-timeout=300s'],
              cwd=i.SOURCE, env=env, timeout=1800, log='native-compatible-history-tests.log')
        help_text, _ = h.run([str(BINARY), 'db', 'benchmark-history-sharing', '--help'], log='sharing-help.txt')
        require('--export-packs' in help_text and '--max-duration' in help_text, 'compatible reader CLI missing')
        record = h.load_json('prepared.json')
        i.verify_prepared(h, record)
        current = original_live(b)
        i.same_configuration(h, record['preflight'], current)
        record.update(upgrade_prepared=True, old_prepared_sha256=args.old_prepared_sha, current=current,
                      old_marker=h.saved_file(b.original.MARKER), old_armed=h.saved_file(b.original.ARMED),
                      reader_guard=h.saved_file(b.original.GUARD), builder_record_sha256=h.file_sha(RELEASE / 'prepared.json'),
                      history_tests_sha256=h.file_sha(RELEASE / 'native-compatible-history-tests.log'))
        h.atomic_write(RELEASE / 'source-commit', (record['source_commit'] + '\n').encode())
        h.atomic_write(RELEASE / 'SHA256SUMS', (record['binary_sha256'] + '  gtron\n').encode())
        h.save_json('upgrade-prepared.json', record)
        return {'prepared': True, 'binary_sha256': record['binary_sha256'],
                'prepared_sha256': h.file_sha(RELEASE / 'upgrade-prepared.json')}


def verify_prepared(b, args):
    h = b.h
    record = h.load_json('upgrade-prepared.json')
    if args.mode != 'prepare':
        require(re.fullmatch('[0-9a-f]{64}', args.prepared_sha or ''), 'reviewed candidate prepare SHA required')
        require(h.file_sha(RELEASE / 'upgrade-prepared.json') == args.prepared_sha, 'candidate prepared record changed')
    require(record.get('upgrade_prepared') is True and record.get('old_prepared_sha256') == args.old_prepared_sha and
            record.get('script_commit') == args.script_revision and record.get('source_commit') == SOURCE_REVISION,
            'upgrade attestation identity differs')
    if args.source_revision: require(record['source_commit'] == args.source_revision, 'candidate source differs')
    b.i.verify_prepared(h, record)
    require(h.file_sha(RELEASE / 'prepared.json') == record['builder_record_sha256'] and
            h.file_sha(RELEASE / 'native-compatible-history-tests.log') == record['history_tests_sha256'], 'native evidence changed')
    require((RELEASE / 'source-commit').read_text() == record['source_commit'] + '\n' and
            (RELEASE / 'SHA256SUMS').read_text() == record['binary_sha256'] + '  gtron\n', 'candidate release markers changed')
    return record


class Deployment:
    def __init__(self, b, args):
        self.b, self.args = b, args
        self.record = verify_prepared(b, args)
        before = self.record['current']
        self.old_exe, self.old_sha = b.i.CURRENT_EXE, b.i.CURRENT_SHA
        self.paths = (b.original.MARKER, b.original.ARMED, RELEASE / 'reader-required.json')
        files = before['main']['files']
        choices = [item for item in files if item['path'] != str(b.original.DROPIN) and self.old_exe.encode() in content(item)]
        require(len(choices) == 1 and content(choices[0]).count(self.old_exe.encode()) == 1, 'ambiguous effective binary edit')
        self.item = choices[0]
        require(content(self.item).count((FLAG + 'true').encode()) == 1, 'ambiguous old writer flag')
        self.units = {'old_on': content(self.item), 'old_off': content(self.item).replace((FLAG+'true').encode(), (FLAG+'false').encode())}
        self.units['candidate'] = self.units['old_on'].replace(self.old_exe.encode(), str(BINARY).encode())
        self.marker = changed(self.record['old_marker'], (json.dumps(b.guard.reader_marker(BINARY, self.record['binary_sha256'],
                               self.record['source_commit']), sort_keys=True) + '\n').encode())
        self.new_armed = copy.deepcopy(self.marker); self.new_armed['path'] = str(self.paths[2])
        self.configs = {mode: copy.deepcopy(before) for mode in self.units}
        for mode, config in self.configs.items():
            for n, item in enumerate(config['main']['files']):
                if item['path'] == self.item['path']: config['main']['files'][n] = changed(item, self.units[mode])
                if mode == 'candidate' and item['path'] == str(b.original.DROPIN):
                    config['main']['files'][n] = changed(item, self.replace_identity(content(item)))
            props = config['main']['properties']
            if mode == 'old_off': props['ExecStart'] = props['ExecStart'].replace(FLAG+'true', FLAG+'false')
            if mode == 'candidate':
                require(props['ExecStart'].count(self.old_exe) == 2, 'unexpected old ExecStart serialization')
                props['ExecStart'] = props['ExecStart'].replace(self.old_exe, str(BINARY))
                props['ExecStartPre'] = self.replace_identity(props['ExecStartPre'].encode()).decode()

    def replace_identity(self, data):
        for old, new in ((self.old_exe, str(BINARY)), (self.old_sha, self.record['binary_sha256']),
                         (CURRENT_SOURCE, self.record['source_commit'])):
            require(data.count(old.encode()) == 1, 'unexpected reader guard identity occurrence')
            data = data.replace(old.encode(), new.encode())
        return data

    def mode(self):
        data = Path(self.item['path']).read_bytes()
        require(data in self.units.values(), 'unapproved service unit bytes')
        return next(mode for mode, value in self.units.items() if data == value)

    def check(self, mode=None, transitional=False, allow_latch=False):
        b, h = self.b, self.b.h
        mode = mode or self.mode()
        now = {'main': h.unit_snapshot(h.SERVICE), 'others': {u: h.unit_snapshot(u) for u in h.PRESERVED_UNITS},
               'holds': {p: h.hold_snapshot(p) for p in (h.GLOBAL_HOLD, h.MAIN_HOLD)},
               'guard_config': h.saved_file(h.GUARD_CONFIG), 'guard_script': h.saved_file(h.GUARD)}
        expected = copy.deepcopy(self.configs[mode])
        require(h.saved_file(b.original.GUARD) == self.record['reader_guard'], 'reader guard code/metadata changed')
        require(h.saved_file(self.paths[1]) == self.record['old_armed'], 'original permanent reader marker changed')
        marker = h.saved_file(self.paths[0])
        allowed_markers = [self.record['old_marker'], self.marker] if transitional else [self.marker if mode == 'candidate' else self.record['old_marker']]
        require(marker in allowed_markers, 'unapproved data reader marker')
        if os.path.lexists(self.paths[2]): require(h.saved_file(self.paths[2]) == self.new_armed, 'candidate permanent marker changed')
        else: require(mode != 'candidate' or transitional, 'candidate permanent marker missing')
        if transitional:
            for prop in ('ExecStart', 'ExecStartPre'):
                actual = b.guard.parse_commands(now['main']['properties'].get(prop, ''))
                require(actual in [b.guard.parse_commands(v['main']['properties'].get(prop, '')) for v in self.configs.values()],
                        'unapproved effective startup command during recovery')
                expected['main']['properties'][prop] = now['main']['properties'].get(prop, '')
            for n, item in enumerate(expected['main']['files']):
                if item['path'] == str(b.original.DROPIN):
                    actual = h.saved_file(item['path'])
                    require(any(actual in v['main']['files'] for v in self.configs.values()), 'unapproved reader drop-in')
                    expected['main']['files'][n] = actual
        if allow_latch and not expected['holds'][h.MAIN_HOLD]['exists'] and now['holds'][h.MAIN_HOLD]['exists']:
            expected['holds'][h.MAIN_HOLD] = now['holds'][h.MAIN_HOLD]
        b.i.same_configuration(h, expected, now)
        require(h.file_sha(self.old_exe) == self.old_sha and h.file_sha(BINARY) == self.record['binary_sha256'], 'reader binary changed')
        return now

    def admit(self):
        require(not (RELEASE / 'upgrade-state.json').exists(), 'upgrade already attempted; inspect saved state or rollback')
        original_live(self.b)
        self.check('old_on')

    def save(self, state):
        self.b.h.save_json('upgrade-state.json', state)

    def stop(self):
        h = self.b.h
        self.check(transitional=True, allow_latch=True)
        props = h.show(h.SERVICE)
        if int(props.get('MainPID', '0')):
            p = h.process_identity(int(props['MainPID']))
            require({self.old_exe: self.old_sha, str(BINARY): self.record['binary_sha256']}.get(p['exe']) == p['exe_sha256'],
                    'refusing to stop an unrelated or pre-v3 process')
        h.run([h.SYSTEMCTL, 'stop', h.SERVICE], timeout=660, log='compatible-stop.log')
        props = h.show(h.SERVICE)
        require(props.get('ActiveState') in ('inactive', 'failed') and int(props.get('MainPID', '0')) == 0, 'graceful stop incomplete')

    def install_mode(self, mode):
        h = self.b.h
        self.check(transitional=True, allow_latch=True)
        if mode == 'candidate': h.atomic_write(self.paths[2], content(self.new_armed), self.new_armed)
        target = self.marker if mode == 'candidate' else self.record['old_marker']
        h.atomic_write(self.paths[0], content(target), target)
        for item in self.configs[mode]['main']['files']:
            if item['path'] in (self.item['path'], str(self.b.original.DROPIN)):
                h.atomic_write(item['path'], content(item), item)
        h.run([h.SYSTEMCTL, 'daemon-reload'], timeout=60, log='compatible-reload.log')
        self.check(mode, allow_latch=True)

    def install(self): self.install_mode('candidate')

    def start_candidate(self):
        h = self.b.h
        h.guard_check()
        h.run([h.SYSTEMCTL, 'start', h.SERVICE], timeout=180, log='compatible-start.log')

    def healthy_mode(self, mode):
        b, h = self.b, self.b.h
        exe, checksum = (str(BINARY), self.record['binary_sha256']) if mode == 'candidate' else (self.old_exe, self.old_sha)
        argv = list(self.record['current']['process']['argv']); argv[0] = exe
        if mode == 'old_off': argv[argv.index(FLAG+'true')] = FLAG+'false'
        result = h.wait_healthy(exe, checksum, argv, seconds=240)
        self.check(mode)
        now = h.preflight(expected_exe=exe, expected_sha=checksum)
        require((now['process']['pid'], now['process']['start_ticks']) ==
                (result['process']['pid'], result['process']['start_ticks']), 'process changed after health')
        metrics = b.g.read_local_metrics()
        require(metrics.get('process/start/unix_nano', {}).get('value') == result['observer']['process_start_unix_nano'], 'metric process changed')
        b.g.small_metric_values(metrics)
        for name in b.original.GC_METRICS:
            value = metrics.get(name, {}).get('value')
            require(type(value) is int and value >= 0 and (not name.endswith('/enabled') or value == 1), 'GC metric contract failed')
        for name in ('attempts', 'selected', 'new_chunks', 'reused_raw_bytes'):
            value = metrics.get('state/history/shared/'+name, {}).get('count')
            require(type(value) is int and value >= 0, 'shared metric contract failed')
        h.save_json('compatible-health-'+mode+'.json', now)
        return result

    def healthy(self, unused): return self.healthy_mode('candidate')

    def rollback(self):
        mode = 'old_off' if self.args.mode == 'rollback' and self.args.rollback_writer == 'off' else 'old_on'
        self.stop(); self.install_mode(mode); self.start_candidate()
        return self.healthy_mode(mode)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=['prepare', 'activate', 'rollback'])
    p.add_argument('--source-revision'); p.add_argument('--script-revision', required=True)
    p.add_argument('--old-prepared-sha', required=True); p.add_argument('--prepared-sha')
    p.add_argument('--rollback-writer', choices=['on', 'off'], default='on')
    args = p.parse_args()
    require(os.geteuid() == 0, 'explicit root execution required')
    b = load(args)
    if args.mode == 'prepare': result = prepare(b, args)
    else:
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP): signal.signal(sig, b.g.interrupted)
        with open('/data/gtron/start.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            d = Deployment(b, args)
            if args.mode == 'rollback':
                result = {'rolled_back': True, 'result': b.g.protected_rollback(d)}
                b.h.save_json('compatible-manual-rollback.json', result)
            else: result = b.g.activate_transaction(d)
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if any(result.get(k) for k in ('prepared', 'active', 'rolled_back')) else 1


if __name__ == '__main__':
    try: sys.exit(main())
    except BaseException as error:
        if isinstance(error, SystemExit): raise
        print(json.dumps({'ok': False, 'error': repr(error)}), file=sys.stderr, flush=True)
        sys.exit(1)
