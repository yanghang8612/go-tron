#!/usr/bin/env python3
"""Pinned Git deployment, Python 3.6+. No database or unrelated unit writes.

prepare builds/tests without stopping gtron; activate/rollback require root.
The operator must review prepared.json and this script before explicit activate.
prepare requires --revision FULL40 and --script-revision FULL40 fetched from GitHub.
Commands never invoke a shell, cargo, kill, reset-failed, or remove a hold.
"""
import argparse
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260912-reclaim-optimization')
SOURCE = RELEASE / 'source'
BINARY = RELEASE / 'gtron'
OLD_EXE = '/data/gtron/releases/20260912-posting-prune-fair-lock/gtron'
# Last recorded binary; the operator must freshly verify all old identity pins.
OLD_SHA = '5fed1e1a9cbf46c2e24a5581d09920f34f83281d23bdbaff0ef720ddeb502836'
# Pending fresh live verification (previously observed 18236 / 4492264350).
OLD_PID = 0
OLD_START_TICKS = 0
BASE_PREFIX = 'd3793a24f6a2f339c8d23dc2e9eb0bc6f2c2ac31'
# The isolated release may be newer than the intentionally unchanged checkout.
CHECKOUT_COMMIT = '19eda11f44424f673a40b06051edcbf629b50846'
RELEASE_KIND = 'reclaim-optimization-git-20260912'
SCRIPT_PATH = 'scripts/deploy_reclaim_optimization_20260912.py'
OBSERVER_ENV = 'GTRON_DB_SPACE_OBSERVER'
CACHE_OBSERVER_ENV = 'GTRON_BASE_CACHE_REUSE_OBSERVER'
PRUNE_ENV = 'GTRON_POSTING_PRUNE'
HISTORY_RANGE_ENV = 'GTRON_HISTORY_RANGE_PRUNE'
# Set only after the real-Pebble A/B and independent safety review pass.
HISTORY_RANGE_APPROVED = True
CACHE_OBSERVER_METRIC_PREFIX = 'blockbuffer/base_cache/reuse/'
SERVICE = 'gtron.service'
SYSTEMCTL = '/bin/systemctl'
GUARD = '/usr/local/libexec/gtron-mainnet-space-guard.py'
GUARD_CONFIG = '/etc/gtron/mainnet-space-guard.json'
GLOBAL_HOLD = '/var/lib/gtron-offline-maintenance.hold'
MAIN_HOLD = '/var/lib/gtron-mainnet-disk-stop.hold'
OTHERS = ['gtron-nile.service', 'gtron-deploy.service', 'gtron-deploy.timer']
TIMER = 'gtron-mainnet-space-guard.timer'
PRESERVED_UNITS = OTHERS + [TIMER, 'gtron-mainnet-space-guard.service']
MANIFEST_NAME = 'source-manifest.json'
# Pinned to the committed source below; script commit is supplied separately.
# This manifest covers every non-document change since the running source.
SOURCE_REVISION = '4940853af2424d04b86236e4ec682dcfc32e75e2'
EXPECTED_FILES = {
    "cmd/gtron/history_load_admission.go": "b2653734d1cdd81cb75f8646ce851c08b98678122656a973cc5832a5ba202c4d",
    "cmd/gtron/history_load_admission_test.go": "b855d680da875315e58db309dc217158829e76d2e6b284624898726e5105c456",
    "cmd/gtron/history_pruner_adapter.go": "ff689769992a27edb12aebea7770c0863225a126af1cbafeae74e373452b5861",
    "cmd/gtron/history_pruner_guard_adapter_test.go": "3c2a3c684a175323fb8a95086e9e59a18585c216faf4d4014ebba34f766e2ff8",
    "cmd/gtron/history_range_prune.go": "f9b6d956c8fd51813cad67b9c53cef6944fe502fc56f0a65dd5c76b12117299f",
    "cmd/gtron/history_range_prune_test.go": "5c024a1d990a882a5f70a352279157f61e5fe0ec090eca7b5153368dbf95affb",
    "cmd/gtron/main.go": "72cd87aa9abaa14401821a020b4063599a46684bb8e3f934289a1f42938cd8c7",
    "cmd/gtron/posting_prune.go": "125170ea284dbd793bfc717dc299c9541ea12e3e9bf85d1c1f856f07daafcbc4",
    "cmd/gtron/posting_prune_test.go": "072a4a286e6a65def81bd28834c1a8d93f97cc4a7b11bf976afacd724f9386e2",
    "core/blockbuffer/history_prefix_settled.go": "a97788edad75819cb54ecaa6402d21da74e6b91513ece929aba737f9196fbcf4",
    "core/blockbuffer/history_prefix_settled_test.go": "670548415511f0be4515aa58c7a614fc034372c3267894064b4f4c6c23401a6c",
    "core/maintenance/heavy_work.go": "ad7cd7097220b36d65524dbd006a6e64760294da4d16ff27f82ec886d5f0abfd",
    "core/maintenance/heavy_work_test.go": "e574e1caa3a46411e0d030e3838b4156411ced17aed96ae1e4f325ba5ebb298b",
    "core/rawdb/accessors_state_change_posting_chunk.go": "f5fb21d52ad455da2b4594b64fa6ac9d71aa011ded9d6cbe7244b506a5b0cf0d",
    "core/rawdb/accessors_state_changeset.go": "ed595f3bef1336180d3e06b2df6eb707325765d4d86da89d4f9768e93a0f5459",
    "core/rawdb/accessors_state_changeset_range_prune.go": "a4673308bd68ef200f2f4f7cfb14518a7d7038d2f9963f00988ae64a2600c803",
    "core/rawdb/accessors_state_changeset_range_prune_ab_test.go": "2997b38a0cbcb0b565adfd7449e838432718cb55c7068099a60f6a019b5b9f12",
    "core/rawdb/accessors_state_changeset_range_prune_test.go": "4afcf378ce1ca5b712947270bfd726c111575a81bc26ec46ed00433e39214f3f",
    "core/rawdb/schema.go": "04ad29d6a3fa3b10c1f387a05177d641d6e9274705a22afeed07ce16307234f9",
    "core/state/pruning/history_range_prune_test.go": "144946fcf89896c4917f0e9d8d45f0aa83ac4efdc5fb082efd8532ce19c3d927",
    "core/state/pruning/posting_worker.go": "a2a95ea3333db0e968db256cdc226d54a1c0341a259d8202b50933c750f42083",
    "core/state/pruning/posting_worker_test.go": "98522716351a2bf4a7a4790a3cc5b74bb83e231450bb6ce2518d53602bdef9e7",
    "core/state/pruning/pruner.go": "88d86bbc7f03dd48447c25577c5f096680616b888693ab17c62e91f88fbac1b2",
    "core/state/pruning/worker.go": "029ac6068df6495eec6d62a5c0d4e8cf56e7deeafdec62f3c35b3566d173fa40",
    "core/state_change_posting_prune.go": "f0f34a8bfee97c942d0b0f3b2284c2d082575d5c55f77119952947e8a06a5243",
    "core/state_change_posting_prune_test.go": "63ef4cbee563f59e91d9f1f5d7ed4c58b183540d5aa7773c3b218992909d8ab8",
    "core/state_domain_change_prune_guard.go": "9c7989f46e948c2a78e886f6747c770eb5aaa79a6122070035dd2e1810ca7344",
    "core/state_domain_change_prune_guard_test.go": "202948ece22dd8dd6ef1dbb3b6db41d0a21d21dcb8346970f93b844d17de1955",
    "scripts/deploy_posting_prune_fair_lock_20260912.py": "1d827bb9c8f1facc7d3a0cad56320edbd45a34cb51eb6c4b56b0586db87174fc"
}
# Candidate-only contract; old/rollback health must never require these metrics.
# Frozen names; allow startup before the first permitted cleanup. The old
# release/rollback still checks only PRUNE_METRIC_CHECKS, never this contract.
RECLAIM_METRIC_CHECKS = (
    {'name': 'state/prune/history/delete/range/enabled', 'field': 'value', 'operator': 'eq', 'expected': 1},
) + tuple(
    {'name': 'state/prune/history/delete/' + name, 'field': 'value', 'operator': 'ge', 'expected': 0}
    for name in ('range/runs', 'range/rows', 'range/logical_bytes',
                 'range/fallback_blocks', 'range/work/total_ns', 'range/work/max_ns',
                 'point/rows', 'point/logical_bytes')
) + (
    {'name': 'state/prune/posting/callbacks', 'field': 'value', 'operator': 'ge', 'expected': 0},
) + tuple(
    {'name': 'state/prune/posting/phase/' + phase + '/' + kind,
     'field': 'value', 'operator': 'ge', 'expected': 0}
    for phase in ('chain_wait', 'chain_held', 'admission', 'proof', 'gate_held', 'scan', 'write')
    for kind in ('last_ns', 'max_ns', 'total_ns')
)
# All 19 worker gauges must be registered. Startup need not have received its
# first cold-prune permission yet; actual scanning/deletion is checked later.
PRUNE_METRIC_CHECKS = (
    {'name': 'state/prune/posting/enabled', 'field': 'value', 'operator': 'eq', 'expected': 1},
    {'name': 'state/prune/posting/errors', 'field': 'value', 'operator': 'eq', 'expected': 0},
) + tuple(
    {'name': 'state/prune/posting/' + name, 'field': 'value', 'operator': 'ge', 'expected': 0}
    for name in ('chunks', 'sweeps', 'deferred/boundary', 'deferred/pressure',
                 'deferred/gate', 'deferred/chain', 'resets', 'scanned/rows',
                 'scanned/bytes', 'deleted/rows', 'deleted/bytes', 'target/block',
                 'completed/block', 'last/duration_ns', 'max/duration_ns',
                 'last/scanned_rows', 'last/deleted_bytes')
)
# Entries: {name: exact metric name, field: "value" or "count",
#           operator: "eq", "ge" or "gt", expected: integer}.
# Startup requires fresh engine publication and the first account range only.
# The full 12-range rotation takes about six minutes and is verified by the
# separate 21-point capture, not by the 240-second activation health deadline.
DB_SPACE_METRIC_CHECKS = tuple(
    {'name': 'storage/engine/' + name, 'field': 'value', 'operator': 'gt', 'expected': 0}
    for name in ('disk/bytes', 'sst/current/bytes', 'wal/live/physical_bytes', 'sampled_at')
) + tuple(
    {'name': 'storage/engine/' + name, 'field': 'value', 'operator': 'ge', 'expected': 0}
    for name in ('sst/zombie/bytes', 'wal/obsolete/physical_bytes',
                 'snapshots/count', 'snapshots/earliest_sequence_number',
                 'snapshots/pinned_keys/total', 'snapshots/pinned_bytes/total',
                 'keys/tombstones/estimated_count')
) + (
    {'name': 'storage/keyspace/enabled', 'field': 'value', 'operator': 'eq', 'expected': 1},
    {'name': 'storage/keyspace/owner', 'field': 'value', 'operator': 'gt', 'expected': 0},
    {'name': 'storage/keyspace/range_count', 'field': 'value', 'operator': 'eq', 'expected': 12},
    {'name': 'storage/keyspace/account_latest/known', 'field': 'value', 'operator': 'eq', 'expected': 1},
    {'name': 'storage/keyspace/account_latest/sampled_at', 'field': 'value', 'operator': 'gt', 'expected': 0},
    {'name': 'storage/keyspace/account_latest/attempts/total', 'field': 'value', 'operator': 'ge', 'expected': 1},
    {'name': 'storage/keyspace/account_latest/errors/total', 'field': 'value', 'operator': 'eq', 'expected': 0},
    {'name': 'storage/keyspace/account_latest/sst_bytes', 'field': 'value', 'operator': 'ge', 'expected': 0},
)
# Pebble v1.1.5 can expose negative obsolete bookkeeping. Preserve raw values
# for diagnosis, but never use them as a nonnegative readiness/reclaim check.
KNOWN_ANOMALOUS_ENGINE_METRICS = (
    'storage/engine/sst/obsolete/files', 'storage/engine/sst/obsolete/bytes',
)

ALLOWED = set(EXPECTED_FILES)


def progress(message):
    print(time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + ' ' + message, flush=True)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    h = hashlib.sha256()
    with open(str(path), 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def run(argv, cwd=None, env=None, timeout=30, check=True, log=None):
    result = subprocess.run([str(x) for x in argv], cwd=str(cwd) if cwd else None,
                            env=env, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            timeout=timeout)
    output = result.stdout.decode('utf-8', 'replace')
    if log is not None:
        atomic_write(RELEASE / log, output.encode('utf-8'))
    if check and result.returncode:
        raise RuntimeError('command failed ({0}): {1}; see {2}'.format(
            result.returncode, ' '.join(str(x) for x in argv), log or output[-1500:]))
    return output, result.returncode


def atomic_write(path, data, metadata=None):
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name + '.', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            os.fchmod(stream.fileno(), metadata['mode'] if metadata else 0o600)
            if metadata:
                os.fchown(stream.fileno(), metadata['uid'], metadata['gid'])
        if metadata:
            for name, value in metadata.get('xattrs', {}).items():
                os.setxattr(temporary, name, base64.b64decode(value))
        os.replace(temporary, str(path))
        parent = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def save_json(name, value):
    atomic_write(RELEASE / name, (json.dumps(value, sort_keys=True, indent=2) + '\n').encode())


def load_json(name):
    with open(str(RELEASE / name)) as stream:
        return json.load(stream)


def saved_file(path):
    info = os.lstat(path)
    require(stat.S_ISREG(info.st_mode), 'not a regular file: ' + str(path))
    data = Path(path).read_bytes()
    attrs = {}
    for name in os.listxattr(path):
        attrs[name] = base64.b64encode(os.getxattr(path, name)).decode('ascii')
    return {'path': str(path), 'sha256': digest(data),
            'data_b64': base64.b64encode(data).decode('ascii'),
            'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid,
            'xattrs': attrs}


def show(unit):
    output, _ = run([SYSTEMCTL, 'show', unit, '--no-pager'])
    return dict(line.split('=', 1) for line in output.splitlines() if '=' in line)


def unit_snapshot(unit):
    props = show(unit)
    paths = [props.get('FragmentPath', '')] + shlex.split(props.get('DropInPaths', ''))
    require(paths[0], 'missing unit fragment: ' + unit)
    files = [saved_file(path) for path in paths if path]
    cat, _ = run([SYSTEMCTL, 'cat', unit, '--no-pager'])
    return {'properties': props, 'files': files, 'cat': cat}


def process_environment_value(pid, name):
    entries = (Path('/proc') / str(pid) / 'environ').read_bytes().split(b'\0')
    prefix = (name + '=').encode()
    values = [entry[len(prefix):].decode('utf-8', 'strict')
              for entry in entries if entry.startswith(prefix)]
    require(len(values) <= 1, 'duplicate environment entries for ' + name)
    return values[0] if values else None


def observer_process_environment(pid):
    return process_environment_value(pid, OBSERVER_ENV)


def cache_observer_process_environment(pid):
    return process_environment_value(pid, CACHE_OBSERVER_ENV)


def posting_prune_process_environment(pid):
    return process_environment_value(pid, PRUNE_ENV)


def history_range_process_environment(pid):
    return process_environment_value(pid, HISTORY_RANGE_ENV)


def expected_history_range_environment(executable):
    require(executable in (OLD_EXE, str(BINARY)), 'unrecognized release for history-range environment')
    return '1' if executable == str(BINARY) else None


def environment_settings(properties):
    settings = {}
    for entry in shlex.split(properties.get('Environment', '')):
        key, separator, value = entry.partition('=')
        require(separator and key and key not in settings, 'invalid effective Environment property')
        settings[key] = value
    return settings


def verify_observer_runtime(pid, expected_exe):
    expected_range = expected_history_range_environment(expected_exe)
    require(history_range_process_environment(pid) == expected_range,
            'history-range process environment differs for expected release')
    require(observer_process_environment(pid) == '1', 'DB-space observer is not enabled in running process')
    require(cache_observer_process_environment(pid) == '1',
            'existing cache observer is not enabled in running process')
    require(posting_prune_process_environment(pid) == '1',
            'existing posting-prune process environment is not enabled')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open('http://127.0.0.1:6062/debug/metrics', timeout=10) as response:
        data = json.loads(response.read(8 * 1024 * 1024).decode('utf-8'))
    metrics = data['metrics']
    values = {name: metrics.get(CACHE_OBSERVER_METRIC_PREFIX + name, {}).get('value')
              for name in ['enabled', 'owner_id', 'metadata_bytes']}
    require(type(values['enabled']) is int and values['enabled'] == 1,
            'observer enabled metric is not 1')
    require(type(values['owner_id']) is int and values['owner_id'] > 0,
            'observer owner identity missing')
    require(type(values['metadata_bytes']) is int and 0 < values['metadata_bytes'] <= 512 * 1024,
            'observer metadata bound not verified')
    process_start = metrics.get('process/start/unix_nano', {}).get('value')
    require(type(process_start) is int and process_start > 0, 'process metric identity missing')
    verify_db_space_metrics(metrics)
    anomalous = {name: metrics.get(name, {}).get('value') for name in KNOWN_ANOMALOUS_ENGINE_METRICS}
    require(all(type(value) is int for value in anomalous.values()),
            'known-anomalous engine metrics missing/noninteger')
    verify_metric_checks(metrics, PRUNE_METRIC_CHECKS, 'posting-prune')
    reclaim = {}
    if expected_range == '1':
        verify_metric_checks(metrics, RECLAIM_METRIC_CHECKS, 'reclaim candidate')
        reclaim = {check['name']: metrics[check['name']][check['field']]
                   for check in RECLAIM_METRIC_CHECKS}
    return {'process_start_unix_nano': process_start,
            'cache_observer': values,
            'history_range_environment': expected_range,
            'reclaim_candidate': reclaim,
            'db_space': {check['name']: metrics[check['name']][check['field']]
                         for check in DB_SPACE_METRIC_CHECKS},
            'engine_obsolete_raw': anomalous,
            'posting_prune': {check['name']: metrics[check['name']][check['field']]
                              for check in PRUNE_METRIC_CHECKS}}


def validate_metric_contract(checks, label):
    require(checks, label + ' metric contract is not frozen')
    seen = set()
    for check in checks:
        require(set(check) == {'name', 'field', 'operator', 'expected'},
                'invalid ' + label + ' metric contract entry')
        require(isinstance(check['name'], str) and check['name'] and check['name'] not in seen,
                'missing or duplicate ' + label + ' metric name')
        require(check['field'] in ('value', 'count') and
                check['operator'] in ('eq', 'ge', 'gt') and type(check['expected']) is int,
                'invalid ' + label + ' metric predicate')
        seen.add(check['name'])


def verify_metric_checks(metrics, checks, label):
    validate_metric_contract(checks, label)
    for check in checks:
        value = metrics.get(check['name'], {}).get(check['field'])
        require(type(value) is int, 'missing/noninteger ' + label + ' metric: ' + check['name'])
        expected = check['expected']
        ok = (value == expected if check['operator'] == 'eq' else
              value >= expected if check['operator'] == 'ge' else value > expected)
        require(ok, label + ' metric readiness failed: ' + check['name'])


def validate_db_space_contract():
    validate_metric_contract(DB_SPACE_METRIC_CHECKS, 'DB-space engine/observer')


def verify_db_space_metrics(metrics):
    verify_metric_checks(metrics, DB_SPACE_METRIC_CHECKS, 'DB-space engine/observer')


def process_identity(pid):
    root = Path('/proc') / str(pid)
    exe = os.readlink(str(root / 'exe'))
    argv = [x.decode('utf-8', 'strict') for x in (root / 'cmdline').read_bytes().split(b'\0') if x]
    status = (root / 'stat').read_text()
    start_ticks = int(status[status.rfind(')') + 2:].split()[19])
    return {'pid': pid, 'exe': exe, 'exe_sha256': file_sha(root / 'exe'),
            'argv': argv, 'start_ticks': start_ticks,
            'observer_environment': observer_process_environment(pid),
            'cache_observer_environment': cache_observer_process_environment(pid),
            'posting_prune_environment': posting_prune_process_environment(pid),
            'history_range_environment': history_range_process_environment(pid),
            'stat': status, 'status': (root / 'status').read_text()}


def hold_snapshot(path):
    if not os.path.lexists(path):
        return {'exists': False}
    return {'exists': True, 'file': saved_file(path)}


def guard_check():
    require(os.path.lexists(GLOBAL_HOLD), 'global maintenance hold unexpectedly absent')
    require(not os.path.lexists(MAIN_HOLD), 'mainnet stop hold exists; never remove it here')
    require(show(TIMER).get('ActiveState') == 'active', 'space guard timer is not active')
    for unit in OTHERS:
        require(show(unit).get('ActiveState') == 'inactive', unit + ' is not inactive')
    command = ['/usr/bin/python3', GUARD, 'check']
    if os.geteuid() == 0:
        runuser = shutil.which('runuser')
        require(runuser is not None, 'runuser is required for the service-user guard check')
        command = [runuser, '-u', 'java-tron', '--'] + command
    else:
        require(pwd.getpwuid(os.geteuid()).pw_name == 'java-tron',
                'prepare must run as java-tron or root for the guard check')
    output, _ = run(command)
    result = json.loads(output)
    require(result.get('ok') is True, 'space guard failed')
    return result


def preflight(expected_pid=None, expected_exe=None, expected_sha=None):
    guard = guard_check()
    main = unit_snapshot(SERVICE)
    require(main['properties'].get('ActiveState') == 'active', 'mainnet is not active')
    pid = int(main['properties'].get('MainPID', '0'))
    if expected_pid is not None:
        require(pid == expected_pid, 'live PID changed; inspect before preparing/activating')
    process = process_identity(pid)
    if expected_pid == OLD_PID:
        require(process['start_ticks'] == OLD_START_TICKS, 'pinned running process start ticks changed')
    require(process['posting_prune_environment'] == '1',
            'posting-prune process environment changed from expected release')
    require(environment_settings(main['properties']).get(PRUNE_ENV) == '1',
            'posting-prune unit environment changed from expected release')
    require(main['properties'].get('MemoryLimit') == str(40 * 1024 ** 3),
            'mainnet MemoryLimit differs from the pinned 40 GiB configuration')
    require(process['observer_environment'] == '1', 'existing DB-space observer process environment changed')
    require(environment_settings(main['properties']).get(OBSERVER_ENV) == '1',
            'existing DB-space observer unit environment changed')
    require(process['cache_observer_environment'] == '1',
            'existing cache observer process environment changed')
    require(environment_settings(main['properties']).get(CACHE_OBSERVER_ENV) == '1',
            'existing cache observer unit environment changed')
    expected_range = expected_history_range_environment(process['exe'])
    require(process['history_range_environment'] == expected_range,
            'history-range process environment changed from expected release')
    require(environment_settings(main['properties']).get(HISTORY_RANGE_ENV) == expected_range,
            'history-range unit environment changed from expected release')
    if expected_exe:
        require(process['exe'] == expected_exe, 'live executable changed')
        require(process['argv'][0] == expected_exe, 'unexpected argv[0]')
    if expected_sha:
        require(process['exe_sha256'] == expected_sha, 'live executable checksum changed')
    stats = {}
    for path in ['/data', '/']:
        v = os.statvfs(path)
        stats[path] = {'free_bytes': v.f_bavail * v.f_frsize, 'free_inodes': v.f_favail}
    return {'captured_at': time.time(), 'main': main, 'process': process,
            'others': {unit: unit_snapshot(unit) for unit in PRESERVED_UNITS},
            'holds': {path: hold_snapshot(path) for path in [GLOBAL_HOLD, MAIN_HOLD]},
            'guard_config': saved_file(GUARD_CONFIG), 'guard_script': saved_file(GUARD),
            'guard_check': guard, 'filesystem': stats}


def effective_exec_file(snapshot):
    effective = []
    for item in snapshot['main']['files']:
        text = base64.b64decode(item['data_b64']).decode('utf-8')
        require(HISTORY_RANGE_ENV not in text, 'history-range already mentioned in original unit files')
        section = ''
        logical = ''
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
                if not value:
                    effective = []
                else:
                    effective.append((item, value))
            logical = ''
        require(not logical, 'unterminated unit continuation')
    require(len(effective) == 1, 'expected exactly one effective ExecStart')
    item, value = effective[0]
    require(value.split()[0] == OLD_EXE, 'effective ExecStart is not the known release')
    require(base64.b64decode(item['data_b64']).count(OLD_EXE.encode()) == 1,
            'old executable must appear exactly once in its live file')
    require(item['uid'] == 0 and not item['mode'] & 0o022, 'unsafe unit ownership or permissions')
    return item


def same_configuration(before, now):
    for key in ['holds', 'guard_config', 'guard_script']:
        require(before[key] == now[key], key + ' changed after prepare')
    require(before['main']['files'] == now['main']['files'], 'main unit files changed after prepare')
    for unit in PRESERVED_UNITS:
        require(before['others'][unit]['files'] == now['others'][unit]['files'],
                unit + ' files changed after prepare')
    # Properties include volatile counters; retain only effective settings relevant to startup.
    for key in ['ExecStart', 'Environment', 'EnvironmentFiles', 'User', 'Group',
                'WorkingDirectory', 'TimeoutStopUSec', 'MemoryLimit', 'MemorySoftLimit',
                'Restart', 'Conditions', 'ExecStartPre', 'KillSignal']:
        require(before['main']['properties'].get(key) == now['main']['properties'].get(key),
                'effective service property changed: ' + key)


def verify_after_configuration(before, now, item, updated):
    expected = json.loads(json.dumps(before))
    for entry in expected['main']['files']:
        if entry['path'] == item['path']:
            entry['data_b64'] = base64.b64encode(updated).decode('ascii')
            entry['sha256'] = digest(updated)
    for key in ['holds', 'guard_config', 'guard_script']:
        require(expected[key] == now[key], key + ' changed during activation')
    require(expected['main']['files'] == now['main']['files'], 'unexpected main unit edit')
    for unit in PRESERVED_UNITS:
        require(expected['others'][unit]['files'] == now['others'][unit]['files'],
                unit + ' configuration changed during activation')
    expected_environment = environment_settings(before['main']['properties'])
    require(HISTORY_RANGE_ENV not in expected_environment, 'history-range already configured before activation')
    expected_environment[HISTORY_RANGE_ENV] = '1'
    require(environment_settings(now['main']['properties']) == expected_environment,
            'effective environment differs beyond approved history-range addition')
    require(now['process']['observer_environment'] == '1', 'running DB-space observer environment differs')
    require(now['process']['cache_observer_environment'] == '1',
            'running cache observer environment differs')
    require(now['process']['posting_prune_environment'] == '1',
            'running posting-prune environment differs')
    require(now['process']['history_range_environment'] == '1',
            'running history-range environment differs')
    for key in ['EnvironmentFiles', 'User', 'Group', 'WorkingDirectory',
                'TimeoutStopUSec', 'MemoryLimit', 'MemorySoftLimit', 'Restart', 'KillSignal']:
        require(expected['main']['properties'].get(key) == now['main']['properties'].get(key),
                'unexpected changed effective setting: ' + key)


def safe_extract_base(archive):
    SOURCE.mkdir()
    with tarfile.open(str(archive)) as bundle:
        for member in bundle:
            rel = Path(member.name)
            require(not rel.is_absolute() and '..' not in rel.parts, 'unsafe base archive path')
            target = SOURCE / rel
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                require(member.isfile(), 'unsupported base archive member: ' + member.name)
                target.parent.mkdir(parents=True, exist_ok=True)
                with bundle.extractfile(member) as stream:
                    target.write_bytes(stream.read())
                os.chmod(str(target), member.mode & 0o777)


def resolve_commit(ref):
    output, _ = run(['git', 'rev-parse', '--verify', ref + '^{commit}'], cwd=REPO)
    value = output.strip()
    require(re.fullmatch(r'[0-9a-f]{40}', value), 'invalid resolved commit: ' + ref)
    return value


def validate_revision(revision, script_revision):
    require(re.fullmatch(r'[0-9a-f]{40}', SOURCE_REVISION), 'source revision pin is not frozen')
    require(revision == SOURCE_REVISION, 'revision differs from frozen source commit')
    validate_db_space_contract()
    validate_metric_contract(PRUNE_METRIC_CHECKS, 'posting-prune')
    validate_metric_contract(RECLAIM_METRIC_CHECKS, 'reclaim candidate')
    require(HISTORY_RANGE_APPROVED is True, 'history-range A/B rollout decision is not frozen')
    require(type(OLD_PID) is int and OLD_PID > 0, 'old process PID pin is not frozen')
    require(type(OLD_START_TICKS) is int and OLD_START_TICKS > 0, 'old process start tick pin is not frozen')
    require(re.fullmatch(r'[0-9a-f]{64}', OLD_SHA), 'invalid rollback SHA-256 pin')
    require(isinstance(revision, str) and re.fullmatch(r'[0-9a-f]{40}', revision),
            'prepare --revision must be an exact 40-character lowercase commit SHA')
    require(resolve_commit(revision) == revision, 'revision does not resolve exactly')
    require(EXPECTED_FILES, 'release file manifest is not populated')
    base = resolve_commit(BASE_PREFIX)
    checkout = resolve_commit(CHECKOUT_COMMIT)
    require(checkout == CHECKOUT_COMMIT, 'pinned checkout does not resolve exactly')
    require(run(['git', 'merge-base', '--is-ancestor', checkout, base], cwd=REPO,
                check=False)[1] == 0, 'pinned checkout is not an ancestor of the running release')
    require(run(['git', 'merge-base', '--is-ancestor', base, revision], cwd=REPO,
                check=False)[1] == 0, 'candidate does not descend from the running base')
    diff, _ = run(['git', 'diff', '--name-status', '--no-renames', base, revision, '--'], cwd=REPO)
    changed = set()
    for line in diff.splitlines():
        parts = line.split('\t')
        require(len(parts) == 2 and parts[0] in ('A', 'M'), 'unexpected diff operation: ' + line)
        name = parts[1]
        require(name in ALLOWED or name == SCRIPT_PATH or
                (name.startswith('docs/') and name.endswith('.md')),
                'candidate includes an unrelated changed path: ' + name)
        changed.add(name)
    require(ALLOWED <= changed, 'candidate must contain every pinned changed file')
    require(isinstance(script_revision, str) and re.fullmatch(r'[0-9a-f]{40}', script_revision),
            'prepare --script-revision must be an exact 40-character lowercase commit SHA')
    require(resolve_commit(script_revision) == script_revision, 'script revision does not resolve exactly')
    require(run(['git', 'merge-base', '--is-ancestor', revision, script_revision], cwd=REPO,
                check=False)[1] == 0, 'ops script commit does not contain the candidate ancestor')
    release_ref = 'refs/remotes/origin/master'
    release_tip = resolve_commit(release_ref)
    require(release_tip == script_revision, 'fetched master tip differs from pinned script commit')
    admitted_refs = [{'ref': release_ref, 'commit': release_tip}]
    script_refs = admitted_refs
    script_bytes = subprocess.check_output(['git', 'show', script_revision + ':' + SCRIPT_PATH],
                                           cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)
    require(digest(script_bytes) == file_sha(__file__),
            'executing script bytes differ from the pinned Git commit')
    head = resolve_commit('HEAD')
    require(head in (checkout, base, revision),
            'server checkout HEAD is neither pinned checkout, running base nor candidate')
    return {'revision': revision, 'running_base_commit': base,
            'pinned_checkout_commit': checkout, 'checkout_head': head,
            'admitted_refs': admitted_refs, 'changed_files': sorted(changed), 'name_status': diff,
            'script_revision': script_revision, 'script_sha256': digest(script_bytes),
            'script_admitted_refs': script_refs}


def verify_source_files(revision):
    hashes = {}
    for name in sorted(ALLOWED):
        target = SOURCE / name
        require(target.is_file() and not target.is_symlink(), 'source missing or symlink: ' + name)
        hashes[name] = file_sha(target)
        require(hashes[name] == EXPECTED_FILES[name], 'pinned source checksum mismatch: ' + name)
    source_manifest = {'source_commit': revision, 'files': hashes}
    save_json(MANIFEST_NAME, source_manifest)
    return source_manifest


def verify_prepared(record):
    require(record.get('prepared') is True and record.get('release_kind') == RELEASE_KIND and
            re.fullmatch(r'[0-9a-f]{40}', record.get('source_commit', '')),
            'missing successful prepare record')
    require(BINARY.is_file() and not BINARY.is_symlink(), 'candidate binary missing or symlink')
    require(file_sha(BINARY) == record['binary_sha256'], 'candidate checksum changed')
    require(file_sha(OLD_EXE) == OLD_SHA, 'rollback release checksum changed')


def prepare(args):
    progress('prepare: validating pinned Git revision and deployment scope')
    admission = validate_revision(args.revision, args.script_revision)
    revision = admission['revision']
    RELEASE.mkdir(parents=True, exist_ok=True)
    lock = open(str(RELEASE / '.prepare.lock'), 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if (RELEASE / 'prepared.json').exists():
        record = load_json('prepared.json')
        require(record.get('source_commit') == revision, 'release directory belongs to another commit')
        require(record.get('script_revision') == args.script_revision and
                record.get('script_sha256') == admission['script_sha256'], 'prepared script identity changed')
        verify_prepared(record)
        print(json.dumps({'prepared': True, 'already_prepared': True,
                          'source_commit': revision, 'binary_sha256': record['binary_sha256']}), flush=True)
        return
    require(not SOURCE.exists(), 'incomplete source directory exists; inspect it, do not overwrite')
    dirty, _ = run(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=REPO)
    require(all(line[3:] == 'third_party/librustzcash' for line in dirty.splitlines()),
            'unexpected tracked server changes')
    progress('prepare: capturing live service, rollback binary, guards, holds and configuration')
    before = preflight(OLD_PID, OLD_EXE, OLD_SHA)
    active_file = effective_exec_file(before)
    save_json('preflight.json', before)
    save_json('git-admission.json', admission)
    go = shutil.which('go')
    require(go is not None, 'go not found in PATH')
    env = dict(os.environ, CGO_ENABLED='1', GOMAXPROCS='2', GOFLAGS='', GOTOOLCHAIN='local')
    env[CACHE_OBSERVER_ENV] = '1'
    env.pop(OBSERVER_ENV, None)
    env.pop(PRUNE_ENV, None)
    env.pop(HISTORY_RANGE_ENV, None)
    go_version, _ = run([go, 'version'], env=env)
    require(go_version.strip() == 'go version go1.25.5 linux/amd64', 'unexpected Go toolchain')
    progress('prepare: git archive ' + revision + ' into isolated source directory')
    source_archive = RELEASE / 'source.tar'
    run(['git', 'archive', '--format=tar', '--output=' + str(source_archive), revision], cwd=REPO)
    safe_extract_base(source_archive)
    manifest = verify_source_files(revision)
    rust = REPO / 'third_party/librustzcash'
    rust_library = rust / 'target/release/librustzcash.a'
    require(rust_library.is_file(), 'native librustzcash static library missing')
    rust_sha = file_sha(rust_library)
    rust_target = SOURCE / 'third_party/librustzcash'
    if rust_target.exists():
        require(rust_target.is_dir() and not any(rust_target.iterdir()), 'nonempty archived rust path')
        rust_target.rmdir()
    rust_target.parent.mkdir(parents=True, exist_ok=True)
    rust_target.symlink_to(rust, target_is_directory=True)
    progress('prepare: complete native rawdb, maintenance, pruning, snapshots, sync and commitment-domain tests')
    run([go, 'test', '-p', '2', '-tags', 'sapling', './core/rawdb', './core/rawdb/pebbledb', './net/...', './core/state/domains',
         './core/blockbuffer', './core/maintenance', './core/state/pruning', './core/state/snapshots', '-count=1', '-timeout=300s'],
        cwd=SOURCE, env=env, timeout=1800, log='native-affected-tests.log')
    progress('prepare: native storage and maintenance tests with all four opt-ins enabled')
    observer_env = dict(env)
    observer_env[OBSERVER_ENV] = '1'
    observer_env[PRUNE_ENV] = '1'
    observer_env[HISTORY_RANGE_ENV] = '1'
    run([go, 'test', '-p', '2', '-tags', 'sapling', './core/rawdb', './core/rawdb/pebbledb', './core/blockbuffer',
         './core/maintenance', './core/state/pruning', './core/state/snapshots',
         '-count=1', '-timeout=300s'], cwd=SOURCE, env=observer_env,
        timeout=1800, log='native-observer-enabled-tests.log')
    progress('prepare: canonical execution, state access and native cryptography regressions')
    test_pattern = ('Test.*(StateChangePostingPruneGuard|PostingPrune|StateDomainChangePruneGuard|HistoryRangePrune|HotHistoryPrune|'
                    'ProcessBlockPublishesBoundaryReadyAsyncVMRetry|StateDBCop|'
                    'StateCode|StrictObject|CodeVerification|GetCodeStrict|'
                    'EmptyRootsVector|AppendCommitmentsVector|CombineKnownDepth25)')
    run([go, 'test', '-p', '2', '-tags', 'sapling', './cmd/gtron', './core',
         './core/state', './core/zksnark', '-run', test_pattern, '-count=1', '-timeout=300s'],
        cwd=SOURCE, env=env, timeout=1800, log='native-targeted-tests.log')
    progress('prepare: executing native Sapling availability and Pedersen probe')
    probe = RELEASE / 'native-sapling-probe.go'
    probe.write_text('package main\nimport("fmt";"os";"github.com/tronprotocol/go-tron/core/zksnark")\n'
                     'func main(){if !zksnark.Available(){os.Exit(2)};'
                     'v,e:=zksnark.Uncommitted();if e!=nil{panic(e)};'
                     'fmt.Printf("nativeSapling=true uncommitted=%x\\n",v)}\n')
    run([go, 'run', '-p', '2', '-tags', 'sapling', str(probe)], cwd=SOURCE, env=env,
        timeout=600, log='native-sapling-probe.log')
    require('nativeSapling=true' in (RELEASE / 'native-sapling-probe.log').read_text(),
            'native Sapling probe did not attest availability')
    progress('prepare: building Linux amd64 candidate with CGO_ENABLED=1 and tags=sapling')
    run([go, 'build', '-p', '2', '-tags', 'sapling', '-o', str(BINARY), './cmd/gtron'],
        cwd=SOURCE, env=env, timeout=1800, log='build.log')
    os.chmod(str(BINARY), 0o755)
    info, _ = run([go, 'version', '-m', str(BINARY)], env=env, log='build-info.txt')
    require('CGO_ENABLED=1' in info and '-tags=sapling' in info and 'GOARCH=amd64' in info and
            'GOOS=linux' in info, 'candidate build settings mismatch')
    run(['file', str(BINARY)], log='binary-file.txt')
    run(['ldd', str(BINARY)], log='binary-ldd.txt')
    run([str(BINARY), 'version'], cwd=RELEASE, log='binary-version.txt')
    progress('prepare: rechecking source hashes, original process and unchanged configuration')
    verify_source_files(revision)
    after = preflight(OLD_PID, OLD_EXE, OLD_SHA)
    same_configuration(before, after)
    require(before['process']['start_ticks'] == after['process']['start_ticks'], 'old PID reused')
    require(file_sha(rust_library) == rust_sha, 'native static library changed during build')
    record = {'prepared': True, 'prepared_at': time.time(), 'release_kind': RELEASE_KIND,
              'source_commit': revision, 'running_base_commit': admission['running_base_commit'],
              'script_revision': admission['script_revision'], 'script_sha256': admission['script_sha256'],
              'source_archive_sha256': file_sha(source_archive), 'source_manifest': manifest,
              'binary_sha256': file_sha(BINARY), 'rust_library_sha256': rust_sha,
              'go_version': go_version.strip(), 'git_admission': admission,
              'preflight': before, 'exec_file': active_file}
    save_json('prepared.json', record)
    atomic_write(RELEASE / 'source-commit', (revision + '\n').encode())
    atomic_write(RELEASE / 'SHA256SUMS', (record['binary_sha256'] + '  gtron\n').encode())
    progress('prepare: complete; service was not stopped or restarted')
    print(json.dumps({'prepared': True, 'release': str(RELEASE), 'source_commit': revision,
                      'binary_sha256': record['binary_sha256'], 'exec_file': active_file['path']}), flush=True)


def wallet_head():
    # Ignore any proxy variables: this is always the server loopback wallet.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open('http://127.0.0.1:8090/wallet/getnowblock', timeout=5) as response:
        data = json.loads(response.read(8 * 1024 * 1024).decode('utf-8'))
    value = data['block_header']['raw_data']['number']
    require(type(value) is int and value >= 0, 'invalid wallet head')
    return value


def wait_healthy(expected_exe, expected_sha, expected_argv, seconds=240):
    deadline = time.monotonic() + seconds
    first = None
    first_identity = None
    last_error = None
    while time.monotonic() < deadline:
        try:
            props = show(SERVICE)
            require(props.get('ActiveState') == 'active', 'service not active')
            proc = process_identity(int(props.get('MainPID', '0')))
            require(proc['exe'] == expected_exe and proc['exe_sha256'] == expected_sha,
                    'unexpected running executable')
            require(proc['argv'] == expected_argv, 'running arguments differ beyond executable')
            require(proc['cache_observer_environment'] == '1',
                    'existing cache observer must remain enabled')
            require(proc['observer_environment'] == '1', 'existing DB-space observer must remain enabled')
            require(proc['posting_prune_environment'] == '1', 'existing posting-prune must remain enabled')
            require(proc['history_range_environment'] == expected_history_range_environment(expected_exe),
                    'history-range environment differs for expected executable')
            head = wallet_head()
            identity = (proc['pid'], proc['start_ticks'])
            if first is None or identity != first_identity:
                first = head
                first_identity = identity
            elif head > first:
                observer = verify_observer_runtime(proc['pid'], expected_exe)
                after_props = show(SERVICE)
                after = process_identity(int(after_props.get('MainPID', '0')))
                require(after_props.get('ActiveState') == 'active' and
                        (after['pid'], after['start_ticks']) == identity,
                        'process changed while verifying wallet and metrics')
                return {'process': proc, 'head_first': first, 'head_last': head,
                        'observer': observer, 'verified_at': time.time()}
        except (OSError, ValueError, KeyError, RuntimeError) as exc:
            last_error = str(exc)
        time.sleep(3)
    raise RuntimeError('service did not become healthy and advance head: ' + str(last_error))


def activation_files(record):
    item = record['exec_file']
    original = base64.b64decode(item['data_b64'])
    require(original.count(OLD_EXE.encode()) == 1, 'invalid original ExecStart backup')
    require(HISTORY_RANGE_ENV.encode() not in original, 'history-range already mentioned in original ExecStart file')
    replacement = original.replace(OLD_EXE.encode(), str(BINARY).encode(), 1)
    require(replacement.replace(str(BINARY).encode(), OLD_EXE.encode(), 1) == original,
            'candidate config differs beyond executable')
    # A fresh Service section is valid regardless of the original final section.
    addition = ('\n[Service]\nEnvironment=' + HISTORY_RANGE_ENV + '=1\n').encode()
    updated = replacement + addition
    return item, original, updated


def rollback_internal(record):
    # A missing/corrupt candidate must never prevent restoration of the known old binary.
    require(record.get('prepared') is True and record.get('release_kind') == RELEASE_KIND and
            re.fullmatch(r'[0-9a-f]{40}', record.get('source_commit', '')),
            'missing successful prepare record')
    require(file_sha(OLD_EXE) == OLD_SHA, 'rollback release checksum changed')
    item, original, updated = activation_files(record)
    current = Path(item['path']).read_bytes()
    require(current in (original, updated), 'unit changed externally; refusing to overwrite')
    props = show(SERVICE)
    if props.get('ActiveState') == 'active':
        proc = process_identity(int(props.get('MainPID', '0')))
        if current == original and proc['exe'] == OLD_EXE and proc['exe_sha256'] == OLD_SHA:
            result = wait_healthy(OLD_EXE, OLD_SHA, record['preflight']['process']['argv'])
            save_json('activation-state.json', {'phase': 'rolled_back', 'result': result})
            return result
        require(proc['exe'] in (OLD_EXE, str(BINARY)), 'unrelated live executable; refusing to stop')
    save_json('activation-state.json', {'phase': 'rollback_stopping', 'time': time.time()})
    run([SYSTEMCTL, 'stop', SERVICE], timeout=660, log='rollback-stop.log')
    require(int(show(SERVICE).get('MainPID', '0')) == 0, 'service still running; will not force stop')
    require(Path(item['path']).read_bytes() in (original, updated), 'unit changed while stopping')
    atomic_write(item['path'], original, item)
    run([SYSTEMCTL, 'daemon-reload'], timeout=60, log='rollback-daemon-reload.log')
    guard_check()
    run([SYSTEMCTL, 'start', SERVICE], timeout=180, log='rollback-start.log')
    result = wait_healthy(OLD_EXE, OLD_SHA, record['preflight']['process']['argv'])
    save_json('activation-state.json', {'phase': 'rolled_back', 'result': result})
    return result


def activate(args):
    require(os.geteuid() == 0, 'activate requires explicit root execution')
    record = load_json('prepared.json')
    verify_prepared(record)
    item, original, updated = activation_files(record)
    current = Path(item['path']).read_bytes()
    props = show(SERVICE)
    pid = int(props.get('MainPID', '0'))
    if current == updated and pid:
        proc = process_identity(pid)
        if proc['exe'] == str(BINARY) and proc['exe_sha256'] == record['binary_sha256']:
            result = wait_healthy(str(BINARY), record['binary_sha256'],
                                  [str(BINARY)] + record['preflight']['process']['argv'][1:])
            guard_check()
            save_json('activation-state.json', {'phase': 'active', 'result': result})
            print(json.dumps({'active': True, 'already_active': True, 'pid': result['process']['pid']}))
            return
    require(current == original, 'interrupted/changed activation; use explicit rollback first')
    now = preflight(OLD_PID, OLD_EXE, OLD_SHA)
    same_configuration(record['preflight'], now)
    require(now['process']['start_ticks'] == record['preflight']['process']['start_ticks'],
            'old process identity changed')
    require(now['process']['argv'] == record['preflight']['process']['argv'], 'old argv changed')
    save_json('activation-before.json', now)
    save_json('activation-state.json', {'phase': 'switching', 'time': time.time()})
    changed = False
    try:
        # Roll back even if replace succeeds but a later directory fsync fails.
        changed = True
        atomic_write(item['path'], updated, item)
        run([SYSTEMCTL, 'daemon-reload'], timeout=60, log='activate-daemon-reload.log')
        run([SYSTEMCTL, 'restart', SERVICE], timeout=900, log='activate-restart.log')
        result = wait_healthy(str(BINARY), record['binary_sha256'],
                              [str(BINARY)] + record['preflight']['process']['argv'][1:])
        guard_check()
        after = preflight(expected_exe=str(BINARY), expected_sha=record['binary_sha256'])
        verify_after_configuration(record['preflight'], after, item, updated)
        save_json('activation-after.json', after)
        save_json('activation-state.json', {'phase': 'active', 'result': result})
        print(json.dumps({'active': True, 'pid': result['process']['pid'],
                          'head_first': result['head_first'], 'head_last': result['head_last'],
                          'binary_sha256': record['binary_sha256']}))
    except BaseException as failure:
        save_json('activation-failure.json', {'error': repr(failure), 'time': time.time()})
        if changed:
            try:
                rollback_internal(record)
            except BaseException as rollback_error:
                save_json('rollback-failure.json', {'error': repr(rollback_error), 'time': time.time()})
                raise RuntimeError('activation failed and rollback needs operator attention: ' +
                                   str(failure) + '; ' + str(rollback_error))
        raise


def status(args):
    props = show(SERVICE)
    result = {'service': props.get('ActiveState'), 'pid': int(props.get('MainPID', '0')),
              'prepared': (RELEASE / 'prepared.json').exists()}
    if result['pid']:
        proc = process_identity(result['pid'])
        result.update({key: proc[key] for key in ['exe', 'exe_sha256', 'start_ticks']})
    if (RELEASE / 'activation-state.json').exists():
        result['deployment_phase'] = load_json('activation-state.json').get('phase')
    print(json.dumps(result, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['prepare', 'activate', 'rollback', 'status'])
    parser.add_argument('--revision', help='prepare only: exact fetched Git commit, 40 lowercase hex characters')
    parser.add_argument('--script-revision', help='prepare only: exact fetched ops-script Git commit')
    args = parser.parse_args()
    if args.mode == 'status':
        status(args)
    elif args.mode == 'prepare':
        prepare(args)
    else:
        require(os.geteuid() == 0, args.mode + ' requires explicit root execution')
        # Nonblocking activation lock shares the existing deploy script's inode.
        with open('/data/gtron/start.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if args.mode == 'activate':
                activate(args)
            else:
                result = rollback_internal(load_json('prepared.json'))
                print(json.dumps({'rolled_back': True, 'pid': result['process']['pid']}))


if __name__ == '__main__':
    try:
        main()
    except BaseException as exc:
        print(json.dumps({'ok': False, 'error': repr(exc)}), file=sys.stderr)
        sys.exit(1)
