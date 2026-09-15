#!/usr/bin/env python3
"""Admit, activate or recover one pinned R4/cache reader; no build or fetch.

admit is read-only for the service and saves a private, durable transaction.
activate requires that exact admission SHA. rollback uses only the saved old
compatible identity, so failed/missing candidate evidence cannot block recovery.
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
import tempfile
import time
import traceback
import types
import urllib.request
import urllib.error
import uuid

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260915-history-shared-read')
BINARY = RELEASE / 'gtron-inspect'
TRANSACTION = RELEASE / 'activation'
OLD_BINARY = '/data/gtron/releases/20260914-cold-recovery/gtron'
OLD_SHA = '409b08c1e916cc9953a5f66e91ecae897e265c43471227af2bf1b208b566d533'
OLD_SOURCE = '7b2b374ae0d66d19971b72269f3fc14aeb1a8e53'
OLD_PID, OLD_TICKS = 27267, 4517828938
EXEC = '/etc/systemd/system/gtron.service.d/zz-genesis-cpu-20260907.conf'
DROPIN = '/etc/systemd/system/gtron.service.d/zz-history-shared-reader.conf'
GUARD = '/usr/local/libexec/gtron-history-shared-reader-guard.py'
SPACE_GUARD = '/usr/local/libexec/gtron-mainnet-space-guard.py'
SPACE_CONFIG = '/etc/gtron/mainnet-space-guard.json'
SPACE_EVIDENCE = '/data/gtron/releases/20260915-shared-read-activation-evidence/space-dependencies.json'
SPACE_HOLD = '/var/lib/gtron-mainnet-disk-stop.hold'
MARKER = '/data/gtron/main/HISTORY_SHARED_CHUNKS_REQUIRES_READER_V3.json'
GUARD_SHA = '036eb75572646f81ffdb2da1d0f270b4f3ff74ea1b573bdcff7e1d6114c52103'
ABBA_PATH = 'scripts/benchmark_history_chunk_cache_20260915.py'
ABBA_SHA = 'd5a7fef3e3386cbf570e282b0311eb41a21fe8fc1d02460cefeddabb85b50b90'
SCRIPT = 'scripts/activate_history_shared_read_20260915.py'
FLAGS = ['--history.shared-read-workers=4', '--history.shared-chunk-cache=true']
SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
CONFIG_KEYS = ('FragmentPath', 'DropInPaths', 'ExecStart', 'ExecStartPre', 'ExecStartPost',
               'Environment', 'EnvironmentFiles', 'WorkingDirectory', 'User', 'Group',
               'MemoryLimit', 'MemorySoftLimit', 'CPUQuotaPerSecUSec', 'CPUAffinity',
               'LimitNOFILE', 'Restart', 'KillMode', 'TimeoutStopUSec')


class OperatorInterrupted(BaseException):
    """Not InterruptedError: selectors may swallow that exception as EINTR."""


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def run(argv, timeout=60):
    return subprocess.check_output(argv, cwd=str(REPO), stdin=subprocess.DEVNULL,
                                   stderr=subprocess.PIPE, timeout=timeout)


def regular(path, limit=128 << 20, root=True):
    path = Path(path)
    require(path.is_absolute() and path.resolve() == path, 'noncanonical file path')
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size <= limit and
                (not root or info.st_uid == 0 and not info.st_mode & 0o022), 'unsafe file')
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    require(len(data) == info.st_size and (info.st_size, info.st_mtime_ns, info.st_ctime_ns) ==
            (after.st_size, after.st_mtime_ns, after.st_ctime_ns), 'file changed during read')
    return data


def saved(path):
    data, info = regular(path), os.stat(path)
    return dict(path=str(path), data_b64=base64.b64encode(data).decode(), sha256=sha(data),
                uid=info.st_uid, gid=info.st_gid, mode=stat.S_IMODE(info.st_mode))


def content(item):
    data = base64.b64decode(item['data_b64'], validate=True)
    require(sha(data) == item['sha256'], 'saved file checksum differs')
    return data


def parent_info(info):
    return dict(uid=info.st_uid, gid=info.st_gid, mode=stat.S_IMODE(info.st_mode), dev=info.st_dev, inode=info.st_ino)


def open_parent(path, expected=None):
    path = Path(path)
    require(path.parent.resolve() == path.parent, 'symlinked write parent')
    fd = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = parent_info(os.fstat(fd))
        service_marker = str(path) == MARKER and (info['uid'], info['gid'], info['mode']) == (1003, 1003, 0o775)
        require(service_marker or info['uid'] == 0 and not info['mode'] & 0o022, 'unsafe write parent')
        require(expected is None or info == expected, 'write parent identity changed')
        return fd, info
    except BaseException:
        os.close(fd); raise


def write_atomic(item, parent_pin=None):
    path = Path(item['path'])
    require(str(path) != MARKER or parent_pin is not None, 'marker requires admitted parent identity')
    directory, info = open_parent(path, parent_pin)
    name = '.' + path.name + '.' + uuid.uuid4().hex
    try:
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        with os.fdopen(fd, 'wb') as stream:
            os.fchown(stream.fileno(), item['uid'], item['gid'])
            os.fchmod(stream.fileno(), item['mode'])
            stream.write(content(item)); stream.flush(); os.fsync(stream.fileno())
        os.replace(name, path.name, src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
        require(parent_info(os.stat(str(path.parent), follow_symlinks=False)) == info, 'write parent moved')
    finally:
        try:
            os.unlink(name, dir_fd=directory)
        except FileNotFoundError:
            pass
        finally:
            os.close(directory)


def changed(item, data):
    result = dict(item)
    result.update(data_b64=base64.b64encode(data).decode(), sha256=sha(data))
    return result


def save(name, value):
    path = TRANSACTION / name
    write_atomic(dict(path=str(path), uid=0, gid=0, mode=0o600,
                      data_b64=base64.b64encode((json.dumps(value, sort_keys=True) + '\n').encode()).decode(),
                      sha256=sha((json.dumps(value, sort_keys=True) + '\n').encode())))


def failure_details(error):
    return {'error_type': type(error).__name__, 'error': str(error)[:8192],
            'traceback': traceback.format_exc()[-65536:], 'at': time.time()}


def private_failure(error):
    # admit may fail before its transaction directory exists. This pre-existing
    # private evidence directory is independent of candidate/recovery state.
    directory = Path(SPACE_EVIDENCE).parent
    require(directory.resolve() == directory and directory.stat().st_uid == 0 and
            stat.S_IMODE(directory.stat().st_mode) == 0o700, 'unsafe private failure directory')
    fd, name = tempfile.mkstemp(prefix='activation-failure-', suffix='.json', dir=str(directory))
    with os.fdopen(fd, 'wb') as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(json.dumps(failure_details(error), sort_keys=True).encode())
        stream.flush(); os.fsync(stream.fileno())


def create_transaction():
    TRANSACTION.mkdir(mode=0o700)
    fd = os.open(str(TRANSACTION.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)  # Persist the recovery-directory name before admission.
    finally:
        os.close(fd)


def module(blob, checksum, name):
    require(sha(blob) == checksum, 'reviewed helper checksum differs')
    result = types.ModuleType(name); result.__file__ = name
    exec(compile(blob, name, 'exec'), result.__dict__)
    return result


def guard():
    return module(regular(GUARD), GUARD_SHA, 'reader_guard')


def show():
    props = {}
    for row in run(['/bin/systemctl', 'show', 'gtron.service', '--no-pager']).decode().splitlines():
        key, sep, value = row.partition('=')
        if sep:
            props[key] = props[key] + ' ; ' + value if key in props and key.startswith('Exec') else value
    return props


def configuration(g, props):
    values = {key: props.get(key, '') for key in CONFIG_KEYS}
    for key in ('ExecStart', 'ExecStartPre', 'ExecStartPost'):
        values[key] = g.parse_commands(values[key])
    return values


def process(pid):
    root = Path('/proc') / str(pid)
    rawstat = (root / 'stat').read_bytes()
    return dict(pid=pid, ticks=int(rawstat.rsplit(b') ', 1)[1].split()[19]),
                exe=os.readlink(str(root / 'exe')),
                argv=(root / 'cmdline').read_bytes().rstrip(b'\0').decode().split('\0'),
                environ=base64.b64encode((root / 'environ').read_bytes()).decode())


def environment(encoded):
    # systemd may inject per-invocation diagnostics; configured environment is exact.
    return sorted(row for row in base64.b64decode(encoded).split(b'\0') if row and
                  row.split(b'=', 1)[0] not in (b'INVOCATION_ID', b'JOURNAL_STREAM', b'SYSTEMD_EXEC_PID'))


def observe():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def get(url):
        with opener.open(url, timeout=10) as reply:
            return json.loads(reply.read(8 << 20))
    block = get('http://127.0.0.1:8090/wallet/getnowblock')
    metrics = get('http://127.0.0.1:6062/debug/metrics')['metrics']
    return int(block['block_header']['raw_data']['number']), metrics


def validate_candidate(args):
    for value, length in ((args.source_revision, 40), (args.script_revision, 40),
                          (args.binary_sha, 64), (args.prepared_sha, 64), (args.abba_sha, 64)):
        require(re.fullmatch('[0-9a-f]{%d}' % length, value or ''), 'exact candidate identity required')
    require(run(['git', 'rev-parse', args.source_revision + '^{commit}']).decode().strip() == args.source_revision,
            'source Git identity differs')
    require(run(['git', 'show', args.script_revision + ':' + SCRIPT]) == regular(Path(__file__).resolve()), 'activation script Git bytes differ')
    data = regular(RELEASE / 'prepared.json'); prepared = json.loads(data)
    require(sha(data) == args.prepared_sha and prepared.get('prepared') is True and
            prepared.get('source_commit') == args.source_revision and prepared.get('binary') == str(BINARY) and
            prepared.get('binary_sha256') == args.binary_sha and sha(regular(BINARY, 512 << 20)) == args.binary_sha,
            'native candidate attestation differs')
    require(prepared['go_version'] == 'go version go1.25.5 linux/amd64' and
            all(item['returncode'] == 0 and sha(regular(RELEASE / item['log'])) == item['log_sha256']
                for item in prepared['native_commands']) and len(prepared['native_commands']) >= 8,
            'native command evidence differs')
    expected = {}
    for row in run(['git', 'ls-tree', '-rz', args.source_revision]).split(b'\0'):
        if row:
            meta, name = row.split(b'\t'); mode, kind, oid = meta.decode().split()
            if kind == 'blob':
                expected[name.decode()] = (oid, int(mode, 8) & 0o777)
    require(set(expected) == set(prepared['manifest']), 'full source manifest differs')
    for name, entry in prepared['manifest'].items():
        data = regular(RELEASE / 'source' / name, 64 << 20)
        require((entry['git_blob'], entry['mode']) == expected[name] and
                hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() == entry['git_blob'] and
                sha(data) == entry['sha256'] and stat.S_IMODE((RELEASE / 'source' / name).stat().st_mode) == entry['mode'],
                'prepared source differs from Git')
    data = regular(args.abba_summary, root=False); summary = json.loads(data)
    require(sha(data) == args.abba_sha and summary.get('complete') is True and
            summary.get('input_unchanged') is True and summary.get('binary_unchanged') is True and
            summary['binary_sha256'] == args.binary_sha and summary['script_sha256'] == ABBA_SHA,
            'complete fixed-binary ABBA evidence required')
    abba = module(run(['git', 'show', args.script_revision + ':' + ABBA_PATH]), ABBA_SHA, 'cache_abba')
    h = abba.load_helpers()
    require(abba.summarize(h, summary['runs']) == summary['results'], 'ABBA summary recomputation differs')
    manifest = regular(Path(summary['input_dir']) / 'manifest.json', root=False)
    require(sha(manifest) == summary['input_manifest_sha256'], 'ABBA input manifest differs')
    source = json.loads(manifest); h.check_export(source)
    check_args = argparse.Namespace(input_dir=Path(summary['input_dir']), manifest_sha=summary['input_manifest_sha256'])
    for index, item in enumerate(summary['runs']):
        require(item['returncode'] == 0 and item['command'][0] == str(BINARY), 'ABBA binary/returncode differs')
        label = '{:02d}-cache-{}{}'.format(index, 'on' if item['cache_enabled'] else 'off', '-profile' if item['profile'] else '')
        require(item['label'] == label, 'ABBA label differs')
        output = Path(summary['output_dir']) / label
        data = regular(output / 'report.json', root=False)
        require(sha(data) == item['report_sha256'], 'ABBA report hash differs')
        rows = abba.check_report(h, json.loads(data), check_args, output, item['cache_enabled'], item['profile'], source)
        require(rows == item['iterations'], 'ABBA full report differs')
    return prepared


def make_plan(files, g, checksum, source):
    bypath = {item['path']: item for item in files}
    require(json.loads(content(bypath[MARKER])) == g.reader_marker(OLD_BINARY, OLD_SHA, OLD_SOURCE), 'old reader marker differs')
    original = content(bypath[EXEC]); require(original.count(OLD_BINARY.encode()) == 1 and
        not any(flag.split('=')[0].encode() in original for flag in FLAGS), 'ambiguous original ExecStart')
    require(b'--history.backlog' not in original, 'unexpected admission throttle')
    newexec = original.replace(OLD_BINARY.encode(), (str(BINARY) + ' ' + ' '.join(FLAGS)).encode(), 1)
    before = content(bypath[DROPIN]); after = before
    for old, new in ((OLD_BINARY, str(BINARY)), (OLD_SHA, checksum), (OLD_SOURCE, source)):
        require(after.count(old.encode()) == 1, 'ambiguous guard pin')
        after = after.replace(old.encode(), new.encode(), 1)
    newmarker = (json.dumps(g.reader_marker(BINARY, checksum, source), sort_keys=True) + '\n').encode()
    return [changed(bypath[EXEC], newexec), changed(bypath[DROPIN], after), changed(bypath[MARKER], newmarker)]


def admit(args):
    validate_candidate(args)
    require(not os.path.lexists(SPACE_HOLD), 'space protection hold is armed')
    data = regular(args.live_evidence); evidence = json.loads(data)
    require(sha(data) == args.live_evidence_sha and len(evidence['ancestor_markers']) == 9, 'live evidence pin differs')
    space_data = regular(SPACE_EVIDENCE); space = json.loads(space_data)
    require(sha(space_data) == args.space_evidence_sha and {f['path'] for f in space} == {SPACE_GUARD, SPACE_CONFIG},
            'space dependency evidence differs')
    for item in evidence['files'] + evidence['ancestor_markers'] + space:
        require(saved(item['path']) == item, 'old pinned file changed')
    g, props = guard(), show(); proc = process(int(props['MainPID']))
    recorded_props = {}
    for row in base64.b64decode(evidence['systemctl_show_b64']).decode().splitlines():
        key, sep, value = row.partition('=')
        if sep:
            recorded_props[key] = recorded_props[key] + ' ; ' + value if key in recorded_props and key.startswith('Exec') else value
    require(configuration(g, props) == configuration(g, recorded_props) and
            run(['/bin/systemctl', 'cat', 'gtron.service']) == base64.b64decode(evidence['systemctl_cat_b64']),
            'captured effective service configuration changed')
    require((proc['pid'], proc['ticks'], proc['exe']) == (OLD_PID, OLD_TICKS, OLD_BINARY) and
            sha(regular(OLD_BINARY, 512 << 20)) == OLD_SHA, 'old process identity differs')
    require(base64.b64decode(evidence['proc']['cmdline']).rstrip(b'\0').decode().split('\0') == proc['argv'] and
            evidence['proc']['environ'] == proc['environ'], 'old process argv/environment differs')
    require(not props.get('EnvironmentFiles'), 'unreviewed environment-file dependency')
    paths = [props['FragmentPath']] + shlex.split(props['DropInPaths'])
    files = [saved(path) for path in dict.fromkeys(paths + [SPACE_GUARD, SPACE_CONFIG] +
             [item['path'] for item in evidence['files']])]
    require(EXEC in paths and DROPIN in paths, 'expected effective startup drop-ins missing')
    plan = make_plan(files, g, args.binary_sha, args.source_revision)
    parents = {}
    for path in [item['path'] for item in plan] + [RELEASE / 'reader-required.json']:
        fd, parents[str(path)] = open_parent(path)
        os.close(fd)
    head, metrics = observe(); require(props['ActiveState'] == 'active' and head > 0, 'old reader not healthy')
    g.check_command(props['ExecStart'], OLD_BINARY, OLD_SHA, OLD_SOURCE, json.loads(content(next(f for f in files if f['path'] == MARKER))))
    require(process(OLD_PID) == proc, 'old process changed while admitting')
    record = dict(version=1, args=vars(args), process=proc, files=files, plan=plan,
                  ancestors=evidence['ancestor_markers'], parents=parents, configuration=configuration(g, props),
                  control_group=props['ControlGroup'],
                  systemctl_cat_b64=base64.b64encode(run(['/bin/systemctl', 'cat', 'gtron.service'])).decode(),
                  head=head, process_start=metrics['process/start/unix_nano']['value'], created_at=time.time())
    healthy(record, False)
    require(process(OLD_PID) == proc, 'old process changed during admission health')
    require(not TRANSACTION.exists(), 'transaction already exists; use saved recovery')
    create_transaction()
    save('admission.json', record)
    return {'admitted': True, 'admission_sha256': sha(regular(TRANSACTION / 'admission.json')), 'pid': proc['pid']}


def check_files(record, target=None):
    for path, pin in record['parents'].items():
        fd, unused = open_parent(path, pin); os.close(fd)
    changes = {item['path']: item for item in record['plan']}
    for item in record['files'] + record['ancestors']:
        actual = saved(item['path'])
        expected = changes.get(item['path'], item) if target == 'new' else item
        require(actual == expected if target else actual in (item, changes.get(item['path'], item)), 'unapproved file drift')
    armed = dict(record['plan'][-1], path=str(RELEASE / 'reader-required.json'))
    if os.path.lexists(armed['path']) or target == 'new':
        require(saved(armed['path']) == armed, 'candidate permanent marker changed')


def expected_config(record, new):
    config = copy.deepcopy(record['configuration'])
    if new:
        command = config['ExecStart'][0]
        command['path'] = str(BINARY)
        command['argv[]'] = command['argv[]'].replace(OLD_BINARY, str(BINARY) + ' ' + ' '.join(FLAGS), 1)
        for command in config['ExecStartPre']:
            if GUARD in command['argv[]']:
                command['argv[]'] = command['argv[]'].replace(OLD_BINARY, str(BINARY)).replace(OLD_SHA, record['args']['binary_sha']).replace(OLD_SOURCE, record['args']['source_revision'])
    return config


def healthy(record, new, timeout=240):
    g = guard(); expected = expected_config(record, new)
    argv = [str(BINARY)] + FLAGS + record['process']['argv'][1:] if new else record['process']['argv']
    checksum = record['args']['binary_sha'] if new else OLD_SHA
    require(sha(regular(argv[0], 512 << 20)) == checksum, 'running binary checksum differs')
    deadline, first, identity = time.monotonic() + timeout, None, None
    while time.monotonic() < deadline:
        props = show()
        if props.get('ActiveState') == 'active' and int(props.get('MainPID', 0)):
            proc = process(int(props['MainPID']))
            require(proc['exe'] == argv[0] and proc['argv'] == argv and environment(proc['environ']) == environment(record['process']['environ']), 'running process configuration differs')
            require(configuration(g, props) == expected, 'effective unit changed')
            require(props.get('ControlGroup') == record['control_group'], 'running control group differs')
            try:
                head, metrics = observe()
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                time.sleep(3); continue
            if 'process/start/unix_nano' not in metrics:
                time.sleep(3); continue
            current = (proc['pid'], proc['ticks'], metrics['process/start/unix_nano']['value'])
            require(head >= record['head'] and (not new or current[2] > record['process_start']), 'head or process metric regressed')
            if identity is None:
                identity, first = current, head
            require(current == identity, 'process restarted during health check')
            if new:
                prefix = 'state/snapshot/cold/history/shared_read/'
                if prefix + 'attempts' not in metrics:
                    time.sleep(3); continue
                for name, field in [('attempts', 'count'), ('published', 'count')] + [('last/' + k, 'value') for k in ('active', 'workers', 'chunk_cache', 'codec_workers', 'fallback_reason')]:
                    value = metrics.get(prefix + name, {}).get(field)
                    require(type(value) is int and value >= 0, 'shared-read metric contract missing')
            if head > first:
                check_files(record, 'new' if new else 'old')
                final = show()
                require(final.get('ActiveState') == 'active' and int(final.get('MainPID', 0)) == proc['pid'] and
                        process(proc['pid']) == proc and configuration(g, final) == expected and
                        final.get('ControlGroup') == record['control_group'], 'process changed during final health observation')
                return dict(pid=proc['pid'], ticks=proc['ticks'], head=head, process_start=current[2])
        time.sleep(3)
    raise RuntimeError('bounded head-forward health check expired')


def stop(record):
    props = show(); pid = int(props.get('MainPID', 0))
    if pid:
        proc = process(pid)
        require(proc['exe'] in (OLD_BINARY, str(BINARY)), 'refusing to stop unrelated process')
        expected = record['process']['argv'] if proc['exe'] == OLD_BINARY else [str(BINARY)] + FLAGS + record['process']['argv'][1:]
        require(proc['argv'] == expected and environment(proc['environ']) == environment(record['process']['environ']) and
                props.get('ControlGroup') == record['control_group'], 'refusing to stop altered process configuration')
        checksum = OLD_SHA if proc['exe'] == OLD_BINARY else record['args']['binary_sha']
        require(sha(regular(proc['exe'], 512 << 20)) == checksum, 'refusing to stop unpinned binary')
    run(['/bin/systemctl', 'stop', 'gtron.service'], timeout=660)
    props = show()
    require(int(props.get('MainPID', 0)) == 0 and props['ActiveState'] in ('inactive', 'failed'), 'service not fully stopped; no forced kill')


def restore(record):
    check_files(record); stop(record)
    for item in record['files']:
        if item['path'] in (EXEC, DROPIN, MARKER) and saved(item['path']) != item:
            write_atomic(item, parent_pin=record['parents'][item['path']])
    run(['/bin/systemctl', 'daemon-reload'])
    require(configuration(guard(), show()) == expected_config(record, False), 'restored effective unit differs')
    run(['/bin/systemctl', 'start', 'gtron.service'], timeout=180)
    return healthy(record, False)


def transact(record, activate):
    if not activate:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            result = restore(record); save('rollback.json', result); return result
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
    validate_candidate(argparse.Namespace(**record['args']))
    require(not os.path.lexists(SPACE_HOLD), 'space protection hold is armed')
    check_files(record, 'old')
    require(process(OLD_PID) == record['process'] and configuration(guard(), show()) == record['configuration'], 'admitted live process/config changed')
    try:
        save('journal.json', {'phase': 'stopping', 'at': time.time()}); stop(record)
        check_files(record, 'old')
        for item in record['plan']:
            write_atomic(item, parent_pin=record['parents'][item['path']])
        # This permanent compatible-reader evidence is retained even after rollback.
        armed = dict(record['plan'][-1], path=str(RELEASE / 'reader-required.json'))
        if os.path.lexists(armed['path']):
            require(saved(armed['path']) == armed, 'candidate permanent marker differs')
        else:
            write_atomic(armed, parent_pin=record['parents'][armed['path']])
        run(['/bin/systemctl', 'daemon-reload'])
        require(configuration(guard(), show()) == expected_config(record, True), 'candidate effective unit differs')
        run(['/bin/systemctl', 'start', 'gtron.service'], timeout=180)
        result = healthy(record, True); save('activated.json', result); return result
    except BaseException as error:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            try:
                save('failure.json', failure_details(error))
            finally:
                try:
                    result = restore(record); save('rollback.json', result)
                except BaseException as recovery_error:
                    try:
                        save('rollback-failed.json', failure_details(recovery_error))
                    except BaseException:
                        pass
                    raise
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('admit', 'activate', 'rollback'))
    for name in ('source-revision', 'script-revision', 'binary-sha', 'prepared-sha', 'abba-summary', 'abba-sha',
                 'live-evidence', 'live-evidence-sha', 'space-evidence-sha', 'admission-sha'):
        parser.add_argument('--' + name)
    args = parser.parse_args()
    require(os.geteuid() == 0 and sys.platform == 'linux', 'native root Linux execution required')
    os.umask(0o077)
    for sig in SIGNALS:
        signal.signal(sig, lambda number, frame: (_ for _ in ()).throw(OperatorInterrupted('operator signal')))
    lock = os.open('/run/lock/gtron-history-shared-read-activation.lock', os.O_CREAT | os.O_NOFOLLOW | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.mode == 'admit':
            result = admit(args)
        else:
            data = regular(TRANSACTION / 'admission.json')
            require(sha(data) == args.admission_sha, 'exact saved admission checksum required')
            result = transact(json.loads(data), args.mode == 'activate')
        print(json.dumps(result, sort_keys=True), flush=True)
    finally:
        os.close(lock)


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        try:
            private_failure(error)
        except BaseException:
            pass
        print(json.dumps({'complete': False, 'error_type': type(error).__name__}), file=sys.stderr)
        sys.exit(1)
