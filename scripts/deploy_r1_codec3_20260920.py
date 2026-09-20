#!/usr/bin/env python3
"""Deploy the fixed 2026-09-20 R1/codec3 binary without touching chain data."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import sys
import time

SERVICE = 'gtron.service'
OLD_RELEASE = Path('/data/gtron/releases/20260918-fresh-mainnet')
OLD_BINARY = OLD_RELEASE / 'gtron'
OLD_SHA = '378f41bfe69d0fb8d89ade226f6b70c620e8efd42f33a2d628b20a80debb6e18'
OLD_SOURCE = '16ea25075c380dd610d0200e9e6048fa46a2d84c'
NEW_RELEASE = Path('/data/gtron/releases/20260920-r1-codec3')
NEW_BINARY = NEW_RELEASE / 'gtron'
NEW_SHA = 'b1e263e71d16097e9a7bc19f9fa2378f603697c16c7b2a21acbb314e52593321'
NEW_SOURCE = '1e6f5c2565cedb581d1c3401da07c8d37520117e'
MEMORY_DROPIN = Path('/etc/systemd/system/gtron.service.d/zz-memory-budget-20260918.conf')
SHARED_DROPIN = Path('/etc/systemd/system/gtron.service.d/zz-history-shared-reader.conf')
GLOBAL_MARKER = Path('/data/gtron/main/HISTORY_SHARED_CHUNKS_REQUIRES_READER_V3.json')
OLD_R1_MARKER = OLD_RELEASE / 'reference-reader-required.json'
NEW_SHARED_MARKER = NEW_RELEASE / 'reader-required.json'
NEW_R1_MARKER = NEW_RELEASE / 'reference-reader-required.json'
SHARED_GUARD = '/usr/local/libexec/gtron-history-shared-reader-guard.py'
R1_GUARD = '/usr/local/libexec/gtron-history-reference-reader-guard.py'
LOCK = '/run/lock/gtron-r1-codec3-deploy.lock'
STATE_PATH = None


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def run(argv, timeout=60, check=True):
    proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try: out, err = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill(); out, err = proc.communicate()
        raise RuntimeError('%s timed out after %ds' % (' '.join(argv), timeout))
    if check and proc.returncode != 0:
        raise RuntimeError('%s failed rc=%d: %s' % (' '.join(argv), proc.returncode, err.decode('utf-8', 'replace')[-2000:]))
    return out


def regular(path, limit):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_uid == 0 and not before.st_mode & 0o022,
                'unsafe root-owned file: ' + str(path))
        require(before.st_size <= limit, 'file too large: ' + str(path))
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    require(len(data) <= limit and before.st_size == len(data), 'short/oversized read: ' + str(path))
    require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
            'file changed while reading: ' + str(path))
    return data


def sha(path, limit=512 << 20):
    return hashlib.sha256(regular(path, limit)).hexdigest()


def atomic_root(path, data, mode):
    path = Path(path)
    tmp = path.with_name('.' + path.name + '.deploy-%d' % os.getpid())
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())
        os.chown(str(tmp), 0, 0); os.chmod(str(tmp), mode); os.replace(str(tmp), str(path))
        dfd = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(dfd)
        finally: os.close(dfd)
    finally:
        if os.path.lexists(str(tmp)): os.unlink(str(tmp))


def marker_shared():
    return {'version': 1, 'required_reader': 3, 'bucket_blocks': 1024,
            'binary': str(NEW_BINARY), 'binary_sha256': NEW_SHA, 'source_commit': NEW_SOURCE}


def marker_r1():
    return {'version': 1, 'container_format': 'GTHREF01',
            'binary': str(NEW_BINARY), 'binary_sha256': NEW_SHA, 'source_commit': NEW_SOURCE}


def json_bytes(value):
    return (json.dumps(value, sort_keys=True) + '\n').encode('utf-8')


def record_stage(stage, detail=None):
    if STATE_PATH is None: return
    value = {'stage': stage, 'updated_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
             'binary': str(NEW_BINARY), 'binary_sha256': NEW_SHA, 'source_commit': NEW_SOURCE}
    if detail is not None: value['detail'] = detail
    atomic_root(STATE_PATH, json_bytes(value), 0o600)


def proc_bytes(pid, name, limit):
    path = '/proc/%d/%s' % (pid, name)
    with open(path, 'rb') as stream:
        data = stream.read(limit + 1)
    require(len(data) <= limit, 'oversized process field: ' + name)
    return data


def proc_exe_sha(pid):
    fd = os.open('/proc/%d/exe' % pid, os.O_RDONLY)
    digest, total = hashlib.sha256(), 0
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode), 'process executable is not regular')
        while True:
            data = stream.read(1 << 20)
            if not data: break
            total += len(data); require(total <= 512 << 20, 'process executable too large'); digest.update(data)
        require(total == info.st_size, 'short process executable read')
    return digest.hexdigest()


def proc_identity(pid):
    exe = os.readlink('/proc/%d/exe' % pid)
    argv = [part.decode('utf-8') for part in proc_bytes(pid, 'cmdline', 1 << 20).split(b'\0') if part]
    return exe, argv


def flag_values(argv):
    values, index = {}, 1
    while index < len(argv):
        token = argv[index]
        if not token.startswith('--'):
            index += 1; continue
        name = token[2:]
        if '=' in name:
            name, value = name.split('=', 1); index += 1
        elif index + 1 < len(argv) and not argv[index + 1].startswith('--'):
            value = argv[index + 1]; index += 2
        else:
            value = 'true'; index += 1
        values.setdefault(name, []).append(value)
    return values


def validate_service_argv(argv):
    rows = flag_values(argv)
    expected = {'datadir': '/data/gtron/main/datadir', 'prune.mode': 'snap', 'db.cache': '4096',
                'history.shared-read-workers': '4', 'history.shared-chunk-cache': 'true',
                'history.reference-container': 'true'}
    for name, value in expected.items(): require(rows.get(name) == [value], 'service flag differs: ' + name)
    require(rows.get('history.cross-block-dedup') in (['true'], ['false']), 'shared writer flag differs')
    defaults = {'p2p.port': '18888', 'discover.port': '0', 'http.port': '8090', 'jsonrpc.port': '8545',
                'grpc.port': '50051', 'pprof.port': '0', 'pprof.addr': '127.0.0.1'}
    required = {'p2p.port': '18890', 'http.port': '8090', 'jsonrpc.port': '8545', 'grpc.port': '50051',
                'pprof.port': '6062', 'pprof.addr': '127.0.0.1'}
    for name, value in required.items(): require(rows.get(name, [defaults[name]]) == [value], 'network flag differs: ' + name)
    require(rows.get('discover.port', ['0']) in (['0'], ['18890']), 'discovery port differs')
    require(not set(rows) & {'config', 'genesis', 'testnet', 'dev', 'witness', 'snapshot.reset',
                             'sync.restart-from', 'sync.replay-stored-to', 'sync.stop-at'}, 'unsafe service mode present')


def show(*properties):
    argv = ['/bin/systemctl', 'show', SERVICE, '--no-pager']
    for name in properties: argv.append('--property=' + name)
    result = {}
    for line in run(argv, 30).decode('utf-8').splitlines():
        key, sep, value = line.partition('=')
        if sep: result.setdefault(key, []).append(value)
    return result


def wait_state(want, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        props = show('ActiveState', 'SubState', 'MainPID')
        active = props.get('ActiveState', [''])[0]
        pid = int(props.get('MainPID', ['0'])[0] or '0')
        if active == want and (want != 'inactive' or pid == 0): return props
        time.sleep(1)
    raise RuntimeError('service did not reach ' + want)


def replace_exact(data, old, new, count, label):
    require(data.count(old) == count, 'ambiguous ' + label)
    return data.replace(old, new)


def main():
    global STATE_PATH
    require(os.geteuid() == 0, 'root required')
    lockfd = os.open(LOCK, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    release = os.lstat(str(NEW_RELEASE)); binary = os.lstat(str(NEW_BINARY))
    require(stat.S_ISDIR(release.st_mode) and release.st_uid == 0 and not stat.S_ISLNK(release.st_mode), 'unsafe new release')
    STATE_PATH = NEW_RELEASE / 'deploy-state.json'
    record_stage('preflight')
    require(stat.S_ISREG(binary.st_mode) and binary.st_uid == 0 and stat.S_IMODE(binary.st_mode) == 0o755 and not stat.S_ISLNK(binary.st_mode), 'unsafe new binary')
    require(sha(NEW_BINARY) == NEW_SHA, 'new binary checksum differs')
    buildinfo = run(['/data/go/bin/go', 'version', '-m', str(NEW_BINARY)], 30).decode('utf-8')
    for token in ('go1.25.5', 'GOOS=linux', 'GOARCH=amd64', 'CGO_ENABLED=1', 'GOAMD64=v1', '-tags=sapling'):
        require(token in buildinfo, 'new binary build info differs: ' + token)
    require(run(['/usr/bin/git', '--git-dir', str(NEW_RELEASE / 'repo.git'), 'rev-parse', NEW_SOURCE + '^{commit}'], 30).decode('utf-8').strip() == NEW_SOURCE,
            'candidate archive commit evidence differs')

    props = show('ActiveState', 'MainPID', 'ExecStart', 'ExecStartPre')
    require(props.get('ActiveState', [''])[0] == 'active', 'service is not active')
    pid = int(props.get('MainPID', ['0'])[0]); require(pid > 1, 'invalid MainPID')
    old_exe, old_argv = proc_identity(pid)
    require(old_exe == str(OLD_BINARY) and old_argv and old_argv[0] == str(OLD_BINARY), 'old process identity differs')
    validate_service_argv(old_argv)
    require(sha(OLD_BINARY) == OLD_SHA, 'old binary checksum differs')

    memory = regular(MEMORY_DROPIN, 1 << 20)
    shared = regular(SHARED_DROPIN, 1 << 20)
    global_old = json.loads(regular(GLOBAL_MARKER, 4096).decode('utf-8'))
    r1_old = json.loads(regular(OLD_R1_MARKER, 4096).decode('utf-8'))
    for marker in (global_old,):
        require(marker == {'version': 1, 'required_reader': 3, 'bucket_blocks': 1024,
                           'binary': str(OLD_BINARY), 'binary_sha256': OLD_SHA, 'source_commit': OLD_SOURCE},
                'old shared marker differs')
    require(r1_old == {'version': 1, 'container_format': 'GTHREF01', 'binary': str(OLD_BINARY),
                       'binary_sha256': OLD_SHA, 'source_commit': OLD_SOURCE}, 'old R1 marker differs')
    require(memory.count(str(OLD_BINARY).encode()) == 1 and b'ExecStart=' in memory, 'memory drop-in contract differs')
    require(shared.count(str(OLD_BINARY).encode()) == 2 and shared.count(OLD_SHA.encode()) == 2 and
            shared.count(OLD_SOURCE.encode()) == 2 and shared.count(str(OLD_R1_MARKER).encode()) == 1 and
            shared.count(SHARED_GUARD.encode()) == 1 and shared.count(R1_GUARD.encode()) == 1,
            'reader drop-in contract differs')
    require(str(OLD_BINARY) in props.get('ExecStart', [''])[0], 'effective old ExecStart differs')
    old_pre_raw = '\n'.join(props.get('ExecStartPre', []))
    require(old_pre_raw.count('/usr/local/libexec/gtron-mainnet-space-guard.py') <= 1,
            'duplicate effective space guard')
    record_stage('preflight-ok', {'old_pid': pid})

    new_memory = replace_exact(memory, str(OLD_BINARY).encode(), str(NEW_BINARY).encode(), 1, 'ExecStart binary')
    new_shared = replace_exact(shared, str(OLD_BINARY).encode(), str(NEW_BINARY).encode(), 2, 'guard binary')
    new_shared = replace_exact(new_shared, OLD_SHA.encode(), NEW_SHA.encode(), 2, 'guard checksum')
    new_shared = replace_exact(new_shared, OLD_SOURCE.encode(), NEW_SOURCE.encode(), 2, 'guard source')
    new_shared = replace_exact(new_shared, str(OLD_R1_MARKER).encode(), str(NEW_R1_MARKER).encode(), 1, 'R1 marker')

    stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
    evidence = NEW_RELEASE / ('deploy-' + stamp)
    os.mkdir(str(evidence), 0o700); os.chown(str(evidence), 0, 0)
    for name, data in (('memory.before', memory), ('shared.before', shared), ('global.before.json', json_bytes(global_old)),
                       ('r1.before.json', json_bytes(r1_old)), ('buildinfo.txt', buildinfo.encode('utf-8')),
                       ('argv.before.json', json_bytes({'pid': pid, 'exe': old_exe, 'argv': old_argv}))):
        atomic_root(evidence / name, data, 0o600)

    record_stage('stopping', {'old_pid': pid})
    run(['/bin/systemctl', 'stop', SERVICE], 120); wait_state('inactive', 120)
    record_stage('stopped', {'old_pid': pid})
    atomic_root(MEMORY_DROPIN, new_memory, 0o644)
    atomic_root(SHARED_DROPIN, new_shared, 0o644)
    atomic_root(GLOBAL_MARKER, json_bytes(marker_shared()), 0o644)
    atomic_root(NEW_SHARED_MARKER, json_bytes(marker_shared()), 0o644)
    atomic_root(NEW_R1_MARKER, json_bytes(marker_r1()), 0o644)
    record_stage('files-installed')
    run(['/bin/systemctl', 'daemon-reload'], 30)

    effective = show('ExecStart', 'ExecStartPre', 'ActiveState', 'MainPID')
    start_raw = '\n'.join(effective.get('ExecStart', []))
    pre_raw = '\n'.join(effective.get('ExecStartPre', []))
    require(str(NEW_BINARY) in start_raw and str(OLD_BINARY) not in start_raw, 'effective ExecStart was not replaced')
    require(pre_raw.count(str(NEW_BINARY)) == 2 and pre_raw.count(NEW_SHA) == 2 and pre_raw.count(NEW_SOURCE) == 2 and
            str(NEW_R1_MARKER) in pre_raw and str(OLD_BINARY) not in pre_raw, 'effective reader guards differ')
    require(pre_raw.count('/usr/local/libexec/gtron-mainnet-space-guard.py') ==
            old_pre_raw.count('/usr/local/libexec/gtron-mainnet-space-guard.py'), 'space guard changed')
    record_stage('reload-verified')
    guards_run = 0
    for line in new_shared.decode('utf-8').splitlines():
        if line.startswith('ExecStartPre='):
            command = line[len('ExecStartPre='):].strip()
            if not command: continue
            argv = shlex.split(command)
            require(len(argv) > 1 and argv[1] in (SHARED_GUARD, R1_GUARD), 'unexpected reader guard')
            run(argv, 60); guards_run += 1
    require(guards_run == 2, 'expected two reader guards')
    record_stage('guards-passed')

    journal_since = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
    record_stage('starting')
    run(['/bin/systemctl', 'start', SERVICE], 120); active = wait_state('active', 120)
    new_pid = int(active.get('MainPID', ['0'])[0]); require(new_pid > 1 and new_pid != pid, 'new MainPID invalid')
    new_exe, new_argv = proc_identity(new_pid)
    require(new_exe == str(NEW_BINARY) and new_argv and new_argv[0] == str(NEW_BINARY), 'new process identity differs')
    require(new_argv == [str(NEW_BINARY)] + old_argv[1:], 'service arguments changed')
    require(proc_exe_sha(new_pid) == NEW_SHA, 'running binary checksum differs')
    record_stage('started', {'new_pid': new_pid})
    status = run(['/bin/systemctl', 'status', SERVICE, '--no-pager', '-l'], 30, False)
    journal = run(['/bin/journalctl', '-u', SERVICE, '--since', journal_since, '--no-pager', '-n', '200'], 30, False)
    final = {'deployed': True, 'sampled_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
             'old_pid': pid, 'new_pid': new_pid, 'binary': new_exe, 'binary_sha256': NEW_SHA,
             'source_commit': NEW_SOURCE, 'argv': new_argv,
             'files': {str(p): sha(p, 1 << 20) for p in (MEMORY_DROPIN, SHARED_DROPIN, GLOBAL_MARKER, NEW_SHARED_MARKER, NEW_R1_MARKER)}}
    atomic_root(evidence / 'status.txt', status, 0o600)
    atomic_root(evidence / 'journal.txt', journal, 0o600)
    atomic_root(evidence / 'result.json', json_bytes(final), 0o600)
    atomic_root(NEW_RELEASE / 'DEPLOYED.json', json_bytes(final), 0o644)
    record_stage('complete', {'new_pid': new_pid, 'evidence': str(evidence)})
    print(json.dumps(final, sort_keys=True))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        try: record_stage('error', str(error))
        except Exception: pass
        print('deploy_r1_codec3_20260920: ' + str(error), file=sys.stderr)
        sys.exit(1)
