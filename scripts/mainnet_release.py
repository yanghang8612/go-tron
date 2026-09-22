#!/usr/bin/env python3
"""Root-owned mainnet release helper; install a reviewed copy in /usr/local/libexec.

The user-owned build script may request a release, but this program verifies the
existing guarded service before changing its pinned binary and reader markers.
It never edits chain data, the space guard, service flags, or Nile.
"""

import argparse
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
import urllib.request

SERVICE = 'gtron.service'
REPO = Path('/data/gtron/go-tron')
RELEASES = Path('/data/gtron/releases')
MEMORY = Path('/etc/systemd/system/gtron.service.d/zz-memory-budget-20260918.conf')
SHARED = Path('/etc/systemd/system/gtron.service.d/zz-history-shared-reader.conf')
MARKER = Path('/data/gtron/main/HISTORY_SHARED_CHUNKS_REQUIRES_READER_V3.json')
SPACE_GUARD = '/usr/local/libexec/gtron-mainnet-space-guard.py'
SHARED_GUARD = '/usr/local/libexec/gtron-history-shared-reader-guard.py'
REFERENCE_GUARD = '/usr/local/libexec/gtron-history-reference-reader-guard.py'
LOCK = Path('/run/lock/gtron-mainnet-release.lock')
HEALTH = 'http://127.0.0.1:8090/wallet/getnodeinfo'
MAX_BINARY = 512 << 20


class SourceDifferent(Exception):
    pass


class SameSourceUnhealthy(Exception):
    pass


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def command(argv, timeout=60):
    result = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=timeout, check=False)
    require(result.returncode == 0,
            '%s failed (%d): %s' % (' '.join(argv), result.returncode,
                                    result.stderr.decode('utf-8', 'replace')[-2000:]))
    return result.stdout.decode('utf-8', 'replace')


def show(*names):
    lines = command(['/bin/systemctl', 'show', SERVICE, '--no-pager'] +
                    ['--property=' + name for name in names], 30).splitlines()
    result = {}
    for line in lines:
        name, separator, value = line.partition('=')
        if separator:
            result.setdefault(name, []).append(value)
    return {name: '\n'.join(values) for name, values in result.items()}


def root_bytes(path, limit):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_uid == 0 and
                not before.st_mode & 0o022 and before.st_size <= limit,
                'unsafe root-owned file: ' + str(path))
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    require(len(data) == before.st_size and len(data) <= limit and
            (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns),
            'file changed during read: ' + str(path))
    return data, stat.S_IMODE(before.st_mode)


def root_sha(path):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_uid == 0 and
                not before.st_mode & 0o022 and before.st_size <= MAX_BINARY,
                'unsafe root-owned binary: ' + str(path))
        digest, size = hashlib.sha256(), 0
        while True:
            chunk = stream.read(1 << 20)
            if not chunk:
                break
            size += len(chunk)
            require(size <= MAX_BINARY, 'binary exceeded hash limit')
            digest.update(chunk)
        after = os.fstat(stream.fileno())
    require(size == before.st_size and
            (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns),
            'binary changed during hash')
    return digest.hexdigest()


def atomic_root(path, data, mode):
    path = Path(path)
    temporary = path.with_name('.' + path.name + '.release-%d' % os.getpid())
    fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chown(str(temporary), 0, 0)
        os.chmod(str(temporary), mode)
        os.replace(str(temporary), str(path))
        directory = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.lexists(str(temporary)):
            os.unlink(str(temporary))


def json_bytes(value):
    return (json.dumps(value, sort_keys=True) + '\n').encode('utf-8')


def shared_marker(binary, digest, source):
    return {'version': 1, 'required_reader': 3, 'bucket_blocks': 1024,
            'binary': binary, 'binary_sha256': digest, 'source_commit': source}


def reference_marker(binary, digest, source):
    return {'version': 1, 'container_format': 'GTHREF01',
            'binary': binary, 'binary_sha256': digest, 'source_commit': source}


def systemd_exec(value):
    matches = re.findall(r'\{([^{}]+)\}', value)
    require(len(matches) == 1 and value.strip() == '{' + matches[0] + '}',
            'expected one effective ExecStart')
    fields = {}
    for field in re.split(r'\s*;\s*', matches[0]):
        field = field.strip()
        name, sep, text = field.partition('=')
        name, text = name.strip(), text.strip()
        require(sep and name not in fields, 'ambiguous ExecStart')
        fields[name] = text
    require(fields.get('ignore_errors') == 'no' and fields.get('path', '').startswith('/'),
            'unsafe effective ExecStart')
    argv = shlex.split(fields.get('argv[]', ''))
    require(argv and argv[0] == fields['path'], 'ExecStart path/argv mismatch')
    return fields['path'], argv


def guard_commands(raw):
    guards = []
    for line in raw.decode('utf-8').splitlines():
        if not line.startswith('ExecStartPre='):
            continue
        value = line[len('ExecStartPre='):].strip()
        if not value:
            continue
        argv = shlex.split(value)
        require(len(argv) >= 10 and argv[0] == '/usr/bin/python3' and
                argv[1] in (SHARED_GUARD, REFERENCE_GUARD), 'unexpected reader guard')
        flags = {}
        for name in ('--binary', '--sha256', '--source', '--marker'):
            require(argv.count(name) == 1, 'reader guard argument missing/duplicated: ' + name)
            position = argv.index(name)
            require(position + 1 < len(argv), 'reader guard argument has no value')
            flags[name] = argv[position + 1]
        guards.append((argv[1], argv, flags))
    require(len(guards) in (1, 2) and guards[0][0] == SHARED_GUARD and
            (len(guards) == 1 or guards[1][0] == REFERENCE_GUARD),
            'reader guard set changed')
    return guards


def proc_identity(pid):
    require(pid > 1, 'invalid MainPID')
    exe = os.readlink('/proc/%d/exe' % pid)
    with open('/proc/%d/cmdline' % pid, 'rb') as stream:
        raw = stream.read(1 << 20)
    require(len(raw) < 1 << 20, 'process arguments too large')
    argv = [part.decode('utf-8') for part in raw.split(b'\0') if part]
    return exe, argv


def proc_sha(pid):
    with open('/proc/%d/exe' % pid, 'rb') as stream:
        digest, size = hashlib.sha256(), 0
        while True:
            chunk = stream.read(1 << 20)
            if not chunk:
                break
            size += len(chunk)
            require(size <= MAX_BINARY, 'process binary too large')
            digest.update(chunk)
    return digest.hexdigest()


def wallet_head():
    with urllib.request.urlopen(HEALTH, timeout=5) as response:
        require(response.status == 200, 'Wallet API returned non-200')
        raw = response.read((1 << 20) + 1)
    require(len(raw) <= 1 << 20, 'Wallet node-info response is too large')
    value = json.loads(raw)
    number = value.get('currentBlock')
    require(isinstance(number, int) and not isinstance(number, bool) and number > 0,
            'Wallet API has no current block')
    return number


def wait_healthy(binary, digest, expected_args, timeout=180):
    deadline = time.time() + timeout
    last = 'service did not start'
    while time.time() < deadline:
        try:
            props = show('ActiveState', 'MainPID')
            require(props.get('ActiveState') == 'active', 'service not active')
            pid = int(props.get('MainPID', '0'))
            exe, argv = proc_identity(pid)
            require(exe == binary and argv == expected_args, 'running process path/flags differ')
            require(proc_sha(pid) == digest, 'running executable SHA differs')
            wallet_head()
            return pid
        except Exception as error:
            last = str(error)
            time.sleep(2)
    raise RuntimeError('service health/identity timeout: ' + last)


def inspect():
    memory, memory_mode = root_bytes(MEMORY, 1 << 20)
    shared, shared_mode = root_bytes(SHARED, 1 << 20)
    marker_raw, marker_mode = root_bytes(MARKER, 4096)
    marker = json.loads(marker_raw)
    guards = guard_commands(shared)
    first = guards[0][2]
    binary, digest, source = first['--binary'], first['--sha256'], first['--source']
    require(re.fullmatch(r'[0-9a-f]{64}', digest or '') and
            re.fullmatch(r'[0-9a-f]{40}', source or '') and
            first['--marker'] == str(MARKER), 'invalid shared reader identity')
    require(marker == shared_marker(binary, digest, source), 'shared reader marker differs')
    reference = None
    for path, _, flags in guards:
        require(flags['--binary'] == binary and flags['--sha256'] == digest and
                flags['--source'] == source, 'reader guards disagree')
        if path == REFERENCE_GUARD:
            reference = Path(flags['--marker'])
            require(reference == Path(binary).parent / 'reference-reader-required.json',
                    'reference marker path differs')
            reference_raw, reference_mode = root_bytes(reference, 4096)
            require(json.loads(reference_raw) == reference_marker(binary, digest, source),
                    'reference marker differs')
    props = show('ActiveState', 'MainPID', 'ExecStart', 'ExecStartPre')
    effective_binary, argv = systemd_exec(props.get('ExecStart', ''))
    require(effective_binary == binary and root_sha(binary) == digest,
            'configured binary differs from reader identity')
    require(memory.count(binary.encode()) == 1 and shared.count(binary.encode()) == len(guards) and
            shared.count(digest.encode()) == len(guards) and
            shared.count(source.encode()) == len(guards), 'drop-in identity is ambiguous')
    require(props.get('ExecStartPre', '').count(SPACE_GUARD) == 1,
            'mainnet space guard is missing/duplicated')
    active = props.get('ActiveState') == 'active'
    pid = int(props.get('MainPID', '0'))
    if active:
        exe, process_args = proc_identity(pid)
        require(exe == binary and process_args == argv and proc_sha(pid) == digest,
                'running process differs from guarded service')
    return {'binary': binary, 'sha': digest, 'source': source, 'argv': argv,
            'active': active, 'memory': (memory, memory_mode),
            'shared': (shared, shared_mode), 'marker': (marker_raw, marker_mode),
            'guards': guards, 'reference': reference, 'space_pre': props.get('ExecStartPre', '')}


def replace_once(raw, old, new, count, label):
    old, new = old.encode(), new.encode()
    require(raw.count(old) == count, 'ambiguous ' + label)
    return raw.replace(old, new)


def plan(old, new_binary, new_sha, source):
    guards = len(old['guards'])
    memory = replace_once(old['memory'][0], old['binary'], new_binary, 1, 'ExecStart binary')
    shared = replace_once(old['shared'][0], old['binary'], new_binary, guards, 'guard binary')
    shared = replace_once(shared, old['sha'], new_sha, guards, 'guard SHA')
    shared = replace_once(shared, old['source'], source, guards, 'guard source')
    new_reference = None
    if old['reference'] is not None:
        new_reference = Path(new_binary).parent / 'reference-reader-required.json'
        shared = replace_once(shared, str(old['reference']), str(new_reference), 1, 'reference marker')
    return {'memory': memory, 'shared': shared,
            'marker': json_bytes(shared_marker(new_binary, new_sha, source)),
            'reference': new_reference}


def candidate_bytes(path, claimed_sha):
    path = Path(path)
    require(re.fullmatch(r'[0-9a-f]{64}', claimed_sha or ''), 'invalid candidate SHA')
    require(re.fullmatch(re.escape(str(REPO / 'build')) + r'/deploy\.[^/]+/gtron', str(path)),
            'candidate outside deployment build directory')
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= MAX_BINARY,
                'candidate binary is missing or too large')
        data = stream.read(MAX_BINARY + 1)
        after = os.fstat(stream.fileno())
    require(len(data) == before.st_size and
            (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns) and
            hashlib.sha256(data).hexdigest() == claimed_sha,
            'candidate binary changed or SHA differs')
    return data


def install_release(source, digest, binary_data, has_reference):
    # Different valid builds of one source revision may have different build
    # metadata. Keep each root-owned binary immutable, including after a
    # failed attempt, while allowing a later retry of the same revision.
    # Keep the full source only in the --source guard value/marker. A full
    # commit in the path would make the strict guard-token count ambiguous.
    release = RELEASES / ('auto-' + source[:12] + '-' + digest[:16])
    if not release.exists():
        os.mkdir(str(release), 0o755)
        os.chown(str(release), 0, 0)
        os.chmod(str(release), 0o755)
    info = os.lstat(str(release))
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022,
            'unsafe release directory')
    binary = release / 'gtron'
    if binary.exists():
        require(root_sha(binary) == digest, 'existing release has different binary')
    else:
        atomic_root(binary, binary_data, 0o755)
    require(root_sha(binary) == digest, 'installed release SHA differs')
    atomic_root(release / 'reader-required.json',
                json_bytes(shared_marker(str(binary), digest, source)), 0o644)
    if has_reference:
        atomic_root(release / 'reference-reader-required.json',
                    json_bytes(reference_marker(str(binary), digest, source)), 0o644)
    return str(binary)


def verify(source):
    old = inspect()
    if old['source'] != source:
        raise SourceDifferent('running source %s differs from requested %s' %
                              (old['source'], source))
    for _, guard_argv, _ in old['guards']:
        command(guard_argv, 60)
    if not old['active']:
        raise SameSourceUnhealthy('requested source is configured but service is inactive')
    try:
        wallet_head()
    except Exception as error:
        raise SameSourceUnhealthy('Wallet API unhealthy: ' + str(error))
    return {'source_commit': source, 'binary': old['binary'],
            'binary_sha256': old['sha'], 'verified': True}


def restore(old):
    atomic_root(MEMORY, *old['memory'])
    atomic_root(SHARED, *old['shared'])
    atomic_root(MARKER, *old['marker'])
    command(['/bin/systemctl', 'daemon-reload'], 30)
    command(['/bin/systemctl', 'start', SERVICE], 120)
    wait_healthy(old['binary'], old['sha'], old['argv'])


def deploy(source, candidate, digest):
    require(re.fullmatch(r'[0-9a-f]{40}', source or ''), 'invalid source commit')
    head = command(['/usr/bin/git', '--git-dir=' + str(REPO / '.git'),
                    'rev-parse', 'HEAD'], 30).strip()
    require(head == source, 'checkout does not match requested source')
    buildinfo = command(['/data/go/bin/go', 'version', '-m', candidate], 30)
    for token in ('\tvcs.revision=' + source, '\t-tags=sapling',
                  '\tCGO_ENABLED=1', '\tGOOS=linux', '\tGOARCH=amd64'):
        require(token in buildinfo, 'candidate build info differs: ' + token.strip())
    old = inspect()
    for _, guard_argv, _ in old['guards']:
        command(guard_argv, 60)
    binary_data = candidate_bytes(candidate, digest)
    new_binary = install_release(source, digest, binary_data, old['reference'] is not None)
    changes = plan(old, new_binary, digest, source)
    switched = False
    try:
        switched = True
        if old['active']:
            command(['/bin/systemctl', 'stop', SERVICE], 120)
        atomic_root(MEMORY, changes['memory'], old['memory'][1])
        atomic_root(SHARED, changes['shared'], old['shared'][1])
        atomic_root(MARKER, changes['marker'], old['marker'][1])
        command(['/bin/systemctl', 'daemon-reload'], 30)
        effective = show('ExecStart', 'ExecStartPre')
        configured, argv = systemd_exec(effective.get('ExecStart', ''))
        require(configured == new_binary and argv == [new_binary] + old['argv'][1:],
                'new effective ExecStart changed service flags')
        require(effective.get('ExecStartPre', '').count(SPACE_GUARD) == 1,
                'mainnet space guard changed')
        require(effective.get('ExecStartPre', '').count(new_binary) == len(old['guards']) and
                effective.get('ExecStartPre', '').count(digest) == len(old['guards']) and
                effective.get('ExecStartPre', '').count(source) == len(old['guards']),
                'effective reader guards differ')
        for _, guard_argv, _ in guard_commands(changes['shared']):
            command(guard_argv, 60)
        command(['/bin/systemctl', 'start', SERVICE], 120)
        pid = wait_healthy(new_binary, digest, argv)
        result = {'deployed': True, 'source_commit': source, 'binary': new_binary,
                  'binary_sha256': digest, 'pid': pid}
        atomic_root(Path(new_binary).parent / 'DEPLOYED.json', json_bytes(result), 0o644)
        return result
    except Exception as error:
        if switched:
            try:
                try:
                    command(['/bin/systemctl', 'stop', SERVICE], 120)
                except Exception:
                    # A failed stop can still have killed the process. Always
                    # restore files and try to start/verify the old release.
                    pass
                restore(old)
            except Exception as rollback_error:
                raise RuntimeError('deployment failed: %s; rollback failed: %s' %
                                   (error, rollback_error))
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action')
    check = sub.add_parser('verify')
    check.add_argument('--source', required=True)
    publish = sub.add_parser('deploy')
    publish.add_argument('--source', required=True)
    publish.add_argument('--candidate', required=True)
    publish.add_argument('--sha256', required=True)
    args = parser.parse_args()
    require(args.action in ('verify', 'deploy'), 'verify or deploy action required')
    require(os.geteuid() == 0, 'root required')
    lockfd = os.open(str(LOCK), os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if args.action == 'verify':
        result = verify(args.source)
    else:
        result = deploy(args.source, args.candidate, args.sha256)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except SourceDifferent as error:
        print('mainnet release: ' + str(error), file=sys.stderr)
        sys.exit(3)
    except SameSourceUnhealthy as error:
        print('mainnet release: ' + str(error), file=sys.stderr)
        sys.exit(4)
    except Exception as error:
        print('mainnet release: ' + str(error), file=sys.stderr)
        sys.exit(1)
