#!/usr/bin/env python3
"""Root-owned mainnet release helper; install a reviewed copy in /usr/local/libexec.

The user-owned build script may request a release, but this program verifies the
existing guarded service before changing its pinned binary and reader markers.
Staging activation may transfer verified Pebble file ownership to the non-root
service user; it never edits database payload, the space guard, flags, or Nile.
"""

import argparse
import base64
from contextlib import ExitStack
import errno
import fcntl
import grp
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
import time
import urllib.request

SERVICE = 'gtron.service'
DEPLOY_SERVICE = 'gtron-deploy.service'
REPO = Path('/data/gtron/go-tron')
RELEASES = Path('/data/gtron/releases')
MEMORY = Path('/etc/systemd/system/gtron.service.d/zz-memory-budget-20260918.conf')
SHARED = Path('/etc/systemd/system/gtron.service.d/zz-history-shared-reader.conf')
MARKER = Path('/data/gtron/main/HISTORY_SHARED_CHUNKS_REQUIRES_READER_V3.json')
SPACE_GUARD = '/usr/local/libexec/gtron-mainnet-space-guard.py'
SHARED_GUARD = '/usr/local/libexec/gtron-history-shared-reader-guard.py'
REFERENCE_GUARD = '/usr/local/libexec/gtron-history-reference-reader-guard.py'
STAGING_GUARD = '/usr/local/libexec/gtron-history-staging-guard.py'
STAGING_PREPARED = Path('/data/gtron/main/HISTORY_STAGING_PREPARED.json')
STAGING_LATCH = Path('/data/gtron/main/MIGRATION_IN_PROGRESS.json')
STAGING_REQUIRED = Path('/data/gtron/main/HISTORY_STAGING_READER_REQUIRED.json')
STAGING_ACTIVATION = Path('/data/gtron/main/HISTORY_STAGING_ACTIVATION_INTENT.json')
STAGING_HANDOFF = Path('/data/gtron/main/HISTORY_STAGING_STORAGE_HANDOFF.json')
STAGING_START_LOCK = Path('/data/gtron/start.lock')
STAGING_SOURCE = Path('/data/gtron/main/datadir/gtron/chaindata')
STAGING_TARGET = Path('/data/gtron/main/datadir/gtron/history-staging')
STAGING_COLD = Path('/data/gtron/main/datadir/gtron/state-snapshots')
STAGING_ANCIENT = Path('/data/gtron/main/datadir/gtron/ancient')
STAGING_STORAGE_LOCK = '.history-staging-migration.lock'
STAGING_PEBBLE_MAX_FILES = 200000
LOCK = Path('/run/lock/gtron-mainnet-release.lock')
HEALTH = 'http://127.0.0.1:8090/wallet/getnodeinfo'
MAX_BINARY = 512 << 20
STAGING_HEALTH_DEFAULT = 12 * 60 * 60
STAGING_HEALTH_MAX = 24 * 60 * 60
STAGING_HEALTH_MIN = 10 * 60
STAGING_DEPLOY_MARGIN = 2 * 60 * 60


class SourceDifferent(Exception):
    pass


class SameSourceUnhealthy(Exception):
    pass


class StartupIdentityError(RuntimeError):
    """The single pinned startup process cannot become the accepted reader."""
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


_STAGING_PEBBLE_NAME = re.compile(
    r'(?:CURRENT|FORMAT-MAJOR-VERSION|LOCK|LOG(?:\.old)?|'
    r'MANIFEST-[0-9]+|OPTIONS-[0-9]+|[0-9]+\.(?:sst|log|blob)|'
    r'CURRENT\.[0-9]+\.dbtmp|temporary\.[0-9]+\.dbtmp|'
    r'marker\.manifest\.[0-9]+\.MANIFEST-[0-9]+|'
    r'marker\.format-version\.[0-9]+\.[0-9]+)\Z')


def _staging_parent_identity(pid):
    require(pid > 1, 'staging migration has no parent orchestrator')
    with open('/proc/%d/status' % pid, 'r') as stream:
        status = stream.read(1 << 20)
    match = re.search(r'^Uid:\s+(\d+)\s+(\d+)\s+', status, re.MULTILINE)
    require(match is not None and int(match.group(1)) == 0 and int(match.group(2)) == 0,
            'staging migration parent is not root')
    with open('/proc/%d/stat' % pid, 'r') as stream:
        value = stream.read(4096)
    close = value.rfind(')')
    fields = value[close + 2:].split() if close >= 0 else []
    require(len(fields) > 19 and fields[19].isdigit(),
            'staging migration parent start time unavailable')
    return fields[19]


def _staging_parent_lock_matches(listing, pid, stats):
    held = set()
    for line in listing.splitlines():
        words = line.split()
        if len(words) < 8 or words[1:4] != ['FLOCK', 'ADVISORY', 'WRITE'] or words[4] != str(pid):
            continue
        parts = words[5].split(':')
        if len(parts) != 3:
            continue
        try:
            held.add((int(parts[0], 16), int(parts[1], 16), int(parts[2])))
        except ValueError:
            continue
    return all((os.major(item.st_dev), os.minor(item.st_dev), item.st_ino) in held for item in stats)


def _staging_root_lock_owned(info):
    return (stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_gid == 0 and
            info.st_nlink == 1 and not info.st_mode & 0o077)


def _staging_require_parent_locks(latch, pid, started):
    require(_staging_parent_identity(pid) == started,
            'staging migration parent changed during activation')
    paths = [STAGING_START_LOCK] + [Path(latch[name]) / STAGING_STORAGE_LOCK
                                    for name in ('source', 'target', 'cold')]
    stats = []
    for path in paths:
        fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            named = os.stat(str(path), follow_symlinks=False)
            require(_staging_root_lock_owned(info) and
                    (info.st_dev, info.st_ino) == (named.st_dev, named.st_ino),
                    'unsafe staging migration lock: ' + str(path))
            stats.append(info)
        finally:
            os.close(fd)
    with open('/proc/locks', 'r') as stream:
        listing = stream.read(16 << 20)
    require(_staging_parent_lock_matches(listing, pid, stats),
            'staging migration parent does not hold all four exact locks')
    require(_staging_parent_identity(pid) == started,
            'staging migration parent changed after lock proof')


def _staging_require_stopped(unit, timer=False):
    fields = ['--property=ActiveState']
    if not timer:
        fields.append('--property=MainPID')
    output = command(['/bin/systemctl', 'show', unit] + fields + ['--no-pager'], 30)
    props = dict(line.split('=', 1) for line in output.splitlines() if '=' in line)
    require(props.get('ActiveState') in ('inactive', 'failed') and
            (timer or props.get('MainPID') == '0'),
            'staging handoff requires stopped ' + unit)


def _staging_service_owner(active=False):
    props = show('ActiveState', 'MainPID', 'User', 'Group', 'DynamicUser')
    state_ok = (props.get('ActiveState') == 'active' and
                props.get('MainPID', '0').isdigit() and int(props['MainPID']) > 0) if active else (
                    props.get('ActiveState') in ('inactive', 'failed') and props.get('MainPID') == '0')
    require(state_ok and props.get('User') == 'java-tron' and
            props.get('Group') == 'java-tron' and props.get('DynamicUser', 'no') in ('no', ''),
            'staging handoff requires pinned java-tron service User/Group and state')
    user = pwd.getpwnam('java-tron')
    group = grp.getgrnam('java-tron')
    require(user.pw_uid > 0 and group.gr_gid > 0 and user.pw_gid == group.gr_gid,
            'staging service numeric User/Group differs')
    return user.pw_uid, group.gr_gid


def _staging_owner_allowed(info, uid, gid):
    return (info.st_uid, info.st_gid) in ((0, 0), (0, gid), (uid, gid))


def _staging_file_identity(info):
    return info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size


def _staging_inventory_pebble(stack, path, uid, gid):
    fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    stack.callback(os.close, fd)
    root = os.fstat(fd)
    named = os.stat(str(path), follow_symlinks=False)
    require(stat.S_ISDIR(root.st_mode) and not root.st_mode & 0o7000 and
            _staging_owner_allowed(root, uid, gid) and
            root.st_mode & 0o700 == 0o700 and not root.st_mode & 0o022 and
            (root.st_dev, root.st_ino) == (named.st_dev, named.st_ino),
            'unsafe staging Pebble directory: ' + str(path))
    lockfd = os.open('LOCK', os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    stack.callback(os.close, lockfd)
    try:
        # Pebble uses POSIX F_SETLK, not BSD flock. Never reopen/close LOCK
        # while this fd is held: POSIX locks are process-wide per inode.
        fcntl.lockf(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError) as error:
        if error.errno in (errno.EACCES, errno.EAGAIN):
            raise RuntimeError('staging Pebble LOCK is held by another process: ' + str(path))
        raise
    names = sorted(os.listdir(fd))
    require(len(names) <= STAGING_PEBBLE_MAX_FILES and 'LOCK' in names and
            any(name == 'CURRENT' or name.startswith('marker.manifest.') for name in names),
            'staging Pebble directory has no bounded complete file inventory')
    inventory = []
    for name in names:
        if name == STAGING_STORAGE_LOCK:
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            require(_staging_root_lock_owned(info),
                    'unsafe root-only staging migration lock')
            continue
        require(_STAGING_PEBBLE_NAME.fullmatch(name) is not None,
                'foreign file or nested directory in staging Pebble store: ' + name)
        if name == 'LOCK':
            info = os.fstat(lockfd)
            named = os.stat(name, dir_fd=fd, follow_symlinks=False)
            require((info.st_dev, info.st_ino) == (named.st_dev, named.st_ino),
                    'staging Pebble LOCK changed before inventory')
        else:
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        mutable = name == 'LOCK' or name.startswith('MANIFEST-') or name.endswith('.log') or name == 'LOG'
        require(stat.S_ISREG(info.st_mode) and not info.st_mode & 0o7000 and
                info.st_nlink == 1 and
                info.st_dev == root.st_dev and _staging_owner_allowed(info, uid, gid) and
                info.st_mode & 0o400 and not info.st_mode & 0o022 and
                (not mutable or info.st_mode & 0o200),
                'unsafe staging Pebble file: ' + name)
        inventory.append((name, info))
    return {'path': path, 'fd': fd, 'lockfd': lockfd, 'root': root, 'files': inventory}


def _staging_handoff_file(store, name, before, uid, gid):
    fd = store['lockfd'] if name == 'LOCK' else os.open(
        name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=store['fd'])
    try:
        current = os.fstat(fd)
        require(_staging_file_identity(current) == _staging_file_identity(before) and
                _staging_owner_allowed(current, uid, gid),
                'staging Pebble file changed during handoff: ' + name)
        if (current.st_uid, current.st_gid) != (uid, gid):
            os.fchown(fd, uid, gid)
            os.fsync(fd)
        current = os.fstat(fd)
        require(_staging_file_identity(current) == _staging_file_identity(before) and
                (current.st_uid, current.st_gid) == (uid, gid) and
                current.st_mode & 0o400 and
                (name != 'LOCK' or current.st_mode & 0o200),
                'staging Pebble file did not become service-readable: ' + name)
    finally:
        if name != 'LOCK':
            os.close(fd)


def _staging_handoff_dir(store, uid, gid):
    current = os.fstat(store['fd'])
    require(_staging_file_identity(current) == _staging_file_identity(store['root']) and
            _staging_owner_allowed(current, uid, gid),
            'staging Pebble directory changed during handoff')
    if (current.st_uid, current.st_gid) != (uid, gid):
        os.fchown(store['fd'], uid, gid)
        os.fsync(store['fd'])
    current = os.fstat(store['fd'])
    named = os.stat(str(store['path']), follow_symlinks=False)
    require(_staging_file_identity(current) == _staging_file_identity(store['root']) and
            (current.st_uid, current.st_gid) == (uid, gid) and
            current.st_mode & 0o700 == 0o700 and
            (current.st_dev, current.st_ino) == (named.st_dev, named.st_ino),
            'staging Pebble directory did not become service-accessible')


def _staging_optional_ancient(stack, uid, gid):
    if not os.path.lexists(str(STAGING_ANCIENT)):
        return None
    directory = os.open(str(STAGING_ANCIENT), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    stack.callback(os.close, directory)
    info = os.fstat(directory)
    named = os.stat(str(STAGING_ANCIENT), follow_symlinks=False)
    require(stat.S_ISDIR(info.st_mode) and not info.st_mode & 0o7000 and
            (info.st_uid, info.st_gid) == (uid, gid) and
            info.st_mode & 0o700 == 0o700 and not info.st_mode & 0o022 and
            (info.st_dev, info.st_ino) == (named.st_dev, named.st_ino),
            'ancient directory is not owned by the service')
    if 'FLOCK' not in os.listdir(directory):
        return {'directory': directory, 'flock': None, 'info': None}
    flock = os.open('FLOCK', os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    stack.callback(os.close, flock)
    row = os.fstat(flock)
    require(stat.S_ISREG(row.st_mode) and not row.st_mode & 0o7000 and row.st_nlink == 1 and
            row.st_dev == info.st_dev and _staging_owner_allowed(row, uid, gid) and
            row.st_mode & 0o600 == 0o600 and not row.st_mode & 0o022,
            'unsafe ancient/FLOCK ownership')
    return {'directory': directory, 'flock': flock, 'info': row}


def _staging_cold_etl(stack, uid, gid):
    cold = os.open(str(STAGING_COLD), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    stack.callback(os.close, cold)
    root = os.fstat(cold)
    named = os.stat(str(STAGING_COLD), follow_symlinks=False)
    require(stat.S_ISDIR(root.st_mode) and not root.st_mode & 0o7000 and
            (root.st_uid, root.st_gid) == (uid, gid) and
            root.st_mode & 0o700 == 0o700 and not root.st_mode & 0o022 and
            (root.st_dev, root.st_ino) == (named.st_dev, named.st_ino),
            'cold directory is not service-writable')
    if 'etl' not in os.listdir(cold):
        return {'cold': cold, 'etl': None, 'info': None}
    etl = os.open('etl', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=cold)
    stack.callback(os.close, etl)
    info = os.fstat(etl)
    named_etl = os.stat('etl', dir_fd=cold, follow_symlinks=False)
    require(stat.S_ISDIR(info.st_mode) and not info.st_mode & 0o7000 and
            (info.st_dev, info.st_ino) == (named_etl.st_dev, named_etl.st_ino) and
            info.st_dev == root.st_dev and
            _staging_owner_allowed(info, uid, gid) and info.st_mode & 0o700 == 0o700 and
            not info.st_mode & 0o022 and
            ((info.st_uid, info.st_gid) == (uid, gid) or not os.listdir(etl)),
            'unsafe cold/etl directory or root-owned residual collector')
    return {'cold': cold, 'etl': etl, 'info': info}


def _staging_handoff_ancillary(ancient, cold, uid, gid):
    if ancient is not None and ancient['flock'] is not None:
        current = os.fstat(ancient['flock'])
        require(_staging_file_identity(current) == _staging_file_identity(ancient['info']) and
                _staging_owner_allowed(current, uid, gid), 'ancient/FLOCK changed during handoff')
        if (current.st_uid, current.st_gid) != (uid, gid):
            os.fchown(ancient['flock'], uid, gid)
            os.fsync(ancient['flock'])
    if cold['etl'] is not None:
        current = os.fstat(cold['etl'])
        require(_staging_file_identity(current) == _staging_file_identity(cold['info']) and
                _staging_owner_allowed(current, uid, gid), 'cold/etl changed during handoff')
        if (current.st_uid, current.st_gid) != (uid, gid):
            require(not os.listdir(cold['etl']), 'root-owned cold/etl gained residual files')
            os.fchown(cold['etl'], uid, gid)
            os.fsync(cold['etl'])


def _staging_probe_service_access(paths):
    runuser = shutil.which('runuser')
    require(runuser is not None, 'runuser is required for staging service access proof')
    # A killed probe cannot leave a foreign entry in the Pebble directory.
    # The child checks effective service permissions without creating files.
    program = ('import os,sys\n'
               'count=int(sys.argv[1]); dirs=sys.argv[2:2+count]; files=sys.argv[2+count:]\n'
               'for path in dirs:\n'
               '  if not os.access(path,os.R_OK|os.W_OK|os.X_OK): raise RuntimeError(path)\n'
               'for path in files:\n'
               '  fd=os.open(path,os.O_RDWR|os.O_NOFOLLOW); os.close(fd)\n')
    command([runuser, '-u', 'java-tron', '--', '/usr/bin/python3', '-c', program,
             str(len(paths['writable']))] +
            [str(path) for path in paths['writable']] + [str(path) for path in paths['files']], 60)


def _staging_dir_identity(path, uid, gid):
    fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        named = os.stat(str(path), follow_symlinks=False)
        require(stat.S_ISDIR(info.st_mode) and not info.st_mode & 0o7000 and
                (info.st_uid, info.st_gid) == (uid, gid) and
                info.st_mode & 0o700 == 0o700 and not info.st_mode & 0o022 and
                (info.st_dev, info.st_ino) == (named.st_dev, named.st_ino),
                'active staging store directory differs: ' + str(path))
        return [info.st_dev, info.st_ino]
    finally:
        os.close(fd)


def _staging_handoff_identity(latch, intent_sha, uid, gid, inodes):
    identity = {'version': 1, 'job_id': latch.get('job_id'),
                'candidate_sha256': latch.get('candidate_sha256'),
                'source_commit': latch.get('source_commit'),
                'activation_intent_sha256': intent_sha, 'uid': uid, 'gid': gid,
                'source': str(STAGING_SOURCE), 'target': str(STAGING_TARGET),
                'cold': str(STAGING_COLD), 'source_inode': inodes['source'],
                'target_inode': inodes['target'], 'cold_inode': inodes['cold']}
    require(re.fullmatch(r'[0-9a-f]{32}', identity['job_id'] or '') is not None and
            re.fullmatch(r'[0-9a-f]{64}', identity['candidate_sha256'] or '') is not None and
            re.fullmatch(r'[0-9a-f]{40}', identity['source_commit'] or '') is not None,
            'invalid staging storage handoff identity')
    return identity


def _staging_check_handoff_journal(identity, allowed):
    require(os.path.lexists(str(STAGING_HANDOFF)), 'staging handoff journal is missing')
    prior = json.loads(root_bytes(STAGING_HANDOFF, 16384)[0])
    require(isinstance(prior, dict) and prior.get('phase') in allowed and
            {key: prior.get(key) for key in identity} == identity,
            'staging storage handoff journal identity differs')


def _staging_postflight(stores, ancient, cold, uid, gid):
    for store in stores:
        directory = os.fstat(store['fd'])
        named = os.stat(str(store['path']), follow_symlinks=False)
        require((directory.st_dev, directory.st_ino) == (named.st_dev, named.st_ino) and
                (directory.st_uid, directory.st_gid) == (uid, gid) and
                directory.st_mode == store['root'].st_mode,
                'staging Pebble directory changed after handoff')
        actual = sorted(os.listdir(store['fd']))
        expected = sorted([name for name, _ in store['files']] + [STAGING_STORAGE_LOCK])
        require(actual == expected, 'staging Pebble inventory changed during handoff')
        for name, before in store['files']:
            current = os.stat(name, dir_fd=store['fd'], follow_symlinks=False)
            if name == 'LOCK':
                held = os.fstat(store['lockfd'])
                require((current.st_dev, current.st_ino) == (held.st_dev, held.st_ino),
                        'staging Pebble LOCK replaced after handoff')
            require(_staging_file_identity(current) == _staging_file_identity(before) and
                    (current.st_uid, current.st_gid) == (uid, gid),
                    'staging Pebble file changed after handoff: ' + name)
    if ancient is not None:
        current = os.fstat(ancient['directory'])
        named = os.stat(str(STAGING_ANCIENT), follow_symlinks=False)
        require((current.st_dev, current.st_ino) == (named.st_dev, named.st_ino) and
                (current.st_uid, current.st_gid) == (uid, gid), 'ancient directory changed')
        if ancient['flock'] is not None:
            current = os.fstat(ancient['flock'])
            named = os.stat(str(STAGING_ANCIENT / 'FLOCK'), follow_symlinks=False)
            require(_staging_file_identity(current) == _staging_file_identity(ancient['info']) and
                    (current.st_dev, current.st_ino) == (named.st_dev, named.st_ino) and
                    (current.st_uid, current.st_gid) == (uid, gid), 'ancient/FLOCK changed')
    current = os.fstat(cold['cold'])
    named = os.stat(str(STAGING_COLD), follow_symlinks=False)
    require((current.st_dev, current.st_ino) == (named.st_dev, named.st_ino) and
            (current.st_uid, current.st_gid) == (uid, gid), 'cold directory changed')
    if cold['etl'] is not None:
        current = os.fstat(cold['etl'])
        named = os.stat(str(STAGING_COLD / 'etl'), follow_symlinks=False)
        require(_staging_file_identity(current) == _staging_file_identity(cold['info']) and
                (current.st_dev, current.st_ino) == (named.st_dev, named.st_ino) and
                (current.st_uid, current.st_gid) == (uid, gid), 'cold/etl changed')


def handoff_staging_storage(latch, prepared, intent_sha):
    """Fail-closed ownership handoff under the parent's four migration locks.

    The durable activation intent precedes the first ownership mutation.
    A separate root journal pins immutable job/path/UID identity; retries scan
    current Pebble files afresh because a previously started reader may have
    published new MANIFEST/WAL/SST files before the migration journal was DONE.
    """
    require(os.geteuid() == 0 and latch.get('state') == 'VERIFIED_PENDING_ACTIVATION' and
            latch.get('service_was_active') is True and
            re.fullmatch(r'[0-9a-f]{64}', intent_sha or '') is not None,
            'staging storage handoff requires active-service verified activation')
    expected = {'source': STAGING_SOURCE, 'target': STAGING_TARGET, 'cold': STAGING_COLD}
    for name, path in expected.items():
        raw = os.path.abspath(str(path))
        require(raw == os.path.realpath(raw) and latch.get(name) == raw and
                prepared.get(name) == raw,
                'staging storage path differs from the pinned physical directory: ' + name)
    service_state = show('ActiveState', 'MainPID')
    active = service_state.get('ActiveState') == 'active'
    uid, gid = _staging_service_owner(active=active)
    _staging_require_stopped('gtron-deploy.timer', timer=True)
    _staging_require_stopped(DEPLOY_SERVICE)
    pid = os.getppid()
    started = _staging_parent_identity(pid)
    _staging_require_parent_locks(latch, pid, started)
    if active:
        inodes = {name: _staging_dir_identity(path, uid, gid)
                  for name, path in expected.items()}
        identity = _staging_handoff_identity(latch, intent_sha, uid, gid, inodes)
        _staging_check_handoff_journal(identity, ('DONE',))
        _staging_require_stopped('gtron-deploy.timer', timer=True)
        _staging_require_stopped(DEPLOY_SERVICE)
        _staging_require_parent_locks(latch, pid, started)
        return True
    with ExitStack() as stack:
        source = _staging_inventory_pebble(stack, STAGING_SOURCE, uid, gid)
        target = _staging_inventory_pebble(stack, STAGING_TARGET, uid, gid)
        cold = _staging_cold_etl(stack, uid, gid)
        ancient = _staging_optional_ancient(stack, uid, gid)
        _staging_require_parent_locks(latch, pid, started)
        stores = (source, target)
        identity = _staging_handoff_identity(latch, intent_sha, uid, gid, {
            'source': [source['root'].st_dev, source['root'].st_ino],
            'target': [target['root'].st_dev, target['root'].st_ino],
            'cold': [os.fstat(cold['cold']).st_dev, os.fstat(cold['cold']).st_ino]})
        if os.path.lexists(str(STAGING_HANDOFF)):
            _staging_check_handoff_journal(identity, ('IN_PROGRESS', 'DONE'))
        atomic_root(STAGING_HANDOFF, json_bytes(dict(identity, phase='IN_PROGRESS')), 0o600)
        for store in stores:
            for name, before in store['files']:
                _staging_handoff_file(store, name, before, uid, gid)
            _staging_handoff_dir(store, uid, gid)
        _staging_handoff_ancillary(ancient, cold, uid, gid)
        _staging_require_parent_locks(latch, pid, started)
        writable = [STAGING_SOURCE, STAGING_TARGET, STAGING_COLD]
        if cold['etl'] is not None:
            writable.append(STAGING_COLD / 'etl')
        if ancient is not None:
            writable.append(STAGING_ANCIENT)
        files = [STAGING_SOURCE / 'LOCK', STAGING_TARGET / 'LOCK']
        if ancient is not None and ancient['flock'] is not None:
            files.append(STAGING_ANCIENT / 'FLOCK')
        _staging_probe_service_access({'writable': writable, 'files': files})
        _staging_postflight(stores, ancient, cold, uid, gid)
        _staging_service_owner()
        _staging_require_stopped('gtron-deploy.timer', timer=True)
        _staging_require_stopped(DEPLOY_SERVICE)
        _staging_require_parent_locks(latch, pid, started)
        atomic_root(STAGING_HANDOFF, json_bytes(dict(identity, phase='DONE')), 0o600)
    return False


def json_bytes(value):
    return (json.dumps(value, sort_keys=True) + '\n').encode('utf-8')


def shared_marker(binary, digest, source):
    return {'version': 1, 'required_reader': 3, 'bucket_blocks': 1024,
            'binary': binary, 'binary_sha256': digest, 'source_commit': source}


def reference_marker(binary, digest, source):
    return {'version': 1, 'container_format': 'GTHREF01',
            'binary': binary, 'binary_sha256': digest, 'source_commit': source}


def staging_marker(digest, source, stop_intent=False, prune_mode=None, history_window=None):
    require(prune_mode in ('snap', 'archive') and
            type(history_window) is int and 0 < history_window < (1 << 64),
            'history-staging reader marker requires frozen mode/window')
    return {'version': 1, 'format_version': 1, 'binary_sha256': digest,
            'source_commit': source, 'stop_intent': stop_intent,
            'prune_mode': prune_mode, 'history_window': history_window}


def require_ordinary_deploy_allowed():
    require(not os.path.lexists(STAGING_LATCH),
            'history-staging migration latch blocks ordinary release and rollback')
    require(not os.path.lexists(STAGING_PREPARED) or os.path.lexists(STAGING_REQUIRED),
            'unmigrated history-staging source is pinned to its legacy reader')


def require_staging_capability(binary):
    raw = command([binary, 'db', 'history-staging', 'capability'], 30)
    require(json.loads(raw) == {'format_version': 1,
                                'history_staging_reader': True,
                                'protocol_version': 1},
            'candidate lacks required history-staging reader capability')


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


def staging_health_timeout(value=None):
    """Bound the slow cold-trio startup audit without changing old readers."""
    if value is None:
        return STAGING_HEALTH_DEFAULT
    require(isinstance(value, str) and re.fullmatch(r'[0-9]+', value),
            'invalid staging health timeout')
    seconds = int(value)
    require(STAGING_HEALTH_MIN <= seconds <= STAGING_HEALTH_MAX,
            'staging health timeout must be 600..86400 seconds')
    return seconds


def systemd_duration_seconds(raw):
    """Parse the human-readable TimeoutStartUSec value from systemd 219."""
    raw = raw.strip()
    if re.fullmatch(r'[0-9]+', raw):
        return int(raw) / 1000000.0
    units = {'d': 86400, 'h': 3600, 'min': 60, 's': 1,
             'ms': 0.001, 'us': 0.000001}
    parts = list(re.finditer(r'([0-9]+)(min|ms|us|d|h|s)', raw))
    if not parts or re.sub(r'([0-9]+)(min|ms|us|d|h|s)', '', raw).strip():
        raise RuntimeError('unrecognized deploy TimeoutStartUSec: ' + raw)
    return sum(int(part.group(1)) * units[part.group(2)] for part in parts)


def require_staging_deploy_timeout(health_timeout):
    raw = command(['/bin/systemctl', 'show', DEPLOY_SERVICE,
                   '--property=TimeoutStartUSec', '--no-pager'], 30)
    field = raw.strip().split('=', 1)
    require(len(field) == 2 and field[0] == 'TimeoutStartUSec',
            'deploy service did not report effective TimeoutStartUSec')
    actual = systemd_duration_seconds(field[1])
    # A changed release may use one full wait and then a second full wait to
    # prove its rollback. The oneshot unit must not kill that recovery.
    minimum = 2 * health_timeout + STAGING_DEPLOY_MARGIN
    require(actual >= minimum,
            'deploy TimeoutStartUSec=%s is below two staging health waits + build margin %ds; '
            'extend the effective gtron-deploy.service timeout before deployment' %
            (field[1], minimum))
    return actual


def proc_exe_fingerprint(pid):
    info = os.stat('/proc/%d/exe' % pid)
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def wait_healthy(binary, digest, expected_args, timeout=180):
    deadline = time.monotonic() + timeout
    started = time.monotonic()
    next_progress = started + 60
    last = 'service did not start'
    state = 'unknown'
    pinned_pid = None
    verified_sha = False
    pinned_exe = None
    while time.monotonic() < deadline:
        try:
            props = show('ActiveState', 'MainPID')
            state = props.get('ActiveState')
            if state in ('inactive', 'failed', 'deactivating'):
                raise StartupIdentityError('service exited during startup verification: ' + str(state))
            require(state == 'active', 'service not active yet: ' + str(state))
            pid = int(props.get('MainPID', '0'))
            if pinned_pid is not None and pid != pinned_pid:
                raise StartupIdentityError('service PID changed during startup verification: %d -> %d' %
                                           (pinned_pid, pid))
            require(pid > 0, 'service has no main PID')
            pinned_pid = pid
            exe, argv = proc_identity(pid)
            if exe != binary or argv != expected_args:
                raise StartupIdentityError('running process path/flags differ')
            fingerprint = proc_exe_fingerprint(pid)
            if pinned_exe is not None and fingerprint != pinned_exe:
                raise StartupIdentityError('running executable changed during startup verification')
            pinned_exe = fingerprint
            if not verified_sha:
                if proc_sha(pid) != digest:
                    raise StartupIdentityError('running executable SHA differs')
                verified_sha = True
            wallet_head()
            if proc_sha(pid) != digest or proc_exe_fingerprint(pid) != pinned_exe:
                raise StartupIdentityError('running executable changed before health acceptance')
            return pid
        except StartupIdentityError:
            raise
        except Exception as error:
            last = str(error)
            now = time.monotonic()
            if now >= next_progress:
                print('mainnet release: waiting for startup verification '
                      'elapsed=%ds pid=%s state=%s last=%s' %
                      (int(now - started), pinned_pid or '-', state, last),
                      file=sys.stderr, flush=True)
                next_progress = now + 60
            time.sleep(min(2, max(0, deadline - now)))
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
    staging = None
    if os.path.lexists(STAGING_REQUIRED):
        staging = root_bytes(STAGING_REQUIRED, 4096)
        marker = json.loads(staging[0])
        require(marker == staging_marker(digest, source, marker.get('stop_intent', False),
                                         marker.get('prune_mode'), marker.get('history_window')),
                'history-staging reader marker differs')
        require(props.get('ExecStartPre', '').count(STAGING_GUARD) == 1,
                'history-staging startup guard is missing/duplicated')
    active = props.get('ActiveState') == 'active'
    pid = int(props.get('MainPID', '0'))
    if active:
        exe, process_args = proc_identity(pid)
        require(exe == binary and process_args == argv and proc_sha(pid) == digest,
                'running process differs from guarded service')
    return {'binary': binary, 'sha': digest, 'source': source, 'argv': argv,
            'active': active, 'memory': (memory, memory_mode),
            'shared': (shared, shared_mode), 'marker': (marker_raw, marker_mode),
            'guards': guards, 'reference': reference, 'staging': staging,
            'space_pre': props.get('ExecStartPre', '')}


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
    staging = None
    if old.get('staging') is not None:
        old_marker = json.loads(old['staging'][0])
        staging = json_bytes(staging_marker(new_sha, source,
                                            old_marker.get('stop_intent', False),
                                            old_marker.get('prune_mode'),
                                            old_marker.get('history_window')))
    return {'memory': memory, 'shared': shared, 'staging': staging,
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
    if old.get('staging') is not None:
        command(['/usr/bin/python3', STAGING_GUARD, 'check-service'], 60)
    if not old['active']:
        raise SameSourceUnhealthy('requested source is configured but service is inactive')
    try:
        wallet_head()
    except Exception as error:
        raise SameSourceUnhealthy('Wallet API unhealthy: ' + str(error))
    return {'source_commit': source, 'binary': old['binary'],
            'binary_sha256': old['sha'], 'verified': True}


def restore(old, health_timeout=None):
    require(not os.path.lexists(STAGING_LATCH),
            'migration latch forbids rollback to the old release')
    atomic_root(MEMORY, *old['memory'])
    atomic_root(SHARED, *old['shared'])
    atomic_root(MARKER, *old['marker'])
    if old.get('staging') is not None:
        atomic_root(STAGING_REQUIRED, *old['staging'])
    command(['/bin/systemctl', 'daemon-reload'], 30)
    command(['/bin/systemctl', 'start', SERVICE], 120)
    if health_timeout is None:
        health_timeout = staging_health_timeout() if old.get('staging') is not None else 180
    wait_healthy(old['binary'], old['sha'], old['argv'], timeout=health_timeout)


def deploy(source, candidate, digest, health_timeout=None):
    require_ordinary_deploy_allowed()
    require(re.fullmatch(r'[0-9a-f]{40}', source or ''), 'invalid source commit')
    head = command(['/usr/bin/git', '--git-dir=' + str(REPO / '.git'),
                    'rev-parse', 'HEAD'], 30).strip()
    require(head == source, 'checkout does not match requested source')
    buildinfo = command(['/data/go/bin/go', 'version', '-m', candidate], 30)
    for token in ('\tvcs.revision=' + source, '\t-tags=sapling',
                  '\tCGO_ENABLED=1', '\tGOOS=linux', '\tGOARCH=amd64'):
        require(token in buildinfo, 'candidate build info differs: ' + token.strip())
    old = inspect()
    if old.get('staging') is not None:
        require(not json.loads(old['staging'][0]).get('stop_intent', False),
                'persistent inactive intent blocks ordinary deployment')
        require_staging_deploy_timeout(health_timeout or staging_health_timeout())
    for _, guard_argv, _ in old['guards']:
        command(guard_argv, 60)
    binary_data = candidate_bytes(candidate, digest)
    if old.get('staging') is not None:
        require_staging_capability(candidate)
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
        if changes['staging'] is not None:
            atomic_root(STAGING_REQUIRED, changes['staging'], old['staging'][1])
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
        if changes['staging'] is not None:
            command(['/usr/bin/python3', STAGING_GUARD, 'check-service'], 60)
        command(['/bin/systemctl', 'start', SERVICE], 120)
        if health_timeout is None:
            health_timeout = staging_health_timeout() if changes['staging'] is not None else 180
        pid = wait_healthy(new_binary, digest, argv, timeout=health_timeout)
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
                restore(old, health_timeout=health_timeout)
            except Exception as rollback_error:
                raise RuntimeError('deployment failed: %s; rollback failed: %s' %
                                   (error, rollback_error))
        raise


def activate_staging(source, candidate, digest, health_timeout=None):
    """Publish only the SHA-pinned capable reader after offline verification.

    Failure intentionally leaves the migration latch and stopped service in
    place. The old reader must never be restored after source adoption.
    """
    latch = json.loads(root_bytes(STAGING_LATCH, 16384)[0])
    prepared = json.loads(root_bytes(STAGING_PREPARED, 16384)[0])
    require(latch.get('version') == 1 and
            latch.get('state') == 'VERIFIED_PENDING_ACTIVATION' and
            latch.get('candidate_sha256') == digest and
            latch.get('source_commit') == source and
            type(latch.get('service_was_active')) is bool,
            'pending activation latch identity differs')
    require(prepared.get('version') == 1 and
            re.fullmatch(r'[0-9a-f]{64}', prepared.get('legacy_binary_sha256', '')),
            'invalid prepared reader pin')
    require(re.fullmatch(r'[0-9a-f]{40}', source or '') and
            re.fullmatch(r'[0-9a-f]{64}', digest or ''),
            'invalid candidate source/SHA')
    candidate = Path(candidate)
    require(candidate.is_absolute() and
            candidate.parent.parent == RELEASES and candidate.name == 'gtron' and
            candidate.parent.name.startswith('staging-') and
            root_sha(candidate) == digest,
            'candidate must be the pinned root-owned staging release')
    require_staging_capability(str(candidate))
    buildinfo = command(['/data/go/bin/go', 'version', '-m', str(candidate)], 30)
    for token in ('\tvcs.revision=' + source, '\t-tags=sapling',
                  '\tCGO_ENABLED=1', '\tGOOS=linux', '\tGOARCH=amd64'):
        require(token in buildinfo, 'candidate build info differs: ' + token.strip())
    intent_sha = latch.get('activation_intent_sha256')
    if intent_sha is None:
        old = inspect()
        require(not old['active'] and old['sha'] == prepared['legacy_binary_sha256'] and
                old.get('staging') is None,
                'migration activation requires the stopped pinned legacy service')
        require(old.get('space_pre', '').count(STAGING_GUARD) == 1,
                'effective history-staging startup guard is missing/duplicated')
        changes = plan(old, str(candidate), digest, source)
        require(changes['staging'] is None, 'unexpected existing staging reader marker')
        def encoded(data, mode):
            return {'bytes': base64.b64encode(data).decode('ascii'), 'mode': mode}
        intent = {'version': 1, 'candidate_sha256': digest,
                  'source_commit': source, 'binary': str(candidate),
                  'old_argv': old['argv'],
                  'files': {
                      'memory': {'old': encoded(*old['memory']),
                                 'new': encoded(changes['memory'], old['memory'][1])},
                      'shared': {'old': encoded(*old['shared']),
                                 'new': encoded(changes['shared'], old['shared'][1])},
                      'marker': {'old': encoded(*old['marker']),
                                 'new': encoded(changes['marker'], old['marker'][1])},
                      'required': {'old': None,
                                   'new': encoded(json_bytes(staging_marker(
                                       digest, source, not latch['service_was_active'],
                                       latch.get('prune_mode'), latch.get('history_window'))), 0o644)}}}
        intent_raw = json_bytes(intent)
        require(len(intent_raw) <= 1 << 20, 'activation intent too large')
        atomic_root(STAGING_ACTIVATION, intent_raw, 0o600)
        latch['activation_intent_sha256'] = hashlib.sha256(intent_raw).hexdigest()
        atomic_root(STAGING_LATCH, json_bytes(latch), 0o644)
    else:
        require(re.fullmatch(r'[0-9a-f]{64}', intent_sha), 'invalid activation intent SHA')
    intent_raw, _ = root_bytes(STAGING_ACTIVATION, 1 << 20)
    require(hashlib.sha256(intent_raw).hexdigest() == latch['activation_intent_sha256'],
            'activation intent changed')
    intent = json.loads(intent_raw)
    require(intent.get('version') == 1 and intent.get('candidate_sha256') == digest and
            intent.get('source_commit') == source and intent.get('binary') == str(candidate) and
            isinstance(intent.get('old_argv'), list) and intent['old_argv'] and
            intent['old_argv'][0] != str(candidate), 'activation intent identity differs')
    argv = [str(candidate)] + intent['old_argv'][1:]
    files = intent['files']
    require(set(files) == {'memory', 'shared', 'marker', 'required'},
            'activation intent file set differs')
    paths = {'memory': MEMORY, 'shared': SHARED, 'marker': MARKER,
             'required': STAGING_REQUIRED}
    def decoded(value):
        require(isinstance(value, dict) and type(value.get('mode')) is int and
                0 < value['mode'] <= 0o777 and isinstance(value.get('bytes'), str),
                'invalid activation file entry')
        return base64.b64decode(value['bytes'], validate=True), value['mode']
    # The intent is durable before the first replacement. Every retry accepts
    # exactly the old or new bytes, then converges deterministically to new.
    for name, path in paths.items():
        target = decoded(files[name]['new'])
        original = decoded(files[name]['old']) if files[name]['old'] is not None else None
        current = root_bytes(path, 1 << 20) if os.path.lexists(path) else None
        require(current == original or current == target,
                'activation file differs from both pinned states: ' + str(path))
    require(not show('ActiveState').get('ActiveState') == 'active' or
            root_bytes(STAGING_REQUIRED, 4096) == decoded(files['required']['new']),
            'unmarked active service during activation')
    current_state = show('ActiveState', 'MainPID')
    active_retry = current_state.get('ActiveState') == 'active'
    if active_retry:
        require(all((root_bytes(path, 1 << 20) if os.path.lexists(path) else None) ==
                    decoded(files[name]['new']) for name, path in paths.items()),
                'active staging reader does not have all pinned activation files')
        effective = show('ExecStart', 'ExecStartPre')
        configured, effective_argv = systemd_exec(effective.get('ExecStart', ''))
        require(configured == str(candidate) and effective_argv == argv and
                effective.get('ExecStartPre', '').count(STAGING_GUARD) == 1,
                'active staging reader effective identity differs')
        require(current_state.get('MainPID', '0').isdigit() and
                int(current_state['MainPID']) > 1, 'active staging reader has no MainPID')
        live_pid = int(current_state['MainPID'])
        live_binary, live_argv = proc_identity(live_pid)
        require(live_binary == str(candidate) and live_argv == argv and
                proc_sha(live_pid) == digest,
                'active staging reader process identity differs')
    handoff_was_active = handoff_staging_storage(
        latch, prepared, latch['activation_intent_sha256'])
    require(handoff_was_active == active_retry,
            'staging service changed state during ownership handoff')
    if active_retry:
        try:
            pid = wait_healthy(str(candidate), digest, argv,
                               timeout=health_timeout or staging_health_timeout())
        except Exception:
            try:
                command(['/bin/systemctl', 'stop', SERVICE], 120)
            except Exception:
                pass
            raise
        return {'activated': True, 'source_commit': source, 'binary': str(candidate),
                'binary_sha256': digest, 'pid': pid, 'service_was_active': True}
    # Reader guard marker files must exist before the changed drop-in can run.
    atomic_root(candidate.parent / 'reader-required.json',
                json_bytes(shared_marker(str(candidate), digest, source)), 0o644)
    if REFERENCE_GUARD.encode() in decoded(files['shared']['new'])[0]:
        atomic_root(candidate.parent / 'reference-reader-required.json',
                    json_bytes(reference_marker(str(candidate), digest, source)), 0o644)
    try:
        for name, path in paths.items():
            atomic_root(path, *decoded(files[name]['new']))
        command(['/bin/systemctl', 'daemon-reload'], 30)
        effective = show('ExecStart', 'ExecStartPre')
        configured, effective_argv = systemd_exec(effective.get('ExecStart', ''))
        require(configured == str(candidate) and effective_argv == argv and
                effective.get('ExecStartPre', '').count(STAGING_GUARD) == 1,
                'effective staging reader identity or guard differs')
        pid = None
        if latch['service_was_active']:
            command(['/usr/bin/python3', STAGING_GUARD, 'check-service'], 60)
            command(['/bin/systemctl', 'start', SERVICE], 120)
            pid = wait_healthy(str(candidate), digest, argv,
                               timeout=health_timeout or staging_health_timeout())
        return {'activated': True, 'source_commit': source, 'binary': str(candidate),
                'binary_sha256': digest, 'pid': pid, 'service_was_active': latch['service_was_active']}
    except Exception:
        try:
            command(['/bin/systemctl', 'stop', SERVICE], 120)
        except Exception:
            pass
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
    publish.add_argument('--staging-health-timeout-sec')
    activate = sub.add_parser('activate-staging')
    activate.add_argument('--source', required=True)
    activate.add_argument('--candidate', required=True)
    activate.add_argument('--sha256', required=True)
    activate.add_argument('--staging-health-timeout-sec')
    timeout_check = sub.add_parser('staging-timeout')
    timeout_check.add_argument('--staging-health-timeout-sec')
    args = parser.parse_args()
    require(args.action in ('verify', 'deploy', 'activate-staging', 'staging-timeout'),
            'verify, deploy, activate-staging or staging-timeout action required')
    require(os.geteuid() == 0, 'root required')
    lockfd = os.open(str(LOCK), os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    health_timeout = None
    if args.action in ('deploy', 'activate-staging', 'staging-timeout') and args.staging_health_timeout_sec is not None:
        health_timeout = staging_health_timeout(args.staging_health_timeout_sec)
    if args.action == 'staging-timeout':
        seconds = health_timeout or staging_health_timeout()
        require_staging_deploy_timeout(seconds)
        print(seconds)
        return
    if args.action == 'verify':
        result = verify(args.source)
    elif args.action == 'deploy':
        result = deploy(args.source, args.candidate, args.sha256, health_timeout=health_timeout)
    else:
        result = activate_staging(args.source, args.candidate, args.sha256,
                                  health_timeout=health_timeout)
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
